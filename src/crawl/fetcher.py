"""异步抓取引擎（L1 aiohttp）。

能力：
- 会话复用：``ClientSession`` + ``TCPConnector`` 连接池 / Keep-Alive / DNS 缓存
- 可配置并发（由 TaskScheduler Worker 按 ``FetchConfig.concurrency`` 控制 in-flight）
- 分段超时：total / connect / sock_read
- 超时与挑战类状态码重试 + 指数退避

更高层（指纹伪装 / Playwright / 代理池）为接口预留，见 ``_escalate``。
"""
from __future__ import annotations

import asyncio
import time
from typing import Optional, Protocol

from config import Config, FetchConfig
from src.domain.models import FetchResult, RenderMode
from src.util.logging_conf import get_logger, log

logger = get_logger("fetcher")

_CHALLENGE_TOKENS = (
    "just a moment", "checking your browser", "enable javascript",
    "captcha", "cf-browser-verification", "attention required",
)
_CHALLENGE_STATUS = {403, 429, 503}
_RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}


def _charset_from_content_type(content_type: str) -> Optional[str]:
    """从 Content-Type 解析 charset，避免 aiohttp get_encoding() 依赖未读 body。"""
    if not content_type:
        return None
    for part in content_type.split(";"):
        part = part.strip()
        if part.lower().startswith("charset="):
            value = part.split("=", 1)[1].strip().strip('"').strip("'")
            return value or None
    return None


def detect_challenge(status: int, body: str) -> bool:
    if status in _CHALLENGE_STATUS:
        return True
    if body:
        low = body[:4000].lower()
        if len(body) < 512 and any(tok in low for tok in _CHALLENGE_TOKENS):
            return True
        if any(tok in low for tok in _CHALLENGE_TOKENS):
            return True
    return False


class Fetcher(Protocol):
    async def fetch(self, url: str, render_mode: RenderMode) -> FetchResult: ...

    async def close(self) -> None: ...


class AiohttpFetcher:
    """L1 静态抓取：会话复用 + 连接池 + 超时重试。"""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._fetch: FetchConfig = config.fetch
        self._session = None  # aiohttp.ClientSession
        self._connector = None  # aiohttp.TCPConnector

    async def _ensure_session(self):
        if self._session is not None and not self._session.closed:
            return self._session

        import aiohttp

        cfg = self._fetch
        # 连接池 per-host 至少覆盖单域任务并发，否则多余请求会在池内排队并触发 ConnectionTimeout
        per_host = max(cfg.connector_limit_per_host, cfg.max_concurrency_per_host)
        self._connector = aiohttp.TCPConnector(
            limit=cfg.connector_limit,
            limit_per_host=per_host,
            ttl_dns_cache=cfg.dns_cache_ttl,
            keepalive_timeout=cfg.keepalive_timeout,
            enable_cleanup_closed=True,
            force_close=False,
        )
        timeout = aiohttp.ClientTimeout(
            total=cfg.timeout_seconds,
            connect=cfg.connect_timeout_seconds,
            sock_read=cfg.sock_read_timeout_seconds,
        )
        headers = {
            "User-Agent": cfg.user_agent,
            "Accept-Language": cfg.accept_language,
            "Accept": "text/html,application/xhtml+xml",
            "Connection": "keep-alive",
        }
        self._session = aiohttp.ClientSession(
            connector=self._connector,
            timeout=timeout,
            headers=headers,
            raise_for_status=False,
        )
        log(
            logger, 20, "fetcher_session_created",
            connector_limit=cfg.connector_limit,
            limit_per_host=cfg.connector_limit_per_host,
            timeout=cfg.timeout_seconds,
            connect_timeout=cfg.connect_timeout_seconds,
            max_retries=cfg.max_retries,
        )
        return self._session

    async def fetch(self, url: str, render_mode: RenderMode = RenderMode.STATIC) -> FetchResult:
        cfg = self._fetch
        last_err: Optional[str] = None
        start = time.monotonic()
        attempts = cfg.max_retries + 1

        for attempt in range(attempts):
            try:
                result = await self._fetch_once(url)
                if result.ok and detect_challenge(result.status, result.html or ""):
                    result.from_challenge = True
                    return await self._escalate(url, result)
                if result.ok:
                    return result
                # 非重试类 HTTP 状态：立即返回（如 404）
                if result.status and result.status not in _RETRYABLE_STATUS and result.status not in _CHALLENGE_STATUS:
                    return result
                last_err = result.error or f"status={result.status}"
            except Exception as exc:  # noqa: BLE001
                last_err = repr(exc)
                log(logger, 30, "fetch_attempt_error", url=url, attempt=attempt, error=last_err)

            if attempt + 1 < attempts:
                delay = cfg.retry_backoff_seconds * (2 ** attempt)
                await asyncio.sleep(delay)

        return FetchResult(
            url=url, ok=False, elapsed=time.monotonic() - start, error=last_err,
        )

    async def _fetch_once(self, url: str) -> FetchResult:
        session = await self._ensure_session()
        start = time.monotonic()
        async with session.get(url, allow_redirects=True) as resp:
            ctype = resp.headers.get("Content-Type", "")
            # 先读 body，禁止在未读时调用 get_encoding()（会触发 RuntimeError）
            raw = await resp.content.read(self._fetch.max_content_bytes)
            if "html" not in ctype.lower():
                return FetchResult(
                    url=str(resp.url), ok=False, status=resp.status,
                    content_type=ctype, elapsed=time.monotonic() - start,
                    error="non-html",
                )
            encoding = _charset_from_content_type(ctype) or "utf-8"
            html = raw.decode(encoding, errors="replace")
            return FetchResult(
                url=str(resp.url),
                ok=resp.status == 200,
                status=resp.status,
                html=html,
                content_type=ctype,
                elapsed=time.monotonic() - start,
                render_mode=RenderMode.STATIC,
                error=None if resp.status == 200 else f"status={resp.status}",
            )

    async def _escalate(self, url: str, l1_result: FetchResult) -> FetchResult:
        if not self._fetch.enable_browser_fallback:
            log(logger, 30, "challenge_detected_no_fallback", url=url, status=l1_result.status)
            l1_result.ok = False
            l1_result.error = "challenge"
            return l1_result
        return await self._browser_fetch(url)

    async def _browser_fetch(self, url: str) -> FetchResult:
        raise NotImplementedError(
            "L2 Playwright 渲染未实现（接口占位）。安装 playwright 后在此接入 stealth 抓取。"
        )

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None
        self._connector = None
        log(logger, 20, "fetcher_session_closed")


class MockFetcher:
    """自测用：从内存页面表返回固定 HTML，无需网络。"""

    def __init__(self, pages: dict) -> None:
        self._pages = pages

    async def fetch(self, url: str, render_mode: RenderMode = RenderMode.STATIC) -> FetchResult:
        await asyncio.sleep(0)
        html = self._pages.get(url)
        if html is None:
            return FetchResult(url=url, ok=False, status=404, error="not-found")
        return FetchResult(
            url=url, ok=True, status=200, html=html, content_type="text/html", elapsed=0.001,
        )

    async def close(self) -> None:
        return None
