"""URL 规范化与可注册域（eTLD+1）解析工具。

优先使用 ``tldextract``（基于 Public Suffix List）；不可用时回退内置简化规则。
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional, Set
from urllib.parse import urljoin, urlparse, urlunparse

try:
    import tldextract

    _TLD_CACHE = Path(__file__).resolve().parent.parent / ".cache" / "tldextract"
    _TLD_EXTRACT = tldextract.TLDExtract(suffix_list_urls=None, cache_dir=str(_TLD_CACHE))
    _HAS_TLDEXTRACT = True
except ImportError:  # pragma: no cover
    tldextract = None  # type: ignore[assignment]
    _TLD_EXTRACT = None
    _HAS_TLDEXTRACT = False

# 回退：常见两段公共后缀
_MULTI_PART_SUFFIXES: Set[str] = {
    "co.uk", "org.uk", "gov.uk", "ac.uk", "co.jp", "com.cn", "net.cn", "org.cn",
    "gov.cn", "edu.cn", "com.hk", "com.tw", "com.au", "co.kr", "co.in", "com.br",
    "com.sg", "co.nz",
}

_SKIP_SCHEMES: Set[str] = {"mailto", "tel", "javascript", "data", "ftp", "file"}
_TRACKING_PREFIXES = ("utm_",)
_TRACKING_KEYS: Set[str] = {"ref", "referrer", "fbclid", "gclid", "spm", "from"}


def registrable_domain(host: str) -> str:
    """返回可注册域（eTLD+1）。例：sub.news.bbc.co.uk → bbc.co.uk。"""
    host = host.lower().strip(".")
    if not host or _is_ip(host):
        return host

    if _HAS_TLDEXTRACT and _TLD_EXTRACT is not None:
        ext = _TLD_EXTRACT(host)
        if ext.domain and ext.suffix:
            return f"{ext.domain}.{ext.suffix}"
        if ext.suffix:
            return ext.suffix
        return host

    parts = host.split(".")
    if len(parts) <= 2:
        return host
    last_two = ".".join(parts[-2:])
    last_three = ".".join(parts[-3:])
    if last_two in _MULTI_PART_SUFFIXES:
        return last_three
    return last_two


def _is_ip(host: str) -> bool:
    parts = host.split(".")
    return len(parts) == 4 and all(p.isdigit() for p in parts)


def tld_of(domain: str) -> str:
    return domain.rsplit(".", 1)[-1] if "." in domain else ""


def normalize_url(url: str, base: Optional[str] = None) -> Optional[str]:
    """规范化 URL：补全相对链接、统一 scheme/host、去 fragment 与跟踪参数。"""
    if not url:
        return None
    url = url.strip()
    if base:
        url = urljoin(base, url)
    try:
        p = urlparse(url)
    except ValueError:
        return None
    if p.scheme in _SKIP_SCHEMES:
        return None
    if p.scheme not in ("http", "https"):
        return None
    if not p.netloc:
        return None

    host = p.hostname or ""
    host = host.lower()
    netloc = host
    if p.port and not ((p.scheme == "http" and p.port == 80) or (p.scheme == "https" and p.port == 443)):
        netloc = f"{host}:{p.port}"

    query = _clean_query(p.query)
    path = p.path or "/"
    return urlunparse((p.scheme, netloc, path, p.params, query, ""))


def _clean_query(query: str) -> str:
    if not query:
        return ""
    kept = []
    for pair in query.split("&"):
        if not pair:
            continue
        key = pair.split("=", 1)[0].lower()
        if key in _TRACKING_KEYS or any(key.startswith(pfx) for pfx in _TRACKING_PREFIXES):
            continue
        kept.append(pair)
    return "&".join(kept)


def domain_of_url(url: str) -> str:
    host = urlparse(url).hostname or ""
    return registrable_domain(host)


def url_health_score(url: str) -> float:
    """URL 结构健康度 [0,1]：路径过深、参数过多、疑似垃圾结构则降分。"""
    p = urlparse(url)
    score = 1.0
    depth = len([s for s in p.path.split("/") if s])
    if depth > 6:
        score -= 0.3
    n_params = len([x for x in p.query.split("&") if x]) if p.query else 0
    if n_params > 4:
        score -= 0.3
    if len(url) > 200:
        score -= 0.2
    lowered = url.lower()
    if any(tok in lowered for tok in ("sessionid=", "phpsessid", "/calendar/", "sid=")):
        score -= 0.2
    return max(0.0, min(1.0, score))
