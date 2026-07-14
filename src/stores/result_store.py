"""结果存储：HTML 写入对象存储 + 离线元数据。

- HTML 正文上传至对象存储（S3/OSS/HDFS），不写本地业务路径。
- 离线表 resource / metadata.jsonl 中 **html 字段 = 对象存储 URL**。
- MVP 同时写本地 mirror 目录便于验证，但权威引用为 object URL。
"""
from __future__ import annotations

import json
import threading
from typing import Any, Dict, Optional

from config import Config
from src.crawl.dedup import ContentDedup
from src.domain.models import HtmlPage
from src.stores.object_store import LocalObjectStore, ObjectStore
from src.stores.offline_store import JsonlOfflineStore


class ResultStore:
    def __init__(
        self,
        config: Config,
        content_dedup: ContentDedup,
        object_store: Optional[ObjectStore] = None,
        offline: Optional[JsonlOfflineStore] = None,
    ) -> None:
        self._config = config
        self._dedup = content_dedup
        self._offline = offline
        self._lock = threading.Lock()
        self._meta_path = config.output_dir / "metadata.jsonl"
        self._meta_path.parent.mkdir(parents=True, exist_ok=True)
        os_cfg = config.object_storage
        self._object_store: ObjectStore = object_store or LocalObjectStore(
            bucket=os_cfg.bucket,
            prefix=os_cfg.prefix,
            scheme=os_cfg.scheme,
            endpoint=os_cfg.endpoint,
            mirror_dir=os_cfg.mirror_dir,
        )
        self.written = 0
        self.dup_skipped = 0

    def store_html(self, domain: str, page: HtmlPage, trace_id: str) -> Optional[Dict[str, Any]]:
        """上传 HTML 至对象存储，返回元数据（html 字段为对象 URL）；重复内容返回 None。"""
        is_dup, content_hash = self._dedup.check_and_add(page.html)
        page.content_hash = content_hash
        data = page.html.encode("utf-8")
        page.size = len(data)

        if is_dup:
            self.dup_skipped += 1
            meta = self._build_meta(domain, page, trace_id, object_url=None, object_key=None, is_dup=True)
            self._append_meta(meta)
            return None

        if isinstance(self._object_store, LocalObjectStore):
            key = self._object_store.html_key(domain, content_hash)
        else:
            key = f"{self._config.object_storage.prefix}/{domain}.{content_hash[:16]}.html"

        object_url = self._object_store.put(key, data, content_type=page.content_type)

        meta = self._build_meta(
            domain, page, trace_id,
            object_url=object_url, object_key=key, is_dup=False,
        )
        self._append_meta(meta)
        with self._lock:
            self.written += 1
        return meta

    def _build_meta(
        self, domain: str, page: HtmlPage, trace_id: str, *,
        object_url: Optional[str], object_key: Optional[str], is_dup: bool,
    ) -> Dict[str, Any]:
        return {
            "trace_id": trace_id,
            "url": page.url,
            "domain": domain,
            "content_type": page.content_type,
            "content_hash": page.content_hash,
            "size": page.size,
            "quality": page.quality,
            "http_status": page.status,
            # 离线表核心字段：html = 对象存储 URL（非本地路径）
            "html": object_url,
            "object_key": object_key,
            "is_dup": is_dup,
        }

    def _append_meta(self, meta: Dict[str, Any]) -> None:
        line = json.dumps(meta, ensure_ascii=False)
        with self._lock:
            with self._meta_path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
        if self._offline is not None:
            self._offline.record_resource(meta)
