"""对象存储抽象层。

HTML 及其他资源写入对象存储（S3/OSS/HDFS 等），离线表/元数据只记录对象 URL，
不存本地路径。MVP 用 LocalObjectStore：本地目录模拟存储后端，返回规范对象 URL。
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional, Protocol


class ObjectStore(Protocol):
    """对象存储接口。生产环境替换为 S3/OSS/HDFS 客户端实现。"""

    def put(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
        """上传对象，返回可访问的对象 URL（写入离线表的 html 等字段）。"""
        ...

    def object_url(self, key: str) -> str:
        """由 object key 构造对象 URL（不上传）。"""
        ...


class LocalObjectStore:
    """MVP：写入本地 mirror 目录，返回 s3:// 风格对象 URL。

    离线表中的 html 字段存此 URL；本地 mirror 仅便于开发验证，非权威数据源。
    """

    def __init__(
        self,
        *,
        bucket: str = "crawl-html",
        prefix: str = "html",
        scheme: str = "s3",
        endpoint: str = "",
        mirror_dir: Path = Path("output/object_store"),
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.scheme = scheme
        self.endpoint = endpoint.rstrip("/")
        self.mirror_dir = Path(mirror_dir)

    def object_url(self, key: str) -> str:
        key = key.lstrip("/")
        if self.endpoint:
            return f"{self.endpoint}/{self.bucket}/{key}"
        return f"{self.scheme}://{self.bucket}/{key}"

    def put(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
        key = key.lstrip("/")
        mirror_path = self.mirror_dir / key
        mirror_path.parent.mkdir(parents=True, exist_ok=True)
        mirror_path.write_bytes(data)
        return self.object_url(key)

    def html_key(self, domain: str, content_hash: str, *, dt: Optional[str] = None) -> str:
        """生成 HTML 对象的 key：{prefix}/dt=YYYY-MM-DD/{domain}.{hash}.html"""
        dt = dt or datetime.now().strftime("%Y-%m-%d")
        safe = domain.replace("/", "_")[:80]
        return f"{self.prefix}/dt={dt}/{safe}.{content_hash[:16]}.html"
