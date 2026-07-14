"""DataQueue Topic：由关键信息拼接，可反解出 domain。

格式：{topic_prefix}.{domain_encoded}
示例：crawl.link.task.example_com → domain=example.com
"""
from __future__ import annotations


def encode_domain(domain: str) -> str:
    """域名编码（`.` → `_`），用于拼入 Topic。"""
    return domain.replace(".", "_")


def decode_domain(encoded: str) -> str:
    """从 Topic 片段反解域名。"""
    return encoded.replace("_", ".")


def build_topic(domain: str, topic_prefix: str = "crawl.link.task") -> str:
    """根据 domain 构建 DataQueue Topic。"""
    return f"{topic_prefix}.{encode_domain(domain)}"


def parse_domain(topic: str, topic_prefix: str = "crawl.link.task") -> str:
    """从 Topic 解析 domain。"""
    head = f"{topic_prefix}."
    if not topic.startswith(head):
        raise ValueError(f"topic {topic!r} does not match prefix {topic_prefix!r}")
    return decode_domain(topic[len(head) :])


def topic_to_storage_key(topic: str) -> str:
    """Topic → HBase 存储目录名。"""
    return topic.replace(".", "_")


def topic_from_storage_key(storage_key: str, topic_prefix: str = "crawl.link.task") -> str:
    """存储目录名 → Topic。"""
    enc_prefix = topic_prefix.replace(".", "_")
    head = f"{enc_prefix}_"
    if not storage_key.startswith(head):
        raise ValueError(f"storage key {storage_key!r} does not match prefix {topic_prefix!r}")
    return build_topic(decode_domain(storage_key[len(head) :]), topic_prefix)
