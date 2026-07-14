"""离线 ETL 子模块（自动寻源管线）。"""
from src.etl.domain_merge import DomainMergeRecord, RawPageRecord, merge_domain_pages

__all__ = ["DomainMergeRecord", "RawPageRecord", "merge_domain_pages"]
