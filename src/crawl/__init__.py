"""抓取相关：Fetcher / Parser / robots / 去重。"""
from src.crawl.dedup import ContentDedup, UrlDedup
from src.crawl.fetcher import AiohttpFetcher, Fetcher, MockFetcher
from src.crawl.parser import Parser
from src.crawl.robots import RobotsCache

__all__ = [
    "AiohttpFetcher",
    "ContentDedup",
    "Fetcher",
    "MockFetcher",
    "Parser",
    "RobotsCache",
    "UrlDedup",
]
