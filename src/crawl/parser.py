"""HTML 解析与链接发现。

从 HTML 中提取出链（用于扩链与新域发现）；HTML 本身为采集目标，不做二次内容抽取。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import List, Optional, Set

from src.domain.models import DiscoveredLink
from src.domain.urls import domain_of_url, normalize_url


@dataclass
class RawLink:
    url: str
    anchor: str
    position: str


@dataclass
class HtmlDocument:
    base_url: str
    text: str = ""
    code_blocks: List[str] = field(default_factory=list)
    image_urls: List[str] = field(default_factory=list)
    links: List[RawLink] = field(default_factory=list)


class _DocParser(HTMLParser):
    _SECTION_TAGS = {"nav": "nav", "footer": "footer", "aside": "ad"}
    _SKIP_TEXT_TAGS = {"script", "style", "noscript"}

    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.doc = HtmlDocument(base_url=base_url)
        self._section_stack: List[str] = []
        self._in_code = 0
        self._in_skip = 0
        self._cur_href: Optional[str] = None
        self._cur_anchor: List[str] = []
        self._code_buf: List[str] = []
        self._text_buf: List[str] = []

    @property
    def _position(self) -> str:
        return self._section_stack[-1] if self._section_stack else "content"

    def handle_starttag(self, tag, attrs):
        adict = dict(attrs)
        if tag in self._SECTION_TAGS:
            self._section_stack.append(self._SECTION_TAGS[tag])
        elif tag in self._SKIP_TEXT_TAGS:
            self._in_skip += 1
        elif tag in ("pre", "code"):
            self._in_code += 1
        elif tag == "a":
            self._cur_href = adict.get("href")
            self._cur_anchor = []
        elif tag == "img":
            src = adict.get("src") or adict.get("data-src")
            norm = normalize_url(src, self.base_url) if src else None
            if norm:
                self.doc.image_urls.append(norm)

    def handle_endtag(self, tag):
        if tag in self._SECTION_TAGS:
            if self._section_stack:
                self._section_stack.pop()
        elif tag in self._SKIP_TEXT_TAGS:
            self._in_skip = max(0, self._in_skip - 1)
        elif tag in ("pre", "code"):
            self._in_code = max(0, self._in_code - 1)
            if self._in_code == 0 and self._code_buf:
                block = "".join(self._code_buf).strip()
                if block:
                    self.doc.code_blocks.append(block)
                self._code_buf = []
        elif tag == "a":
            if self._cur_href:
                norm = normalize_url(self._cur_href, self.base_url)
                if norm:
                    self.doc.links.append(
                        RawLink(url=norm, anchor=" ".join(self._cur_anchor).strip(),
                                position=self._position)
                    )
            self._cur_href = None
            self._cur_anchor = []

    def handle_data(self, data):
        if self._in_skip:
            return
        if self._in_code:
            self._code_buf.append(data)
        text = data.strip()
        if text:
            self._text_buf.append(text)
            if self._cur_href is not None:
                self._cur_anchor.append(text)

    def finalize(self) -> HtmlDocument:
        self.doc.text = "\n".join(self._text_buf)
        return self.doc


class Parser:
    def parse(self, base_url: str, html: str) -> HtmlDocument:
        p = _DocParser(base_url)
        try:
            p.feed(html)
        except Exception:  # noqa: BLE001 容错：解析异常返回已收集内容
            pass
        return p.finalize()

    def discovered_links(self, doc: HtmlDocument, source_domain: str) -> List[DiscoveredLink]:
        seen: Set[str] = set()
        out: List[DiscoveredLink] = []
        for raw in doc.links:
            if raw.url in seen:
                continue
            seen.add(raw.url)
            dom = domain_of_url(raw.url)
            out.append(
                DiscoveredLink(
                    url=raw.url, domain=dom, anchor=raw.anchor, position=raw.position,
                    same_domain=(dom == source_domain),
                )
            )
        return out
