from collections import deque
from typing import List, Set, Tuple, Optional
import urllib.parse

from bs4 import BeautifulSoup

from core.utils import normalize_url, robots_allowed


def is_same_domain(seed_url: str, other_url: str) -> bool:
    a = urllib.parse.urlparse(seed_url)
    b = urllib.parse.urlparse(other_url)
    return a.netloc.lower() == b.netloc.lower()


def extract_links_from_html(base_url: str, html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.select("a[href]"):
        u = normalize_url(base_url, a.get("href"))
        if u:
            links.append(u)
    return links


def find_next_page(base_url: str, html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")

    rel_next = soup.select_one('a[rel="next"][href]')
    if rel_next:
        return normalize_url(base_url, rel_next.get("href"))

    for a in soup.select("a[href]"):
        txt = (a.get_text() or "").strip().lower()
        if txt in ("next", "next ›", "older", "more"):
            return normalize_url(base_url, a.get("href"))

    return None


def collect_links(
    seed_url: str,
    session,
    timeout: int,
    user_agent: str,
    max_pages: int,
    stop_flag,
    log_fn,
    progress_fn=None,
) -> Tuple[List[str], List[str]]:
    """
    Crawl internal pages (same domain) up to max_pages.
    Returns (discovered_links, visited_pages).
    """
    allowed, reason = robots_allowed(seed_url, user_agent=user_agent or "*")
    log_fn(("warning" if not allowed else "success"), f"robots.txt: {reason}")
    if not allowed:
        return [], []

    q = deque([seed_url])
    visited_pages: Set[str] = set()
    discovered_links: Set[str] = set()

    while q and len(visited_pages) < max_pages:
        if stop_flag():
            log_fn("warning", "Stop requested. Link collection halted.")
            break

        page_url = q.popleft()
        if page_url in visited_pages:
            continue

        try:
            log_fn("success", f"Collecting links from: {page_url}")
            r = session.get(page_url, timeout=timeout)
            visited_pages.add(page_url)

            ct = (r.headers.get("Content-Type") or "").lower()
            if "text/html" not in ct:
                log_fn("warning", f"Skipped non-HTML page: {page_url}")
                if progress_fn:
                    progress_fn(len(visited_pages), max_pages, len(discovered_links))
                continue

            html = r.text
            links = extract_links_from_html(page_url, html)
            for link in links:
                discovered_links.add(link)
                if is_same_domain(seed_url, link) and link not in visited_pages:
                    if len(visited_pages) + len(q) < max_pages:
                        q.append(link)

            nxt = find_next_page(page_url, html)
            if nxt and is_same_domain(seed_url, nxt) and nxt not in visited_pages:
                if len(visited_pages) + len(q) < max_pages:
                    q.append(nxt)

            if progress_fn:
                progress_fn(len(visited_pages), max_pages, len(discovered_links))

        except Exception as e:
            log_fn("error", f"Failed collecting links from {page_url}: {e}")
            if progress_fn:
                progress_fn(len(visited_pages), max_pages, len(discovered_links))

    return sorted(discovered_links), sorted(visited_pages)
