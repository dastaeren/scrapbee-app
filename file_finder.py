import re
import urllib.parse
from typing import List, Dict, Set, Optional, Tuple
from bs4 import BeautifulSoup

from core.utils import normalize_url, robots_allowed
from core.models import Settings


DEFAULT_FILE_EXTS = [
    ".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx", ".csv", ".txt", ".rtf",
    ".zip", ".rar", ".7z", ".tar", ".gz",
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg",
    ".mp4", ".mov", ".avi", ".mkv", ".mp3", ".wav",
    ".json", ".xml", ".sql", ".log"
]

def same_domain(a: str, b: str) -> bool:
    pa = urllib.parse.urlparse(a)
    pb = urllib.parse.urlparse(b)
    return pa.netloc.lower() == pb.netloc.lower()

def is_probably_file(url: str, exts: List[str]) -> bool:
    u = url.lower().split("?")[0].split("#")[0]
    return any(u.endswith(ext) for ext in exts)

def guess_filename(url: str) -> str:
    u = url.split("?")[0].split("#")[0]
    name = u.rstrip("/").split("/")[-1] or "download"
    if len(name) > 120:
        name = name[-120:]
    return name

def extract_all_links(base_url: str, html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links = []

    # a[href]
    for a in soup.select("a[href]"):
        u = normalize_url(base_url, a.get("href"))
        if u: links.append(u)

    # img[src], source[src], link[href]
    for img in soup.select("img[src]"):
        u = normalize_url(base_url, img.get("src"))
        if u: links.append(u)

    for s in soup.select("source[src]"):
        u = normalize_url(base_url, s.get("src"))
        if u: links.append(u)

    for l in soup.select("link[href]"):
        u = normalize_url(base_url, l.get("href"))
        if u: links.append(u)

    return links

def find_next_page(base_url: str, html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")

    rel_next = soup.select_one('a[rel="next"][href]')
    if rel_next:
        return normalize_url(base_url, rel_next.get("href"))

    for a in soup.select("a[href]"):
        txt = (a.get_text() or "").strip().lower()
        if txt in ("next", "next ›", "older", "more", "load more"):
            return normalize_url(base_url, a.get("href"))
    return None


def discover_files_from_sites(
    site_urls: List[str],
    session,
    settings: Settings,
    max_pages: int,
    file_exts: List[str],
    stop_flag,
    log_fn,
    progress_fn=None,
) -> List[Dict[str, str]]:
    files: Dict[str, Dict[str, str]] = {}
    visited_pages: Set[str] = set()

    for seed_url in site_urls:
        if stop_flag():
            log_fn("warning", "Stop requested. File discovery halted.")
            break

        allowed, reason = robots_allowed(seed_url, user_agent=settings.user_agent or "*")
        if not allowed:
            log_fn("warning", f"robots.txt blocked: {seed_url}")
            continue
        else:
            log_fn("success", f"robots.txt ok: {seed_url}")

        queue: List[str] = [seed_url]
        local_count = 0

        while queue and local_count < max_pages:
            if stop_flag():
                log_fn("warning", "Stop requested. File discovery halted.")
                break

            page_url = queue.pop(0)
            if page_url in visited_pages:
                continue

            try:
                r = session.get(page_url, timeout=settings.timeout_seconds)
                visited_pages.add(page_url)
                local_count += 1

                ct = (r.headers.get("Content-Type") or "").lower()
                if "text/html" not in ct:
                    if is_probably_file(page_url, file_exts):
                        if page_url not in files:
                            files[page_url] = {
                                "File URL": page_url,
                                "Filename": guess_filename(page_url),
                                "Type": page_url.split(".")[-1].split("?")[0].upper(),
                                "Source Page": page_url,
                            }
                    continue

                html = r.text
                links = extract_all_links(page_url, html)

                for u in links:
                    if not u:
                        continue

                    # collect file
                    if is_probably_file(u, file_exts):
                        if u not in files:
                            files[u] = {
                                "File URL": u,
                                "Filename": guess_filename(u),
                                "Type": u.split(".")[-1].split("?")[0].upper(),
                                "Source Page": page_url,
                            }
                        continue

                    # crawl internal pages only
                    if same_domain(seed_url, u) and u not in visited_pages and len(queue) < max_pages:
                        queue.append(u)

                nxt = find_next_page(page_url, html)
                if nxt and same_domain(seed_url, nxt) and nxt not in visited_pages and len(queue) < max_pages:
                    queue.append(nxt)

                if progress_fn:
                    progress_fn(len(visited_pages), max_pages * max(1, len(site_urls)), len(files))

            except Exception as e:
                log_fn("error", f"Failed: {page_url} — {e}")
                if progress_fn:
                    progress_fn(len(visited_pages), max_pages * max(1, len(site_urls)), len(files))

    return list(files.values())
