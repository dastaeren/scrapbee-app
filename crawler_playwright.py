import os
from urllib.parse import urlparse
from typing import List, Dict, Any, Optional, Callable

import requests

from crawler_requests import (
    is_valid_url,
    normalize_ext,
    looks_like_download_endpoint,
    probe_file,
    discover_sitemaps,
    fetch_sitemap_urls,
)

def crawl_playwright(
    start_urls: List[str],
    allowed_exts: List[str],
    settings,
    stop_cb: Callable[[], bool],
    progress_cb: Optional[Callable[[int, str], None]] = None,
) -> List[Dict[str, Any]]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        raise RuntimeError(
            "Playwright not installed or browsers missing. Run:\n"
            "pip install playwright\n"
            "playwright install chromium"
        ) from e

    allowed_set = set([e.lower().strip() for e in allowed_exts])

    # Session to probe endpoints
    sess = requests.Session()
    sess.headers.update({"User-Agent": settings.user_agent})
    probe_cache = {}

    # Expand with sitemaps (optional)
    expanded = []
    if settings.use_sitemaps:
        seen_domains = set()
        for su in start_urls:
            if not is_valid_url(su):
                continue
            dom = urlparse(su).netloc
            expanded.append(su)
            if dom in seen_domains:
                continue
            seen_domains.add(dom)

            sitemaps = discover_sitemaps(sess, su, timeout=settings.timeout_seconds)
            sm_urls = []
            for sm in sitemaps:
                sm_urls.extend(fetch_sitemap_urls(
                    sess, sm, timeout=settings.timeout_seconds, max_urls=settings.max_sitemap_urls - len(sm_urls)
                ))
                if len(sm_urls) >= settings.max_sitemap_urls:
                    break
            if settings.same_domain_only:
                sm_urls = [u for u in sm_urls if urlparse(u).netloc == dom]
            expanded.extend(sm_urls[: settings.max_sitemap_urls])

        expanded = list(dict.fromkeys(expanded))
    else:
        expanded = [u for u in start_urls if is_valid_url(u)]

    files: List[Dict[str, Any]] = []
    seen_files = set()
    visited_pages = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent=settings.user_agent)

        for idx, url in enumerate(expanded, start=1):
            if stop_cb():
                break
            if len(visited_pages) >= settings.max_pages:
                break
            if url in visited_pages:
                continue

            visited_pages.add(url)

            if progress_cb:
                pct = int(min(99, (len(visited_pages) / max(1, settings.max_pages)) * 100))
                progress_cb(pct, f"JS crawling {len(visited_pages)}/{settings.max_pages} | files={len(files)}")

            try:
                page.goto(url, wait_until="networkidle", timeout=settings.timeout_seconds * 1000)
            except Exception:
                continue

            hrefs = page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)") or []
            root = urlparse(url).netloc

            for link in hrefs:
                if stop_cb():
                    break
                if not is_valid_url(link):
                    continue
                if settings.same_domain_only and urlparse(link).netloc != root:
                    continue

                ext = normalize_ext(link)
                filename = ""

                if (not ext) and settings.deep_detect_downloads and looks_like_download_endpoint(link):
                    if link not in probe_cache:
                        probe_cache[link] = probe_file(sess, link, timeout=settings.timeout_seconds)
                    probed = probe_cache.get(link)
                    if probed:
                        ext = probed["ext"]
                        link = probed["final_url"]
                        filename = probed["filename"]

                if ext and (not allowed_set or ext in allowed_set):
                    if link not in seen_files:
                        files.append({
                            "Select": False,
                            "File": filename or os.path.basename(urlparse(link).path) or link,
                            "Type": ext,
                            "URL": link,
                            "Source": url,
                        })
                        seen_files.add(link)
                        if len(files) >= settings.max_files:
                            break

            if len(files) >= settings.max_files:
                break

        browser.close()

    if progress_cb:
        progress_cb(100, f"Done. Pages={len(visited_pages)}, Found files={len(files)}")
    return files
