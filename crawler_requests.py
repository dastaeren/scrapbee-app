import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urljoin, parse_qs, unquote
from typing import List, Dict, Any, Optional, Callable, Tuple, Set

import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET


DEFAULT_FILE_EXTS = [
    ".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx", ".csv", ".txt", ".rtf",
    ".jpg", ".jpeg", ".png", ".gif", ".webp",
    ".mp4", ".mp3", ".wav", ".avi",
    ".json", ".xml", ".zip", ".rar", ".7z"
]


def is_valid_url(u: str) -> bool:
    try:
        p = urlparse(u.strip())
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False


def normalize_ext(url: str) -> str:
    path = urlparse(url).path.lower()
    for ext in DEFAULT_FILE_EXTS:
        if path.endswith(ext):
            return ext
    return ""


def looks_like_download_endpoint(url: str) -> bool:
    u = urlparse(url)
    path = (u.path or "").lower()
    qs = parse_qs(u.query or "")

    if "/download/" in path:
        return True
    if "download" in qs or "dlm_download" in qs or "attachment_id" in qs or "file" in qs:
        return True
    if "dlm_download_category" in qs:
        return False  # category page (not file)
    return False


def parse_content_disposition_filename(cd: str) -> Optional[str]:
    if not cd:
        return None
    m = re.search(r"filename\*\s*=\s*([^']*)''([^;]+)", cd, flags=re.IGNORECASE)
    if m:
        return unquote(m.group(2)).strip().strip('"').strip("'")
    m = re.search(r'filename\s*=\s*"([^"]+)"', cd, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m = re.search(r"filename\s*=\s*([^;]+)", cd, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip().strip('"').strip("'")
    return None


def guess_extension_from_headers(url: str, headers: dict) -> Tuple[str, str]:
    cd = headers.get("content-disposition") or headers.get("Content-Disposition") or ""
    ct = (headers.get("content-type") or headers.get("Content-Type") or "").lower()

    filename = parse_content_disposition_filename(cd) or ""
    if filename:
        ext = os.path.splitext(filename.lower())[1]
        if ext in DEFAULT_FILE_EXTS:
            return filename, ext

    ct_map = {
        "application/pdf": ".pdf",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
        "application/vnd.ms-excel": ".xls",
        "text/csv": ".csv",
        "application/zip": ".zip",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
        "application/octet-stream": "",  # unknown; leave empty
    }
    for k, v in ct_map.items():
        if k in ct and v:
            return (filename or ""), v

    return (filename or ""), normalize_ext(url)


def probe_file(session: requests.Session, url: str, timeout: int) -> Optional[Dict[str, str]]:
    # HEAD first
    try:
        r = session.head(url, allow_redirects=True, timeout=timeout)
        if r.status_code < 400:
            fname, ext = guess_extension_from_headers(r.url, r.headers)
            if ext:
                return {"final_url": r.url, "filename": fname or os.path.basename(urlparse(r.url).path) or r.url, "ext": ext}
    except Exception:
        pass

    # GET fallback
    try:
        r = session.get(url, allow_redirects=True, stream=True, timeout=timeout)
        if r.status_code < 400:
            fname, ext = guess_extension_from_headers(r.url, r.headers)
            if ext:
                return {"final_url": r.url, "filename": fname or os.path.basename(urlparse(r.url).path) or r.url, "ext": ext}
    except Exception:
        return None

    return None


def fetch_html(session: requests.Session, url: str, timeout: int) -> Optional[str]:
    try:
        r = session.get(url, timeout=timeout)
        if r.status_code >= 400:
            return None
        ct = (r.headers.get("content-type") or "").lower()
        if "text/html" not in ct:
            return None
        return r.text
    except Exception:
        return None


# ---------------------------
# Better link extraction
# ---------------------------
JS_URL_PATTERNS = [
    r"window\.location\s*=\s*['\"]([^'\"]+)['\"]",
    r"location\.href\s*=\s*['\"]([^'\"]+)['\"]",
    r"document\.location\s*=\s*['\"]([^'\"]+)['\"]",
]

def extract_links_from_html(base_url: str, html: str) -> Set[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: Set[str] = set()

    # a[href]
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if href:
            links.add(urljoin(base_url, href))

    # button[onclick], a[onclick]
    for el in soup.select("[onclick]"):
        onclick = (el.get("onclick") or "").strip()
        for pat in JS_URL_PATTERNS:
            for m in re.findall(pat, onclick, flags=re.IGNORECASE):
                links.add(urljoin(base_url, m))

    # raw html string scan (rare but helps)
    for pat in JS_URL_PATTERNS:
        for m in re.findall(pat, html, flags=re.IGNORECASE):
            links.add(urljoin(base_url, m))

    # filter valid urls
    out = set()
    for u in links:
        if is_valid_url(u):
            out.add(u)
    return out


# ---------------------------
# Sitemap discovery
# ---------------------------
def domain_root(url: str) -> str:
    u = urlparse(url)
    return f"{u.scheme}://{u.netloc}"


def discover_sitemaps(session: requests.Session, start_url: str, timeout: int) -> List[str]:
    root = domain_root(start_url)
    sitemaps = set()

    # robots.txt
    try:
        r = session.get(root + "/robots.txt", timeout=timeout)
        if r.status_code < 400 and r.text:
            for line in r.text.splitlines():
                if line.lower().startswith("sitemap:"):
                    sm = line.split(":", 1)[1].strip()
                    if sm:
                        sitemaps.add(sm)
    except Exception:
        pass

    # common
    sitemaps.add(root + "/sitemap.xml")
    sitemaps.add(root + "/sitemap_index.xml")
    return list(sitemaps)


def parse_sitemap_xml(xml_text: str) -> List[str]:
    urls = []
    try:
        root = ET.fromstring(xml_text.strip())
        # Namespace handling (ignore namespaces by stripping)
        for elem in root.iter():
            tag = elem.tag.split("}")[-1].lower()
            if tag == "loc" and elem.text:
                urls.append(elem.text.strip())
    except Exception:
        return []
    return urls


def fetch_sitemap_urls(
    session: requests.Session,
    sitemap_url: str,
    timeout: int,
    max_urls: int,
) -> List[str]:
    try:
        r = session.get(sitemap_url, timeout=timeout)
        if r.status_code >= 400 or not r.text:
            return []
        candidates = parse_sitemap_xml(r.text)

        # if sitemap index -> fetch child sitemaps
        # heuristic: if many URLs end with .xml, treat as index
        xmls = [u for u in candidates if u.lower().endswith(".xml")]
        if len(xmls) >= 2:
            out = []
            for child in xmls:
                out.extend(fetch_sitemap_urls(session, child, timeout=timeout, max_urls=max_urls - len(out)))
                if len(out) >= max_urls:
                    break
            return out[:max_urls]

        return candidates[:max_urls]
    except Exception:
        return []


# ---------------------------
# Serper (pagination)
# ---------------------------
def serper_search_paginated(query: str, total_results: int, timeout: int, api_key: str) -> "pd.DataFrame":
    import pandas as pd

    if not api_key:
        raise RuntimeError("SERPER_API_KEY missing")

    url = "https://google.serper.dev/search"
    headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}

    target = max(1, min(int(total_results), 300))
    rows: List[Dict[str, Any]] = []

    page = 1
    while len(rows) < target:
        batch = min(100, target - len(rows))
        payload = {"q": query, "num": batch, "page": page}
        r = requests.post(url, headers=headers, json=payload, timeout=timeout)
        r.raise_for_status()
        data = r.json()

        organic = data.get("organic", []) or []
        if not organic:
            break

        for item in organic:
            rows.append({
                "Select": False,
                "Title": item.get("title", ""),
                "URL": item.get("link", ""),
                "Snippet": item.get("snippet", "")
            })
            if len(rows) >= target:
                break

        page += 1
        time.sleep(0.05)

    df = pd.DataFrame(rows).drop_duplicates(subset=["URL"]).reset_index(drop=True)
    return df


# ---------------------------
# Concurrent crawler
# ---------------------------
def crawl_requests_concurrent(
    start_urls: List[str],
    allowed_exts: List[str],
    settings,
    stop_cb: Callable[[], bool],
    progress_cb: Optional[Callable[[int, str], None]] = None,
) -> List[Dict[str, Any]]:
    allowed_set = set([e.lower().strip() for e in allowed_exts])
    sess = requests.Session()
    sess.headers.update({"User-Agent": settings.user_agent})

    # Expand start URLs with sitemaps (optional)
    expanded_start = []
    if settings.use_sitemaps:
        seen_domains = set()
        for su in start_urls:
            if not is_valid_url(su):
                continue
            dom = urlparse(su).netloc
            expanded_start.append(su)
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

            # keep same domain only if requested
            if settings.same_domain_only:
                sm_urls = [u for u in sm_urls if urlparse(u).netloc == dom]
            expanded_start.extend(sm_urls[: settings.max_sitemap_urls])

        # de-dupe
        expanded_start = list(dict.fromkeys(expanded_start))
    else:
        expanded_start = [u for u in start_urls if is_valid_url(u)]

    # Determine roots for each original URL for domain locking
    roots = {}
    for u in expanded_start:
        if is_valid_url(u):
            roots[u] = urlparse(u).netloc

    visited: Set[str] = set()
    files: List[Dict[str, Any]] = []
    seen_files = set()

    probe_cache: Dict[str, Optional[Dict[str, str]]] = {}

    # level-based BFS for clean concurrency
    current_level = [(u, 0, urlparse(u).netloc) for u in expanded_start if is_valid_url(u)]
    pages_done = 0

    def process_page(url: str, root_netloc: str) -> Tuple[str, Optional[str], Set[str]]:
        html = fetch_html(sess, url, timeout=settings.timeout_seconds)
        if not html:
            return (url, None, set())
        links = extract_links_from_html(url, html)
        return (url, html, links)

    while current_level:
        if stop_cb():
            break

        # stop pages
        if pages_done >= settings.max_pages:
            break

        depth = current_level[0][1]

        # limit batch for max_pages
        batch = current_level[: max(1, settings.max_pages - pages_done)]
        current_level = current_level[len(batch):]

        if progress_cb:
            pct = int(min(99, (pages_done / max(1, settings.max_pages)) * 100))
            progress_cb(pct, f"Crawling depth={depth} | pages={pages_done}/{settings.max_pages} | files={len(files)}")

        # concurrent fetch
        next_urls: List[Tuple[str, int, str]] = []
        with ThreadPoolExecutor(max_workers=settings.workers) as ex:
            futs = {}
            for url, d, root in batch:
                if url in visited:
                    continue
                visited.add(url)
                futs[ex.submit(process_page, url, root)] = (url, d, root)

            for fut in as_completed(futs):
                if stop_cb():
                    break

                url, d, root = futs[fut]
                pages_done += 1
                try:
                    _, html, links = fut.result()
                except Exception:
                    continue

                if not links:
                    continue

                for link in links:
                    if stop_cb():
                        break

                    if settings.same_domain_only and urlparse(link).netloc != root:
                        continue

                    ext = normalize_ext(link)
                    filename = ""

                    # deep detect download endpoints without ext
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
                        continue

                    # crawl deeper
                    if d < settings.max_depth and link not in visited:
                        next_urls.append((link, d + 1, root))

                if len(files) >= settings.max_files or pages_done >= settings.max_pages:
                    break

        # push next urls to queue (de-dupe)
        if depth < settings.max_depth:
            # preserve order but remove dupes
            seen = set(u for u, _, _ in current_level)
            for u, d, r in next_urls:
                if u not in seen:
                    current_level.append((u, d, r))
                    seen.add(u)

        if len(files) >= settings.max_files:
            break

        if settings.delay_seconds:
            time.sleep(settings.delay_seconds)

    if progress_cb:
        progress_cb(100, f"Done. Pages={pages_done}, Found files={len(files)}")

    return files
