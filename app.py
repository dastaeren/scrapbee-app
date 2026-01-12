import os
import re
import io
import json
import time
import zipfile
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from urllib.parse import urlparse, urljoin, parse_qs, unquote

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup

# Optional dotenv
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


# =========================
# App Config
# =========================
APP_NAME = "ScrapBee"
TAGLINE = "Web Data Extraction Platform"
st.set_page_config(page_title=APP_NAME, layout="wide")


# =========================
# Styling
# =========================
CUSTOM_CSS = """
<style>
    :root{
        --sb-bg: #6495ED;
        --sb-accent: #4682B4;
        --sb-card: rgba(255,255,255,0.55);
        --sb-border: rgba(255,255,255,0.65);
        --sb-text: #0b1220;
    }
    html, body, [class*="css"]  { background: var(--sb-bg) !important; }
    .block-container { max-width: 1250px; padding-top: 1.2rem; padding-bottom: 2rem; }
    .sb-header {
        background: var(--sb-card);
        border: 1px solid var(--sb-border);
        border-radius: 16px;
        padding: 18px 22px;
        backdrop-filter: blur(8px);
        box-shadow: 0 16px 35px rgba(0,0,0,0.10);
        margin-bottom: 14px;
    }
    .sb-title { font-size: 40px; font-weight: 800; color: #0B3D91; margin: 0; line-height: 1.1; }
    .sb-tagline { font-size: 15px; color: var(--sb-text); margin-top: 6px; opacity: 0.9; }
    .sb-panel {
        background: var(--sb-card);
        border: 1px solid var(--sb-border);
        border-radius: 16px;
        padding: 16px;
        backdrop-filter: blur(8px);
        box-shadow: 0 16px 35px rgba(0,0,0,0.08);
    }
    section[data-testid="stSidebar"] {
        background: rgba(255,255,255,0.35) !important;
        border-right: 1px solid var(--sb-border) !important;
    }
    div.stButton > button, div.stDownloadButton > button {
        border-radius: 10px !important;
        padding: 0.65rem 1rem !important;
        font-weight: 700 !important;
        background: var(--sb-accent) !important;
        border: 1px solid rgba(255,255,255,0.55) !important;
        color: white !important;
    }
    div.stButton > button:hover, div.stDownloadButton > button:hover {
        filter: brightness(1.05);
        transform: translateY(-1px);
        transition: 180ms ease;
    }
    input[type="checkbox"]{ accent-color: var(--sb-accent) !important; }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


# =========================
# Helpers: state + logging
# =========================
def init_state():
    st.session_state.setdefault("history", [])
    st.session_state.setdefault("search_df", pd.DataFrame())
    st.session_state.setdefault("files_df", pd.DataFrame())
    st.session_state.setdefault("extract_rows", [])
    st.session_state.setdefault("extract_platform", "YouTube")
    st.session_state.setdefault(
        "extract_columns",
        ["Video Title", "Upload Date", "View Count", "Duration", "Channel Name", "Video URL"]
    )
    st.session_state.setdefault("stop_flag", False)
    st.session_state.setdefault("zip_bytes", None)

init_state()


def log(level: str, msg: str):
    st.session_state.history.append({
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "level": level,
        "msg": msg
    })


def stop_requested() -> bool:
    return bool(st.session_state.get("stop_flag", False))


@dataclass
class Settings:
    delay_seconds: float = 0.2
    timeout_seconds: int = 20
    max_pages_per_site: int = 10
    user_agent: str = "ScrapBee/1.0"
    deep_detect_downloads: bool = True  # <-- IMPORTANT for NSB /download/ID/


# =========================
# Secrets / Env
# =========================
def get_secret(name: str, default: str = "") -> str:
    try:
        v = st.secrets.get(name, None)
        if v:
            return str(v)
    except Exception:
        pass
    return os.getenv(name, default)

SERPER_API_KEY = get_secret("SERPER_API_KEY", "")
YOUTUBE_API_KEY = get_secret("YOUTUBE_API_KEY", "")


# =========================
# URL helpers
# =========================
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
    """WordPress Download Monitor & similar endpoints: /download/<id>/ or ?dlm_download=..."""
    u = urlparse(url)
    path = (u.path or "").lower()
    qs = parse_qs(u.query or "")
    if "/download/" in path:
        return True
    if "dlm_download" in qs or "download_id" in qs:
        return True
    if "dlm_download_category" in qs:
        return True
    return False


# =========================
# HTTP session
# =========================
def get_session(settings: Settings) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": settings.user_agent})
    return s


def fetch_html(url: str, settings: Settings, session: requests.Session) -> Optional[str]:
    try:
        r = session.get(url, timeout=settings.timeout_seconds)
        if r.status_code >= 400:
            return None
        ct = (r.headers.get("content-type") or "").lower()
        if "text/html" not in ct:
            return None
        return r.text
    except Exception:
        return None


def parse_content_disposition_filename(cd: str) -> Optional[str]:
    """
    Handles:
      Content-Disposition: attachment; filename="file.xlsx"
      Content-Disposition: attachment; filename*=UTF-8''file%20name.xlsx
    """
    if not cd:
        return None

    # filename*= form
    m = re.search(r"filename\*\s*=\s*([^']*)''([^;]+)", cd, flags=re.IGNORECASE)
    if m:
        enc = m.group(1) or "utf-8"
        fname = unquote(m.group(2))
        return fname.strip().strip('"').strip("'")

    # filename= form
    m = re.search(r'filename\s*=\s*"([^"]+)"', cd, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()

    m = re.search(r"filename\s*=\s*([^;]+)", cd, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip().strip('"').strip("'")

    return None


def guess_extension_from_headers(url: str, headers: dict) -> Tuple[str, str]:
    """
    Returns (filename, ext). ext includes leading dot or "".
    """
    cd = headers.get("content-disposition", "") or headers.get("Content-Disposition", "") or ""
    ct = (headers.get("content-type") or headers.get("Content-Type") or "").lower()

    filename = parse_content_disposition_filename(cd) or ""
    ext = ""

    if filename:
        ext = os.path.splitext(filename.lower())[1]
        if ext in DEFAULT_FILE_EXTS:
            return filename, ext

    # fallback on content-type
    ct_map = {
        "application/pdf": ".pdf",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
        "application/vnd.ms-excel": ".xls",
        "text/csv": ".csv",
        "application/zip": ".zip",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    }
    for k, v in ct_map.items():
        if k in ct:
            return (filename or ""), v

    # last fallback: url path ext
    ext2 = normalize_ext(url)
    return (filename or ""), ext2


def probe_file(url: str, settings: Settings, session: requests.Session) -> Optional[Dict[str, str]]:
    """
    HEAD first (fast). If blocked, GET with stream.
    Returns: {"filename": "...", "ext": ".xlsx", "final_url": "..."} or None
    """
    try:
        r = session.head(url, allow_redirects=True, timeout=settings.timeout_seconds)
        if r.status_code < 400:
            filename, ext = guess_extension_from_headers(r.url, r.headers)
            if ext:
                return {"filename": filename or os.path.basename(urlparse(r.url).path) or r.url,
                        "ext": ext,
                        "final_url": r.url}
    except Exception:
        pass

    # fallback GET (some servers don't allow HEAD)
    try:
        r = session.get(url, allow_redirects=True, stream=True, timeout=settings.timeout_seconds)
        if r.status_code >= 400:
            return None
        filename, ext = guess_extension_from_headers(r.url, r.headers)
        if ext:
            return {"filename": filename or os.path.basename(urlparse(r.url).path) or r.url,
                    "ext": ext,
                    "final_url": r.url}
    except Exception:
        return None

    return None


# =========================
# Serper Search (FIX: >10 results)
# =========================
def serper_search(query: str, num_results: int, timeout: int) -> pd.DataFrame:
    """
    FIX: paginate Serper so slider > 10 actually returns more.
    """
    if not SERPER_API_KEY:
        raise RuntimeError("SERPER_API_KEY is missing. Add it to .streamlit/secrets.toml or environment variables.")

    url = "https://google.serper.dev/search"
    headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}

    target = max(1, min(int(num_results), 200))  # keep reasonable for UI
    rows: List[Dict[str, Any]] = []

    page = 1
    while len(rows) < target:
        if stop_requested():
            break

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

        # small pause to be polite
        time.sleep(0.1)

    df = pd.DataFrame(rows).drop_duplicates(subset=["URL"]).reset_index(drop=True)
    if not df.empty:
        df["Title"] = df["Title"].fillna("")
        df["URL"] = df["URL"].fillna("")
        df["Snippet"] = df["Snippet"].fillna("")
    return df


# =========================
# Crawl + discover files (FIX: NSB Excel)
# =========================
def discover_files_from_sites(
    sites: List[str],
    exts: List[str],
    settings: Settings,
    session: requests.Session
) -> pd.DataFrame:
    exts = [e.lower().strip() for e in exts]
    found: List[Dict[str, Any]] = []
    probe_cache: Dict[str, Optional[Dict[str, str]]] = {}

    for site in sites:
        if stop_requested():
            log("WARN", "Stopped by user.")
            break

        if not is_valid_url(site):
            log("WARN", f"Skipping invalid site URL: {site}")
            continue

        log("INFO", f"Crawling site: {site}")
        visited = set()
        queue = [site]
        pages_crawled = 0

        root_netloc = urlparse(site).netloc

        while queue and pages_crawled < settings.max_pages_per_site:
            if stop_requested():
                log("WARN", "Stopped by user.")
                break

            cur = queue.pop(0)
            if cur in visited:
                continue
            visited.add(cur)

            html = fetch_html(cur, settings, session=session)
            pages_crawled += 1
            if settings.delay_seconds:
                time.sleep(settings.delay_seconds)

            if not html:
                continue

            soup = BeautifulSoup(html, "html.parser")
            for a in soup.select("a[href]"):
                href = (a.get("href") or "").strip()
                if not href:
                    continue

                abs_url = urljoin(cur, href)
                if not is_valid_url(abs_url):
                    continue

                # 1) direct extension in URL
                ext = normalize_ext(abs_url)

                # 2) if no ext BUT looks like download endpoint, probe headers (NSB fix)
                if not ext and settings.deep_detect_downloads and looks_like_download_endpoint(abs_url):
                    if abs_url not in probe_cache:
                        probe_cache[abs_url] = probe_file(abs_url, settings=settings, session=session)
                    probed = probe_cache.get(abs_url)
                    if probed:
                        ext = probed["ext"]
                        # use final_url for actual download
                        abs_url = probed["final_url"]
                        fname = probed["filename"]
                    else:
                        fname = ""
                else:
                    fname = ""

                if ext and (not exts or ext in exts):
                    found.append({
                        "Select": False,
                        "File": fname or os.path.basename(urlparse(abs_url).path) or abs_url,
                        "Type": ext,
                        "URL": abs_url,
                        "Source": site
                    })
                    continue

                # crawl deeper only inside same domain
                try:
                    if urlparse(abs_url).netloc == root_netloc:
                        if abs_url not in visited and abs_url not in queue:
                            queue.append(abs_url)
                except Exception:
                    pass

        log("OK", f"Finished crawling {site}. Pages crawled: {pages_crawled}")

    df = pd.DataFrame(found)
    if df.empty:
        return df
    df = df.drop_duplicates(subset=["URL"]).reset_index(drop=True)
    return df


# =========================
# Download selected files as ZIP
# =========================
def download_files_as_zip(urls: List[str], settings: Settings, session: requests.Session) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for u in urls:
            if stop_requested():
                break
            try:
                r = session.get(u, timeout=settings.timeout_seconds)
                r.raise_for_status()

                # Try filename from headers
                cd = r.headers.get("content-disposition", "") or r.headers.get("Content-Disposition", "")
                fname = parse_content_disposition_filename(cd) if cd else None
                name = fname or os.path.basename(urlparse(u).path) or f"file_{int(time.time())}"

                # avoid duplicates
                if name in zf.namelist():
                    base, ext = os.path.splitext(name)
                    name = f"{base}_{int(time.time())}{ext}"

                zf.writestr(name, r.content)

                if settings.delay_seconds:
                    time.sleep(settings.delay_seconds)
            except Exception:
                continue

    buf.seek(0)
    return buf.read()


# =========================
# Exporters
# =========================
def export_csv_bytes(rows: List[Dict[str, Any]], columns: List[str]) -> bytes:
    df = pd.DataFrame(rows)
    if columns:
        for c in columns:
            if c not in df.columns:
                df[c] = "N/A"
        df = df[columns]
    return df.to_csv(index=False).encode("utf-8")

def export_json_bytes(rows: List[Dict[str, Any]], columns: List[str]) -> bytes:
    if columns:
        rows2 = [{c: r.get(c, "N/A") for c in columns} for r in rows]
    else:
        rows2 = rows
    return json.dumps(rows2, indent=2, ensure_ascii=False).encode("utf-8")

def export_xlsx_bytes(rows: List[Dict[str, Any]], columns: List[str], meta: Dict[str, Any]) -> bytes:
    out = io.BytesIO()
    df = pd.DataFrame(rows)
    if columns:
        for c in columns:
            if c not in df.columns:
                df[c] = "N/A"
        df = df[columns]

    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Data")
        meta_df = pd.DataFrame([{"Key": k, "Value": str(v)} for k, v in meta.items()])
        meta_df.to_excel(writer, index=False, sheet_name="Metadata")
    out.seek(0)
    return out.read()

def export_sqlite_bytes(rows: List[Dict[str, Any]], columns: List[str]) -> bytes:
    df = pd.DataFrame(rows)
    if columns:
        for c in columns:
            if c not in df.columns:
                df[c] = "N/A"
        df = df[columns]

    con = sqlite3.connect(":memory:")
    df.to_sql("data", con, if_exists="replace", index=False)

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    con2 = sqlite3.connect(path)
    con.backup(con2)
    con2.close()
    con.close()

    with open(path, "rb") as f:
        data = f.read()
    try:
        os.remove(path)
    except Exception:
        pass
    return data

def default_filename(prefix: str, ext: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}.{ext}"


# =========================
# UI Header
# =========================
st.markdown(
    f"""
    <div class="sb-header">
        <div class="sb-title">{APP_NAME}</div>
        <div class="sb-tagline">{TAGLINE}</div>
    </div>
    """,
    unsafe_allow_html=True
)


# =========================
# Sidebar Controls
# =========================
st.sidebar.markdown("## Controls")
delay = st.sidebar.slider("Request delay (seconds)", 0.0, 3.0, 0.2, 0.05)
max_pages = st.sidebar.slider("Max pages per site (crawl)", 1, 50, 10, 1)
timeout = st.sidebar.slider("Timeout (seconds)", 5, 60, 20, 1)
deep_detect = st.sidebar.checkbox("Deep-detect download links (needed for NSB Excel)", value=True)

settings = Settings(
    delay_seconds=float(delay),
    timeout_seconds=int(timeout),
    max_pages_per_site=int(max_pages),
    deep_detect_downloads=bool(deep_detect),
)

session = get_session(settings)

st.sidebar.markdown("---")
st.sidebar.markdown("## Search settings")
search_k = st.sidebar.slider("Number of search results", 1, 200, 30, 10)

st.sidebar.markdown("---")
st.sidebar.markdown("## File types")
exts = st.sidebar.multiselect(
    "Extensions to find",
    DEFAULT_FILE_EXTS,
    default=[".pdf", ".xlsx", ".xls", ".csv", ".docx", ".pptx"]
)

st.sidebar.markdown("---")
col_export = st.sidebar.selectbox(
    "Export format (data)",
    ["xlsx", "csv", "json", "sqlite"],
    index=0
)

st.sidebar.markdown("---")
if st.sidebar.button("Stop"):
    st.session_state.stop_flag = True
    log("WARN", "Stop requested.")
if st.sidebar.button("Reset"):
    st.session_state.stop_flag = False
    st.session_state.search_df = pd.DataFrame()
    st.session_state.files_df = pd.DataFrame()
    st.session_state.extract_rows = []
    st.session_state.zip_bytes = None
    log("INFO", "Reset completed.")


# =========================
# History
# =========================
with st.expander("History", expanded=False):
    if st.session_state.history:
        hdf = pd.DataFrame(st.session_state.history)
        st.dataframe(hdf, use_container_width=True, hide_index=True)
    else:
        st.info("No history yet.")


# =========================
# Tabs
# =========================
tab_search, tab_extract = st.tabs(["Site Search", "Data Extractor"])


# =========================
# TAB 1: Search & Download
# =========================
with tab_search:
    st.markdown('<div class="sb-panel">', unsafe_allow_html=True)

    st.markdown("### Option A — Paste URLs directly (recommended for NSB)")
    manual_urls = st.text_area(
        "Manual site/page URLs (one per line)",
        placeholder="Example:\nhttps://www.nsb.gov.bt/?dlm_download_category=ialc-excel",
        height=80
    )

    st.markdown("---")
    st.markdown("### Option B — Search the web (Serper)")
    query = st.text_input("Search query", value="", placeholder="Example: NSB Bhutan IALC excel download")

    colA, colB = st.columns([1, 1])
    with colA:
        do_search = st.button("Search Websites", use_container_width=True)
    with colB:
        do_crawl = st.button("Crawl Selected/Manual Sites (Find Files)", use_container_width=True)

    # Search
    if do_search:
        st.session_state.stop_flag = False
        if not query.strip():
            st.warning("Please enter a search query.")
        else:
            try:
                log("INFO", f"Searching web: {query} (results={search_k})")
                df = serper_search(query.strip(), num_results=int(search_k), timeout=settings.timeout_seconds)
                st.info(f"Search results returned: {len(df)}")
                st.session_state.search_df = df
            except Exception as e:
                log("ERROR", str(e))
                st.error(str(e))

    selected_sites = []

    # show search results
    if not st.session_state.search_df.empty:
        st.markdown("### Search results (tick Select to choose sites)")
        sdf = st.session_state.search_df.copy()
        edited = st.data_editor(
            sdf,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Select": st.column_config.CheckboxColumn("Select", default=False),
                "URL": st.column_config.LinkColumn("URL", display_text="Open link"),
            },
            disabled=["Title", "URL", "Snippet"]
        )
        st.session_state.search_df = edited
        selected_sites = edited.loc[edited["Select"] == True, "URL"].dropna().tolist()
        st.write(f"Selected from search: **{len(selected_sites)}**")

    # add manual urls
    manual_list = [u.strip() for u in manual_urls.splitlines() if u.strip()]
    manual_list = [u for u in manual_list if is_valid_url(u)]

    if manual_list:
        st.write(f"Manual URLs: **{len(manual_list)}**")

    combined_sites = list(dict.fromkeys(selected_sites + manual_list))
    st.write(f"Total sites/pages to crawl: **{len(combined_sites)}**")

    # Crawl
    if do_crawl:
        st.session_state.stop_flag = False
        if not combined_sites:
            st.warning("Add at least one manual URL or select a website from search.")
        else:
            prog = st.progress(0, text="Crawling...")
            try:
                log("INFO", f"Crawling {len(combined_sites)} sites/pages...")
                df_files = discover_files_from_sites(combined_sites, exts, settings=settings, session=session)
                st.session_state.files_df = df_files
                if df_files.empty:
                    log("WARN", "No files found.")
                    st.warning("No matching files found. Try enabling deep-detect and include .xlsx/.xls/.csv in extensions.")
                else:
                    log("OK", f"Files found: {len(df_files)}")
                    st.success(f"Found {len(df_files)} files.")
                prog.progress(100, text="Done.")
            except Exception as e:
                log("ERROR", str(e))
                st.error(str(e))
            finally:
                prog.empty()

    # Files + ZIP
    if not st.session_state.files_df.empty:
        st.markdown("---")
        st.markdown("### Select files to download")
        fdf = st.session_state.files_df.copy()

        fedited = st.data_editor(
            fdf,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Select": st.column_config.CheckboxColumn("Select", default=False),
                "URL": st.column_config.LinkColumn("URL", display_text="Open file"),
            },
            disabled=["File", "Type", "URL", "Source"]
        )
        st.session_state.files_df = fedited

        selected_files = fedited.loc[fedited["Select"] == True, "URL"].dropna().tolist()
        st.write(f"Selected files: **{len(selected_files)}**")

        col1, col2 = st.columns([1, 1])
        with col1:
            if st.button("Prepare ZIP (Selected Files)", use_container_width=True):
                st.session_state.stop_flag = False
                if not selected_files:
                    st.warning("Select at least one file.")
                else:
                    prog = st.progress(0, text="Preparing ZIP...")
                    log("INFO", f"Preparing ZIP for {len(selected_files)} files...")
                    zip_bytes = download_files_as_zip(selected_files, settings=settings, session=session)
                    st.session_state.zip_bytes = zip_bytes
                    prog.progress(100, text="ZIP ready.")
                    st.success("ZIP is ready.")
                    prog.empty()

        with col2:
            if st.session_state.zip_bytes:
                st.download_button(
                    "Download ZIP",
                    data=st.session_state.zip_bytes,
                    file_name=default_filename("ScrapBee_Files", "zip"),
                    mime="application/zip",
                    use_container_width=True
                )

    st.markdown("</div>", unsafe_allow_html=True)


# =========================
# TAB 2: Data Extractor (kept simple placeholder)
# =========================
with tab_extract:
    st.markdown('<div class="sb-panel">', unsafe_allow_html=True)
    st.info("Your YouTube extractor section can be pasted back here unchanged if you want. "
            "This answer focused on fixing NSB Excel crawling + search results.")
    st.markdown("</div>", unsafe_allow_html=True)


# Footer warnings
if not SERPER_API_KEY:
    st.warning("SERPER_API_KEY is missing. Web search will not work until you add it to secrets.toml or env vars.")
if not YOUTUBE_API_KEY:
    st.info("YOUTUBE_API_KEY missing (only needed if you add the YouTube extractor back).")
