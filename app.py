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
from typing import Optional, List, Dict, Any
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
    .sb-header{
        background: var(--sb-card);
        border: 1px solid var(--sb-border);
        border-radius: 16px;
        padding: 18px 22px;
        backdrop-filter: blur(8px);
        box-shadow: 0 16px 35px rgba(0,0,0,0.10);
        margin-bottom: 14px;
    }
    .sb-title{ font-size: 40px; font-weight: 800; color: #0B3D91; margin: 0; line-height: 1.1; }
    .sb-tagline{ font-size: 15px; color: var(--sb-text); margin-top: 6px; opacity: 0.9; }
    .sb-panel{
        background: var(--sb-card);
        border: 1px solid var(--sb-border);
        border-radius: 16px;
        padding: 16px;
        backdrop-filter: blur(8px);
        box-shadow: 0 16px 35px rgba(0,0,0,0.08);
    }
    section[data-testid="stSidebar"]{
        background: rgba(255,255,255,0.35) !important;
        border-right: 1px solid var(--sb-border) !important;
    }
    div.stButton > button, div.stDownloadButton > button{
        border-radius: 10px !important;
        padding: 0.65rem 1rem !important;
        font-weight: 700 !important;
        background: var(--sb-accent) !important;
        border: 1px solid rgba(255,255,255,0.55) !important;
        color: white !important;
    }
    div.stButton > button:hover, div.stDownloadButton > button:hover{
        filter: brightness(1.05);
        transform: translateY(-1px);
        transition: 180ms ease;
    }
    input[type="checkbox"]{ accent-color: var(--sb-accent) !important; }
    .streamlit-expanderHeader{ font-weight: 700; }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


# =========================
# Session State + Logging
# =========================
def init_state():
    st.session_state.setdefault("history", [])
    st.session_state.setdefault("search_df", pd.DataFrame())
    st.session_state.setdefault("files_df", pd.DataFrame())
    st.session_state.setdefault("extract_rows", [])
    st.session_state.setdefault("extract_platform", "YouTube")
    st.session_state.setdefault("extract_columns", [])
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


# =========================
# Settings
# =========================
@dataclass
class Settings:
    delay_seconds: float = 0.25
    timeout_seconds: int = 15
    max_pages_per_site: int = 10
    user_agent: str = "ScrapBee/1.0 (+local)"
    max_queue: int = 300
    use_head_detect: bool = False  # optional: slower but finds file URLs without extensions


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
# Utility
# =========================
DEFAULT_FILE_EXTS = [
    ".pdf", ".docx", ".pptx", ".xlsx", ".xls", ".csv", ".txt", ".rtf",
    ".jpg", ".jpeg", ".png", ".gif",
    ".mp4", ".mp3", ".wav", ".avi",
    ".json", ".xml", ".zip", ".rar", ".7z"
]

CONTENT_TYPE_TO_EXT = {
    "application/pdf": ".pdf",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.ms-excel": ".xls",
    "text/csv": ".csv",
    "application/zip": ".zip",
    "application/json": ".json",
    "application/xml": ".xml",
    "text/xml": ".xml",
    "text/plain": ".txt",
}

def is_valid_url(u: str) -> bool:
    try:
        p = urlparse(u.strip())
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False

def same_domain(a: str, b: str) -> bool:
    try:
        return urlparse(a).netloc == urlparse(b).netloc
    except Exception:
        return False

def normalize_ext(url: str) -> str:
    """
    Try to infer extension from:
    - path ending (.xlsx)
    - query params (file=data.xlsx)
    """
    try:
        u = urlparse(url)
        path = (u.path or "").lower()

        # direct path ext
        for ext in DEFAULT_FILE_EXTS:
            if path.endswith(ext):
                return ext

        # query params
        qs = parse_qs(u.query)
        for key, vals in qs.items():
            for v in vals:
                v = unquote(v).lower()
                for ext in DEFAULT_FILE_EXTS:
                    if v.endswith(ext):
                        return ext

        return ""
    except Exception:
        return ""

def infer_ext_from_content_type(ct: str) -> str:
    if not ct:
        return ""
    ct = ct.split(";")[0].strip().lower()
    return CONTENT_TYPE_TO_EXT.get(ct, "")

def safe_filename_from_url(u: str) -> str:
    try:
        name = os.path.basename(urlparse(u).path)
        if name:
            return name
        return f"file_{int(time.time())}"
    except Exception:
        return f"file_{int(time.time())}"

def fetch_html(url: str, settings: Settings) -> Optional[str]:
    try:
        headers = {"User-Agent": settings.user_agent}
        r = requests.get(url, headers=headers, timeout=settings.timeout_seconds)
        if r.status_code >= 400:
            return None
        ct = (r.headers.get("content-type") or "").lower()
        if "text/html" not in ct:
            return None
        return r.text
    except Exception:
        return None

def head_detect_file(url: str, settings: Settings) -> str:
    """
    Optional: HEAD request to detect file type when URL has no extension.
    Returns inferred extension or "".
    """
    try:
        headers = {"User-Agent": settings.user_agent}
        r = requests.head(url, headers=headers, timeout=settings.timeout_seconds, allow_redirects=True)
        ct = r.headers.get("content-type") or ""
        return infer_ext_from_content_type(ct)
    except Exception:
        return ""


# =========================
# Serper: Website search (FIXES "only 10")
# =========================
def serper_search_websites(query: str, num_results: int, timeout: int) -> pd.DataFrame:
    """
    Returns up to num_results website results by paginating Serper with 'page'.
    """
    if not SERPER_API_KEY:
        raise RuntimeError("SERPER_API_KEY is missing. Add it to secrets.toml or env.")

    url = "https://google.serper.dev/search"
    headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}

    target = min(max(int(num_results), 1), 100)
    rows = []
    page = 1

    while len(rows) < target and page <= 10:
        payload = {"q": query, "num": min(10, target - len(rows)), "page": page}
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

        page += 1

    df = pd.DataFrame(rows).drop_duplicates(subset=["URL"]).reset_index(drop=True)
    return df


# =========================
# Serper: FAST file search for a domain (BEST for NSB)
# =========================
def serper_find_files(domain_or_site_url: str, exts: List[str], max_results: int, timeout: int) -> pd.DataFrame:
    if not SERPER_API_KEY:
        raise RuntimeError("SERPER_API_KEY is missing.")

    s = domain_or_site_url.strip()
    if s.startswith("http"):
        dom = urlparse(s).netloc
    else:
        dom = s.replace("https://", "").replace("http://", "").split("/")[0]

    ftypes = []
    for e in exts:
        e = e.lower().strip().lstrip(".")
        if e:
            ftypes.append(f"filetype:{e}")
    if not ftypes:
        ftypes = ["filetype:xlsx", "filetype:xls", "filetype:csv", "filetype:zip", "filetype:pdf"]

    q = f"site:{dom} ({' OR '.join(ftypes)})"

    url = "https://google.serper.dev/search"
    headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}

    target = min(max(int(max_results), 1), 100)
    rows = []
    page = 1

    while len(rows) < target and page <= 10:
        payload = {"q": q, "num": min(10, target - len(rows)), "page": page}
        r = requests.post(url, headers=headers, json=payload, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        organic = data.get("organic", []) or []
        if not organic:
            break

        for item in organic:
            link = (item.get("link") or "").strip()
            title = (item.get("title") or "").strip()
            if not link:
                continue

            ext = normalize_ext(link) or "unknown"
            rows.append({
                "Select": False,
                "File": os.path.basename(urlparse(link).path) or title or link,
                "Type": ext,
                "URL": link,
                "Source": dom
            })

        page += 1

    df = pd.DataFrame(rows).drop_duplicates(subset=["URL"]).reset_index(drop=True)
    return df


# =========================
# Crawl + discover files (improved)
# =========================
def discover_files_from_sites(sites: List[str], exts: List[str], settings: Settings) -> pd.DataFrame:
    exts = [e.lower().strip() for e in exts]
    found: List[Dict[str, Any]] = []

    for site in sites:
        if stop_requested():
            log("WARN", "Stopped by user.")
            break

        if not is_valid_url(site):
            log("WARN", f"Skipping invalid site URL: {site}")
            continue

        log("INFO", f"Crawling: {site}")
        visited = set()
        queue = [site]
        pages_crawled = 0

        while queue and pages_crawled < settings.max_pages_per_site and len(queue) < settings.max_queue:
            if stop_requested():
                log("WARN", "Stopped by user.")
                break

            cur = queue.pop(0)
            if cur in visited:
                continue
            visited.add(cur)

            html = fetch_html(cur, settings)
            pages_crawled += 1
            if settings.delay_seconds > 0:
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

                # file by extension or query params
                ext = normalize_ext(abs_url)

                # optional: detect by content-type if no ext
                if (not ext) and settings.use_head_detect:
                    ext = head_detect_file(abs_url, settings)

                if ext and (not exts or ext in exts):
                    found.append({
                        "Select": False,
                        "File": safe_filename_from_url(abs_url),
                        "Type": ext,
                        "URL": abs_url,
                        "Source": site
                    })
                else:
                    # keep crawling only within same domain
                    if same_domain(abs_url, site) and abs_url not in visited and abs_url not in queue:
                        queue.append(abs_url)

        log("OK", f"Finished crawling {site}. Pages crawled: {pages_crawled}")

    df = pd.DataFrame(found)
    if df.empty:
        return df

    df = df.drop_duplicates(subset=["URL"]).reset_index(drop=True)
    return df


# =========================
# Download selected files as ZIP
# =========================
def download_files_as_zip(urls: List[str], settings: Settings) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for u in urls:
            if stop_requested():
                break
            try:
                headers = {"User-Agent": settings.user_agent}
                r = requests.get(u, headers=headers, timeout=settings.timeout_seconds)
                r.raise_for_status()
                name = safe_filename_from_url(u)

                if name in zf.namelist():
                    base, ext = os.path.splitext(name)
                    name = f"{base}_{int(time.time())}{ext}"

                zf.writestr(name, r.content)
                if settings.delay_seconds > 0:
                    time.sleep(settings.delay_seconds)
            except Exception:
                continue
    buf.seek(0)
    return buf.read()


# =========================
# Export helpers
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

delay = st.sidebar.slider("Request delay (seconds)", 0.0, 2.0, 0.25, 0.05)
max_pages = st.sidebar.slider("Max pages per site (crawl)", 1, 80, 12, 1)
timeout = st.sidebar.slider("Timeout (seconds)", 5, 60, 15, 1)
use_head = st.sidebar.checkbox("Use HEAD detect (slower, finds hidden file types)", value=False)

settings = Settings(
    delay_seconds=float(delay),
    timeout_seconds=int(timeout),
    max_pages_per_site=int(max_pages),
    use_head_detect=bool(use_head)
)

st.sidebar.markdown("---")
st.sidebar.markdown("## Search settings")
search_k = st.sidebar.slider("Number of search results", 10, 100, 30, 10)

st.sidebar.markdown("---")
st.sidebar.markdown("## File types")
exts = st.sidebar.multiselect(
    "Extensions to find",
    DEFAULT_FILE_EXTS,
    default=[".pdf", ".xlsx", ".xls", ".csv", ".zip", ".docx", ".pptx"]
)

st.sidebar.markdown("---")
col_export = st.sidebar.selectbox("Export format (data)", ["xlsx", "csv", "json", "sqlite"], index=0)

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
        st.dataframe(pd.DataFrame(st.session_state.history), use_container_width=True, hide_index=True)
    else:
        st.info("No history yet.")


# =========================
# Tabs
# =========================
tab_search = st.tabs(["Site Search & File Finder"])[0]

with tab_search:
    st.markdown('<div class="sb-panel">', unsafe_allow_html=True)

    st.markdown("### Step 1 — Find websites OR find files directly (FAST)")
    st.write(
        "✅ **Best for NSB / JS-heavy websites:** Use **Find Files via Google (FAST)**.\n\n"
        "Crawl works only if the page HTML contains the file links (not loaded by JavaScript)."
    )

    query = st.text_input("Search query OR domain", value="", placeholder="Example: nsb.gov.bt OR Bhutan agriculture report")

    colA, colB, colC = st.columns([1, 1, 1])
    with colA:
        do_search = st.button("Search Websites", use_container_width=True)
    with colB:
        do_fast_files = st.button("Find Files via Google (FAST)", use_container_width=True)
    with colC:
        do_crawl = st.button("Crawl Selected Sites (HTML Crawl)", use_container_width=True)

    # Website search
    if do_search:
        st.session_state.stop_flag = False
        if not query.strip():
            st.warning("Please enter a search query.")
        else:
            try:
                log("INFO", f"Searching websites: {query} (results={search_k})")
                df = serper_search_websites(query.strip(), num_results=int(search_k), timeout=settings.timeout_seconds)
                st.session_state.search_df = df
                st.info(f"Website results returned: {len(df)}")
                if df.empty:
                    st.warning("No results found.")
                else:
                    log("OK", f"Website results: {len(df)}")
            except Exception as e:
                log("ERROR", str(e))
                st.error(str(e))

    # Show website results
    if not st.session_state.search_df.empty:
        st.markdown("### Website search results (tick Select)")
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
        st.write(f"Selected websites: **{len(selected_sites)}**")
    else:
        selected_sites = []

    # FAST file finder (best)
    if do_fast_files:
        st.session_state.stop_flag = False

        # If user selected websites, use those. Otherwise use query as domain.
        targets = selected_sites if selected_sites else ([query.strip()] if query.strip() else [])
        if not targets:
            st.warning("Select a website OR type a domain like: nsb.gov.bt")
        else:
            all_files = []
            prog = st.progress(0, text="Finding files via Google (FAST)...")
            try:
                for i, t in enumerate(targets, start=1):
                    if stop_requested():
                        break
                    log("INFO", f"FAST file search: {t}")
                    df_files = serper_find_files(t, exts, max_results=int(search_k), timeout=settings.timeout_seconds)
                    all_files.append(df_files)
                    prog.progress(int((i / max(1, len(targets))) * 100), text=f"Processed {i}/{len(targets)}")
                prog.progress(100, text="Done.")
            finally:
                prog.empty()

            if all_files:
                out = pd.concat(all_files, ignore_index=True).drop_duplicates(subset=["URL"]).reset_index(drop=True)
                st.session_state.files_df = out
                if out.empty:
                    st.warning("No files found. Try adding .zip/.csv/.xlsx and increase search results.")
                else:
                    st.success(f"Found {len(out)} files (FAST mode).")

    # HTML crawl (slower + may fail for JS pages)
    if do_crawl:
        st.session_state.stop_flag = False
        if not selected_sites:
            st.warning("Select at least one website (tick Select) from website results first.")
        else:
            prog = st.progress(0, text="Crawling selected sites (HTML)...")
            try:
                log("INFO", f"Crawling {len(selected_sites)} sites (HTML crawl)...")
                df_files = discover_files_from_sites(selected_sites, exts, settings=settings)
                st.session_state.files_df = df_files
                prog.progress(100, text="Crawl completed.")
                if df_files.empty:
                    st.warning("No files found by HTML crawl. For NSB, use FAST mode.")
                else:
                    st.success(f"Found {len(df_files)} files by crawl.")
            except Exception as e:
                log("ERROR", str(e))
                st.error(str(e))
            finally:
                prog.empty()

    # Show found files
    if not st.session_state.files_df.empty:
        st.markdown("---")
        st.markdown("### Step 2 — Select files to download")

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

        st.markdown("### Step 3 — Download ZIP")
        col1, col2 = st.columns([1, 1])

        with col1:
            if st.button("Prepare ZIP (Selected Files)", use_container_width=True):
                st.session_state.stop_flag = False
                if not selected_files:
                    st.warning("Select at least one file.")
                else:
                    prog = st.progress(0, text="Preparing ZIP...")
                    try:
                        log("INFO", f"Zipping {len(selected_files)} files...")
                        st.session_state.zip_bytes = download_files_as_zip(selected_files, settings=settings)
                        prog.progress(100, text="ZIP ready.")
                        st.success("ZIP ready.")
                    finally:
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

    else:
        st.info("No files yet. Tip: For NSB, use **Find Files via Google (FAST)**.")

    st.markdown("</div>", unsafe_allow_html=True)

# Footer warnings
if not SERPER_API_KEY:
    st.warning("SERPER_API_KEY is missing. Web search / FAST file search won't work until you set it.")
