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
from urllib.parse import urlparse, urljoin, parse_qs

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
    div[data-baseweb="progress-bar"] > div{ background-color: var(--sb-accent) !important; }
    input[type="checkbox"]{ accent-color: var(--sb-accent) !important; }
    .streamlit-expanderHeader { font-weight: 700; }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


# =========================
# State + logging
# =========================
def init_state():
    st.session_state.setdefault("history", [])
    st.session_state.setdefault("search_df", pd.DataFrame())
    st.session_state.setdefault("files_df", pd.DataFrame())
    st.session_state.setdefault("extract_rows", [])
    st.session_state.setdefault("extract_platform", "YouTube")
    st.session_state.setdefault("extract_columns", [
        "Video Title", "Upload Date", "View Count", "Duration", "Channel Name", "Video URL"
    ])
    st.session_state.setdefault("stop_flag", False)
    st.session_state.setdefault("zip_bytes", None)

    # ✅ ADDED: allow API keys to be pasted in UI
    st.session_state.setdefault("SERPER_API_KEY_UI", "")
    st.session_state.setdefault("YOUTUBE_API_KEY_UI", "")

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
    delay_seconds: float = 1.5
    timeout_seconds: int = 20
    max_pages_per_site: int = 10
    user_agent: str = "ScrapBee/1.0"
    max_head_checks_per_page: int = 40  # avoid too many HEAD/GET header checks


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
# Search (Serper) - FIX: paginate beyond 10 results
# =========================
def serper_search_paginated(query: str, total_results: int, timeout: int, settings: Settings) -> pd.DataFrame:
    """
    Serper often returns ~10 organic results per page for some accounts/plans.
    This function paginates using "page" and aggregates until total_results is reached.
    """
    if not SERPER_API_KEY:
        raise RuntimeError("SERPER_API_KEY is missing. Add it to .streamlit/secrets.toml or environment variables.")

    url = "https://google.serper.dev/search"
    headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}

    rows: List[Dict[str, Any]] = []
    seen = set()

    target = max(1, min(int(total_results), 200))  # keep sane
    page = 1

    while len(rows) < target and page <= 20:  # up to 20 pages safeguard
        if stop_requested():
            break

        remaining = target - len(rows)
        per_page = min(100, remaining)

        payload = {"q": query, "num": per_page, "page": page}

        r = requests.post(url, headers=headers, json=payload, timeout=timeout)
        r.raise_for_status()
        data = r.json()

        organic = data.get("organic", []) or []
        if not organic:
            break

        added_this_page = 0
        for item in organic:
            link = item.get("link", "") or ""
            if not link or link in seen:
                continue
            seen.add(link)
            rows.append({
                "Select": False,
                "Title": item.get("title", "") or "",
                "URL": link,
                "Snippet": item.get("snippet", "") or ""
            })
            added_this_page += 1
            if len(rows) >= target:
                break

        # if nothing new added, stop (prevents infinite loop)
        if added_this_page == 0:
            break

        page += 1
        time.sleep(settings.delay_seconds)

    df = pd.DataFrame(rows)
    if not df.empty:
        df["Title"] = df["Title"].fillna("")
        df["URL"] = df["URL"].fillna("")
        df["Snippet"] = df["Snippet"].fillna("")
    return df


# =========================
# Crawl + discover files (FIX: detect files via headers too)
# =========================
DEFAULT_FILE_EXTS = [
    ".pdf",
    ".doc", ".docx",
    ".ppt", ".pptx",
    ".xls", ".xlsx", ".xlsm", ".xlsb",
    ".csv", ".txt", ".rtf",
    ".jpg", ".jpeg", ".png", ".gif", ".webp",
    ".mp4", ".mp3", ".wav", ".avi",
    ".json", ".xml",
    ".zip", ".rar", ".7z"
]

CONTENT_TYPE_TO_EXT = {
    "application/pdf": ".pdf",
    "text/csv": ".csv",
    "application/csv": ".csv",
    "application/json": ".json",
    "application/xml": ".xml",
    "text/xml": ".xml",
    "application/zip": ".zip",

    # Excel
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.ms-excel": ".xls",
    "application/vnd.ms-excel.sheet.macroenabled.12": ".xlsm",
    "application/vnd.ms-excel.sheet.binary.macroenabled.12": ".xlsb",

    # Word
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/msword": ".doc",

    # PowerPoint
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "application/vnd.ms-powerpoint": ".ppt",
}

def is_valid_url(u: str) -> bool:
    try:
        p = urlparse(u.strip())
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False

def normalize_ext_from_path(url: str) -> str:
    path = urlparse(url).path.lower()
    for ext in DEFAULT_FILE_EXTS:
        if path.endswith(ext):
            return ext
    return ""

def guess_ext_from_headers(url: str, settings: Settings) -> str:
    """
    If URL doesn't end with .xlsx/.csv etc, try HEAD/GET to detect Content-Type / Content-Disposition.
    """
    headers = {"User-Agent": settings.user_agent}

    def parse_content_disposition(cd: str) -> str:
        # filename="abc.xlsx" OR filename*=UTF-8''abc.xlsx
        if not cd:
            return ""
        m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', cd, flags=re.IGNORECASE)
        if m:
            fn = m.group(1).strip()
            _, ext = os.path.splitext(fn)
            return ext.lower()
        return ""

    try:
        r = requests.head(url, headers=headers, timeout=settings.timeout_seconds, allow_redirects=True)
        ct = (r.headers.get("content-type") or "").split(";")[0].strip().lower()
        cd = r.headers.get("content-disposition") or ""
        ext_cd = parse_content_disposition(cd)
        if ext_cd in DEFAULT_FILE_EXTS:
            return ext_cd
        if ct in CONTENT_TYPE_TO_EXT:
            return CONTENT_TYPE_TO_EXT[ct]
    except Exception:
        pass

    # fallback to GET headers (still no full download)
    try:
        r = requests.get(url, headers=headers, timeout=settings.timeout_seconds, stream=True, allow_redirects=True)
        ct = (r.headers.get("content-type") or "").split(";")[0].strip().lower()
        cd = r.headers.get("content-disposition") or ""
        ext_cd = parse_content_disposition(cd)
        if ext_cd in DEFAULT_FILE_EXTS:
            return ext_cd
        if ct in CONTENT_TYPE_TO_EXT:
            return CONTENT_TYPE_TO_EXT[ct]
    except Exception:
        pass

    return ""

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

def discover_files_from_sites(
    sites: List[str],
    exts: List[str],
    settings: Settings
) -> pd.DataFrame:
    exts = [e.lower().strip() for e in exts]
    found: List[Dict[str, Any]] = []

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

        site_netloc = urlparse(site).netloc

        while queue and pages_crawled < settings.max_pages_per_site:
            if stop_requested():
                log("WARN", "Stopped by user.")
                break

            cur = queue.pop(0)
            if cur in visited:
                continue
            visited.add(cur)

            html = fetch_html(cur, settings)
            pages_crawled += 1
            time.sleep(settings.delay_seconds)

            if not html:
                continue

            soup = BeautifulSoup(html, "html.parser")

            head_checks_used = 0

            for a in soup.select("a[href]"):
                href = (a.get("href") or "").strip()
                if not href:
                    continue

                abs_url = urljoin(cur, href)
                if not is_valid_url(abs_url):
                    continue

                # If direct extension exists in URL path
                ext = normalize_ext_from_path(abs_url)

                # If not in URL path, try header detection (limited per page)
                if not ext and head_checks_used < settings.max_head_checks_per_page:
                    # cheap heuristic: only check likely download links
                    hlow = href.lower()
                    if any(k in hlow for k in ["download", "attachment", "file", "export", "xls", "xlsx", "csv"]):
                        ext = guess_ext_from_headers(abs_url, settings)
                        head_checks_used += 1

                if ext and (not exts or ext in exts):
                    found.append({
                        "Select": False,
                        "File": os.path.basename(urlparse(abs_url).path) or abs_url,
                        "Type": ext,
                        "URL": abs_url,
                        "Source": site
                    })
                    continue

                # continue crawling inside same domain
                try:
                    u = urlparse(abs_url)
                    if u.netloc == site_netloc:
                        if abs_url not in visited and abs_url not in queue:
                            queue.append(abs_url)
                except Exception:
                    pass

            log("INFO", f"Page crawled: {cur} | queue={len(queue)} | files_found={len(found)}")

        log("OK", f"Finished crawling {site}. Pages crawled: {pages_crawled}")

    df = pd.DataFrame(found)
    if df.empty:
        return df
    return df.drop_duplicates(subset=["URL"]).reset_index(drop=True)


# =========================
# Download selected files as ZIP
# =========================
def download_files_as_zip(urls: List[str], settings: Settings) -> bytes:
    buf = io.BytesIO()
    headers = {"User-Agent": settings.user_agent}

    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for u in urls:
            if stop_requested():
                break
            try:
                r = requests.get(u, headers=headers, timeout=settings.timeout_seconds)
                r.raise_for_status()

                name = os.path.basename(urlparse(u).path) or f"file_{int(time.time())}"
                # if no extension in path, try Content-Disposition
                cd = r.headers.get("content-disposition") or ""
                m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', cd, flags=re.IGNORECASE)
                if m:
                    name = m.group(1).strip()

                if name in zf.namelist():
                    base, ext = os.path.splitext(name)
                    name = f"{base}_{int(time.time())}{ext}"

                zf.writestr(name, r.content)
                time.sleep(settings.delay_seconds)
            except Exception:
                continue

    buf.seek(0)
    return buf.read()


# =========================
# YouTube helpers (unchanged)
# =========================
def _iso8601_duration_to_hms(d: str) -> str:
    if not d or not isinstance(d, str):
        return "N/A"
    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", d)
    if not m:
        return d
    h = int(m.group(1) or 0)
    mi = int(m.group(2) or 0)
    s = int(m.group(3) or 0)
    return f"{h:02d}:{mi:02d}:{s:02d}"

def is_youtube_url(s: str) -> bool:
    try:
        u = urlparse(s.strip())
        return u.scheme in ("http", "https") and ("youtube.com" in u.netloc or "youtu.be" in u.netloc)
    except Exception:
        return False

def parse_youtube_video_id(url: str) -> Optional[str]:
    try:
        u = urlparse(url.strip())
        if "youtu.be" in u.netloc:
            vid = u.path.strip("/").split("/")[0]
            return vid or None
        if "youtube.com" in u.netloc:
            qs = parse_qs(u.query)
            if "v" in qs and qs["v"]:
                return qs["v"][0]
            parts = [p for p in u.path.split("/") if p]
            if len(parts) >= 2 and parts[0] in ("shorts", "embed"):
                return parts[1]
        return None
    except Exception:
        return None

def parse_youtube_channel_id(url: str) -> Optional[str]:
    try:
        u = urlparse(url.strip())
        if "youtube.com" not in u.netloc:
            return None
        parts = [p for p in u.path.split("/") if p]
        if parts and parts[0] == "channel" and len(parts) >= 2:
            return parts[1]
        return None
    except Exception:
        return None

def youtube_api_get(endpoint: str, params: dict, timeout: int = 20) -> dict:
    base = "https://www.googleapis.com/youtube/v3/"
    r = requests.get(base + endpoint, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def resolve_channel_id_from_url_or_text(text: str, api_key: str, timeout: int = 20) -> Optional[str]:
    text = text.strip()

    if is_youtube_url(text):
        cid = parse_youtube_channel_id(text)
        if cid:
            return cid

        u = urlparse(text)
        parts = [p for p in u.path.split("/") if p]
        token = None
        if parts:
            if parts[0].startswith("@"):
                token = parts[0][1:]
            elif parts[0] in ("c", "user") and len(parts) >= 2:
                token = parts[1]
            else:
                token = parts[0]

        if not token:
            return None

        try:
            data = youtube_api_get("channels", {"part": "id", "forHandle": token, "key": api_key}, timeout=timeout)
            items = data.get("items", [])
            if items:
                return items[0].get("id")
        except Exception:
            pass

        try:
            data = youtube_api_get("search", {"part": "snippet", "q": token, "type": "channel", "maxResults": 1, "key": api_key}, timeout=timeout)
            items = data.get("items", [])
            if items:
                return items[0]["id"].get("channelId")
        except Exception:
            return None

        return None

    try:
        data = youtube_api_get("search", {"part": "snippet", "q": text, "type": "channel", "maxResults": 1, "key": api_key}, timeout=timeout)
        items = data.get("items", [])
        if items:
            return items[0]["id"].get("channelId")
    except Exception:
        return None

    return None

def youtube_list_channel_video_ids(channel_id: str, api_key: str, max_items: int, timeout: int = 20) -> list[str]:
    ids: list[str] = []
    page_token = None

    while len(ids) < max_items:
        params = {"part": "id", "channelId": channel_id, "type": "video", "order": "date", "maxResults": 50, "key": api_key}
        if page_token:
            params["pageToken"] = page_token

        data = youtube_api_get("search", params, timeout=timeout)
        for item in data.get("items", []):
            vid = item.get("id", {}).get("videoId")
            if vid:
                ids.append(vid)
                if len(ids) >= max_items:
                    break

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    seen = set()
    out = []
    for v in ids:
        if v not in seen:
            out.append(v)
            seen.add(v)
    return out

def youtube_video_details(video_ids: list[str], api_key: str, timeout: int = 20) -> list[dict]:
    details: list[dict] = []
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i+50]
        data = youtube_api_get("videos", {"part": "snippet,contentDetails,statistics", "id": ",".join(chunk), "key": api_key}, timeout=timeout)
        for it in data.get("items", []):
            sn = it.get("snippet", {}) or {}
            stt = it.get("statistics", {}) or {}
            cd = it.get("contentDetails", {}) or {}
            vid = it.get("id", "")

            details.append({
                "Video Title": sn.get("title", "N/A"),
                "Upload Date": sn.get("publishedAt", "N/A"),
                "View Count": stt.get("viewCount", "N/A"),
                "Duration": _iso8601_duration_to_hms(cd.get("duration", "")),
                "Like Count": stt.get("likeCount", "N/A"),
                "Comment Count": stt.get("commentCount", "N/A"),
                "Description": sn.get("description", "N/A"),
                "Channel Name": sn.get("channelTitle", "N/A"),
                "Video URL": f"https://www.youtube.com/watch?v={vid}" if vid else "N/A",
            })
    return details

def youtube_extract(first_line: str, max_items: int, settings: Settings) -> List[Dict[str, Any]]:
    if not YOUTUBE_API_KEY:
        raise RuntimeError("YOUTUBE_API_KEY is missing. Add it to .streamlit/secrets.toml or environment variables.")

    first_line = first_line.strip()
    if is_youtube_url(first_line):
        vid = parse_youtube_video_id(first_line)
        if vid:
            return youtube_video_details([vid], YOUTUBE_API_KEY, timeout=settings.timeout_seconds)

        cid = resolve_channel_id_from_url_or_text(first_line, YOUTUBE_API_KEY, timeout=settings.timeout_seconds)
        if cid:
            vids = youtube_list_channel_video_ids(cid, YOUTUBE_API_KEY, max_items=max_items, timeout=settings.timeout_seconds)
            return youtube_video_details(vids, YOUTUBE_API_KEY, timeout=settings.timeout_seconds)

        q = first_line
    else:
        q = first_line

    ids = []
    page_token = None
    while len(ids) < max_items:
        params = {"part": "id", "q": q, "type": "video", "maxResults": 50, "key": YOUTUBE_API_KEY}
        if page_token:
            params["pageToken"] = page_token

        data = youtube_api_get("search", params, timeout=settings.timeout_seconds)

        for it in data.get("items", []):
            vid = it.get("id", {}).get("videoId")
            if vid:
                ids.append(vid)
                if len(ids) >= max_items:
                    break

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return youtube_video_details(ids[:max_items], YOUTUBE_API_KEY, timeout=settings.timeout_seconds)


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

def export_pdf_bytes(rows: List[Dict[str, Any]], columns: List[str], title: str) -> Optional[bytes]:
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib import colors
        from reportlab.lib.styles import getSampleStyleSheet
    except Exception:
        return None

    df = pd.DataFrame(rows)
    if df.empty:
        return None
    if columns:
        for c in columns:
            if c not in df.columns:
                df[c] = "N/A"
        df = df[columns]

    df = df.head(200)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter)
    styles = getSampleStyleSheet()
    elements = [Paragraph(title, styles["Title"]), Spacer(1, 12)]

    data = [list(df.columns)] + df.astype(str).values.tolist()
    table = Table(data, repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#0B3D91")),
        ("TEXTCOLOR", (0,0), (-1,0), colors.white),
        ("GRID", (0,0), (-1,-1), 0.25, colors.grey),
        ("FONTSIZE", (0,0), (-1,-1), 8),
        ("VALIGN", (0,0), (-1,-1), "TOP"),
    ]))
    elements.append(table)
    doc.build(elements)
    buf.seek(0)
    return buf.read()

def default_filename(prefix: str, ext: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}.{ext}"


# =========================
# Header
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

delay = st.sidebar.slider("Request delay (seconds)", 0.0, 5.0, 1.0, 0.25)
max_pages = st.sidebar.slider("Max pages per site (crawl deeper)", 1, 200, 30, 5)
timeout = st.sidebar.slider("Timeout (seconds)", 5, 60, 20, 1)
head_checks = st.sidebar.slider("Header checks per page (find hidden Excel/CSV)", 0, 200, 40, 10)

settings = Settings(
    delay_seconds=float(delay),
    timeout_seconds=int(timeout),
    max_pages_per_site=int(max_pages),
    max_head_checks_per_page=int(head_checks),
)

# ✅ ADDED: API key paste box (overrides env/secrets for this session)
st.sidebar.markdown("---")
with st.sidebar.expander("🔑 API Keys (Paste here)", expanded=False):
    st.write("Paste keys here if you don't want to use secrets.toml/env. (Session only)")
    serper_in = st.text_input(
        "SERPER_API_KEY",
        value=st.session_state.get("SERPER_API_KEY_UI", ""),
        type="password",
        placeholder="Paste SERPER API key"
    )
    youtube_in = st.text_input(
        "YOUTUBE_API_KEY",
        value=st.session_state.get("YOUTUBE_API_KEY_UI", ""),
        type="password",
        placeholder="Paste YouTube API key"
    )

    if st.button("Save Keys (this session)"):
        st.session_state["SERPER_API_KEY_UI"] = serper_in.strip()
        st.session_state["YOUTUBE_API_KEY_UI"] = youtube_in.strip()
        st.success("Saved for this session. Now run Search/Extract.")

# Override globals if user pasted keys
if st.session_state.get("SERPER_API_KEY_UI", "").strip():
    SERPER_API_KEY = st.session_state["SERPER_API_KEY_UI"].strip()
if st.session_state.get("YOUTUBE_API_KEY_UI", "").strip():
    YOUTUBE_API_KEY = st.session_state["YOUTUBE_API_KEY_UI"].strip()


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
col_export = st.sidebar.selectbox("Export format (data)", ["xlsx", "csv", "json", "sqlite", "pdf (if available)"], index=0)

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
# History expander
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
    st.markdown("### Step 1 — Search the web ")
    st.write("Enter a keyword → **Search Websites** → tick sites → **Crawl Selected Sites**.")

    query = st.text_input("Search query", value="", placeholder="Search")

    colA, colB = st.columns([1, 1])
    with colA:
        do_search = st.button("Search Websites", use_container_width=True)
    with colB:
        do_crawl = st.button("Crawl Selected Sites (Find Files)", use_container_width=True)

    if do_search:
        st.session_state.stop_flag = False
        if not query.strip():
            st.warning("Please enter a search query.")
        else:
            try:
                log("INFO", f"Searching web: {query} (target_results={search_k})")
                df = serper_search_paginated(query.strip(), total_results=int(search_k), timeout=settings.timeout_seconds, settings=settings)
                st.info(f"Search results returned: {len(df)}")
                st.session_state.search_df = df
                if df.empty:
                    log("WARN", "No search results returned.")
                    st.warning("No results found.")
                else:
                    log("OK", f"Search results: {len(df)}")
            except Exception as e:
                log("ERROR", str(e))
                st.error(str(e))

    if not st.session_state.search_df.empty:
        st.markdown("### Search results (tick Select to choose sites)")
        sdf = st.session_state.search_df.copy()

        edited = st.data_editor(
            sdf,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Select": st.column_config.CheckboxColumn("Select", help="Tick to select this website", default=False),
                "URL": st.column_config.LinkColumn("URL", display_text="Open link"),
            },
            disabled=["Title", "URL", "Snippet"]
        )
        st.session_state.search_df = edited

        selected_sites = edited.loc[edited["Select"] == True, "URL"].dropna().tolist()
        st.write(f"Selected websites: **{len(selected_sites)}**")

        if do_crawl:
            st.session_state.stop_flag = False
            if not selected_sites:
                st.warning("Select at least one website (tick Select).")
            else:
                prog = st.progress(0, text="Crawling selected sites...")
                try:
                    log("INFO", f"Crawling {len(selected_sites)} selected sites...")
                    df_files = discover_files_from_sites(selected_sites, exts, settings=settings)
                    st.session_state.files_df = df_files
                    prog.progress(100, text="Crawling completed.")
                    if df_files.empty:
                        log("WARN", "No files found.")
                        st.warning("No matching files found. Increase crawl depth or enable more file types.")
                    else:
                        log("OK", f"Files found: {len(df_files)}")
                        st.success(f"Found {len(df_files)} files.")
                except Exception as e:
                    log("ERROR", str(e))
                    st.error(str(e))
                finally:
                    prog.empty()

    if not st.session_state.files_df.empty:
        st.markdown("---")
        st.markdown("### Step 2 — Select files to download")
        fdf = st.session_state.files_df.copy()

        fedited = st.data_editor(
            fdf,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Select": st.column_config.CheckboxColumn("Select", help="Tick to include in download ZIP", default=False),
                "URL": st.column_config.LinkColumn("URL", display_text="Open file"),
            },
            disabled=["File", "Type", "URL", "Source"]
        )
        st.session_state.files_df = fedited

        selected_files = fedited.loc[fedited["Select"] == True, "URL"].dropna().tolist()
        st.write(f"Selected files: **{len(selected_files)}**")

        st.markdown("### Step 3 — Download")
        col1, col2 = st.columns([1, 1])

        with col1:
            if st.button("Prepare ZIP (Selected Files)", use_container_width=True):
                st.session_state.stop_flag = False
                if not selected_files:
                    st.warning("Select at least one file (tick Select).")
                else:
                    prog = st.progress(0, text="Preparing ZIP...")
                    log("INFO", f"Preparing ZIP for {len(selected_files)} files...")
                    zip_bytes = download_files_as_zip(selected_files, settings=settings)
                    st.session_state["zip_bytes"] = zip_bytes
                    prog.progress(100, text="ZIP ready.")
                    st.success("ZIP is ready. Use the download button.")
                    prog.empty()

        with col2:
            zip_bytes = st.session_state.get("zip_bytes", None)
            if zip_bytes:
                st.download_button(
                    "Download ZIP",
                    data=zip_bytes,
                    file_name=default_filename("ScrapBee_Files", "zip"),
                    mime="application/zip",
                    use_container_width=True
                )

    st.markdown("</div>", unsafe_allow_html=True)


# =========================
# TAB 2: Data Extractor
# =========================
with tab_extract:
    st.markdown('<div class="sb-panel">', unsafe_allow_html=True)
    st.markdown("### Step 1 — Choose platform and define columns")

    platform = st.selectbox("Platform", ["YouTube", "Generic Website (basic)"], index=0)
    st.session_state.extract_platform = platform

    templates = {
        "YouTube": ["Video Title", "Upload Date", "View Count", "Duration", "Like Count", "Comment Count", "Channel Name", "Video URL"],
        "Generic Website (basic)": ["Page Title", "H1", "Meta Description", "URL"]
    }
    default_cols = templates.get(platform, [])
    if not st.session_state.extract_columns:
        st.session_state.extract_columns = default_cols

    if st.button("Load Suggested Columns"):
        st.session_state.extract_columns = default_cols
        log("INFO", f"Loaded suggested columns for {platform}")

    cols_text = st.text_area(
        "Columns (one per line)",
        value="\n".join(st.session_state.extract_columns),
        height=160
    )
    columns = [c.strip() for c in cols_text.splitlines() if c.strip()]
    st.session_state.extract_columns = columns

    st.markdown("---")
    st.markdown("### Step 2 — Provide input and run")

    if platform == "YouTube":
        st.write("Input can be **keyword**, **channel URL**, or **video URL**.")
        placeholder = "Example:\nBhutanese Dreamer\nOR\nhttps://www.youtube.com/channel/UC...\nOR\nhttps://www.youtube.com/watch?v=..."
    else:
        st.write("Input: one URL per line (basic extraction).")
        placeholder = "Example:\nhttps://example.com/page1\nhttps://example.com/page2"

    raw_input = st.text_area("Input", height=130, placeholder=placeholder)
    max_items = st.slider("Max items to extract", 5, 1000, 25, 5)
    run = st.button("Run Extraction", use_container_width=True)

    if run:
        st.session_state.stop_flag = False
        st.session_state.extract_rows = []
        prog = st.progress(0, text="Starting extraction...")

        try:
            lines = [l.strip() for l in raw_input.splitlines() if l.strip()]
            if not lines:
                st.warning("Please provide input.")
            else:
                if platform == "YouTube":
                    first = lines[0]
                    log("INFO", "Running YouTube extraction...")
                    rows = youtube_extract(first, max_items=max_items, settings=settings)

                    for i, r in enumerate(rows, start=1):
                        if stop_requested():
                            log("WARN", "Stopped by user.")
                            break
                        st.session_state.extract_rows.append({c: r.get(c, "N/A") for c in columns})
                        prog.progress(int((i / max(1, len(rows))) * 100), text=f"Extracted {i}/{len(rows)}")

                else:
                    urls = [u for u in lines if is_valid_url(u)]
                    log("INFO", f"Running generic extraction on {len(urls)} URLs...")

                    for i, u in enumerate(urls, start=1):
                        if stop_requested():
                            log("WARN", "Stopped by user.")
                            break
                        html = fetch_html(u, settings)
                        row = {c: "N/A" for c in columns}
                        if "URL" in row:
                            row["URL"] = u

                        if html:
                            soup = BeautifulSoup(html, "html.parser")
                            title = (soup.title.text.strip() if soup.title and soup.title.text else "N/A")
                            h1 = (soup.find("h1").get_text(strip=True) if soup.find("h1") else "N/A")
                            meta = soup.find("meta", attrs={"name": "description"})
                            meta_desc = meta.get("content", "").strip() if meta else "N/A"

                            if "Page Title" in row:
                                row["Page Title"] = title
                            if "H1" in row:
                                row["H1"] = h1
                            if "Meta Description" in row:
                                row["Meta Description"] = meta_desc

                        st.session_state.extract_rows.append(row)
                        prog.progress(int((i / max(1, len(urls))) * 100), text=f"Extracted {i}/{len(urls)}")
                        time.sleep(settings.delay_seconds)

                prog.progress(100, text="Extraction finished.")
                log("OK", f"Extraction complete. Rows: {len(st.session_state.extract_rows)}")

        except Exception as e:
            log("ERROR", str(e))
            st.error(str(e))
        finally:
            prog.empty()

    st.markdown("---")
    st.markdown("### Step 3 — Preview and download")

    rows = st.session_state.extract_rows
    if rows:
        df_out = pd.DataFrame(rows)

        # Show nice link column if present
        if "Video URL" in df_out.columns:
            st.data_editor(
                df_out,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Video URL": st.column_config.LinkColumn("Video URL", display_text="Open")
                },
                disabled=list(df_out.columns)
            )
        else:
            st.dataframe(df_out, use_container_width=True)

        meta = {
            "Date": datetime.now().strftime("%Y%m%d_%H%M%S"),
            "Platform": platform,
            "Columns": ", ".join(columns),
            "Rows": len(rows)
        }

        fmt = col_export
        if fmt == "xlsx":
            data = export_xlsx_bytes(rows, columns, meta)
            st.download_button("Download XLSX", data=data, file_name=default_filename("ScrapBee_Data", "xlsx"),
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               use_container_width=True)

        elif fmt == "csv":
            data = export_csv_bytes(rows, columns)
            st.download_button("Download CSV", data=data, file_name=default_filename("ScrapBee_Data", "csv"),
                               mime="text/csv", use_container_width=True)

        elif fmt == "json":
            data = export_json_bytes(rows, columns)
            st.download_button("Download JSON", data=data, file_name=default_filename("ScrapBee_Data", "json"),
                               mime="application/json", use_container_width=True)

        elif fmt == "sqlite":
            data = export_sqlite_bytes(rows, columns)
            st.download_button("Download SQLite DB", data=data, file_name=default_filename("ScrapBee_Data", "db"),
                               mime="application/octet-stream", use_container_width=True)

        else:
            pdf = export_pdf_bytes(rows, columns, title=f"{APP_NAME} Export")
            if pdf is None:
                st.warning("PDF export needs `reportlab`. Install it with: pip install reportlab")
            else:
                st.download_button("Download PDF", data=pdf, file_name=default_filename("ScrapBee_Data", "pdf"),
                                   mime="application/pdf", use_container_width=True)

    else:
        st.info("No extracted data yet. Run an extraction to enable downloads.")

    st.markdown("</div>", unsafe_allow_html=True)


# =========================
# Footer warnings (keys)
# =========================
if not SERPER_API_KEY:
    st.warning("SERPER_API_KEY is missing. Web search will not work until you add it to secrets.toml or environment variables.")
if not YOUTUBE_API_KEY:
    st.warning("YOUTUBE_API_KEY is missing. YouTube extraction will not work until you add it to secrets.toml or environment variables.")
