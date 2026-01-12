import os
from dataclasses import dataclass
from datetime import datetime
from typing import List

import pandas as pd
import streamlit as st

from crawler_requests import (
    serper_search_paginated,
    crawl_requests_concurrent,
    DEFAULT_FILE_EXTS,
)
from crawler_playwright import crawl_playwright
from download_utils import download_files_as_zip

APP_NAME = "ScrapBee"
TAGLINE = "Universal Web File Finder (Fast + JS Mode + Sitemap + Concurrency)"

st.set_page_config(page_title=APP_NAME, layout="wide")

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
    input[type="checkbox"]{ accent-color: var(--sb-accent) !important; }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


def get_secret(name: str, default: str = "") -> str:
    try:
        v = st.secrets.get(name, None)
        if v:
            return str(v)
    except Exception:
        pass
    return os.getenv(name, default)


SERPER_API_KEY = get_secret("SERPER_API_KEY", "")


def init_state():
    st.session_state.setdefault("stop_flag", False)
    st.session_state.setdefault("history", [])
    st.session_state.setdefault("search_df", pd.DataFrame())
    st.session_state.setdefault("files_df", pd.DataFrame())
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
class CrawlSettings:
    delay_seconds: float = 0.0
    timeout_seconds: int = 20
    max_depth: int = 2
    max_pages: int = 120
    max_files: int = 800
    workers: int = 12

    same_domain_only: bool = True
    deep_detect_downloads: bool = True

    use_sitemaps: bool = True
    max_sitemap_urls: int = 2000

    user_agent: str = "ScrapBee/3.0"


st.markdown(
    f"""
    <div class="sb-header">
        <div class="sb-title">{APP_NAME}</div>
        <div class="sb-tagline">{TAGLINE}</div>
    </div>
    """,
    unsafe_allow_html=True
)

# Sidebar
st.sidebar.markdown("## Crawl Controls")

crawl_mode = st.sidebar.selectbox(
    "Crawl mode",
    ["Fast (requests + concurrency)", "JS Render (Playwright)"],
    index=0
)

timeout = st.sidebar.slider("Timeout (seconds)", 5, 60, 20, 1)
max_depth = st.sidebar.slider("Max depth", 0, 8, 2, 1)
max_pages = st.sidebar.slider("Max pages", 1, 1000, 120, 20)
max_files = st.sidebar.slider("Max files", 10, 20000, 800, 100)

workers = st.sidebar.slider("Workers (speed)", 2, 40, 12, 1)

same_domain_only = st.sidebar.checkbox("Stay in same domain", value=True)
deep_detect = st.sidebar.checkbox("Deep detect download endpoints", value=True)

use_sitemaps = st.sidebar.checkbox("Use sitemap discovery", value=True)
max_sitemap_urls = st.sidebar.slider("Max sitemap URLs", 100, 20000, 2000, 100)

st.sidebar.markdown("---")
st.sidebar.markdown("## File types")
exts = st.sidebar.multiselect(
    "Find these extensions",
    DEFAULT_FILE_EXTS,
    default=[".pdf", ".xlsx", ".xls", ".csv", ".docx", ".pptx", ".zip"]
)

st.sidebar.markdown("---")
if st.sidebar.button("Stop"):
    st.session_state.stop_flag = True
    log("WARN", "Stop requested.")

if st.sidebar.button("Reset"):
    st.session_state.stop_flag = False
    st.session_state.search_df = pd.DataFrame()
    st.session_state.files_df = pd.DataFrame()
    st.session_state.zip_bytes = None
    st.session_state.history = []
    log("INFO", "Reset done.")


settings = CrawlSettings(
    timeout_seconds=int(timeout),
    max_depth=int(max_depth),
    max_pages=int(max_pages),
    max_files=int(max_files),
    workers=int(workers),
    same_domain_only=bool(same_domain_only),
    deep_detect_downloads=bool(deep_detect),
    use_sitemaps=bool(use_sitemaps),
    max_sitemap_urls=int(max_sitemap_urls),
)

with st.expander("History / Logs", expanded=False):
    if st.session_state.history:
        st.dataframe(pd.DataFrame(st.session_state.history), use_container_width=True, hide_index=True)
    else:
        st.info("No logs yet.")

tab_search, tab_crawl = st.tabs(["Search (Serper)", "Crawl + Download"])


# =========================
# Search (Serper)
# =========================
with tab_search:
    st.markdown('<div class="sb-panel">', unsafe_allow_html=True)
    st.markdown("### Web search (returns >10 results)")

    if not SERPER_API_KEY:
        st.warning("SERPER_API_KEY missing. Add it to .streamlit/secrets.toml (local) or env var on server.")

    query = st.text_input("Search query", placeholder="Example: NSB Bhutan excel download", value="")
    k = st.slider("Results to fetch", 10, 300, 30, 10)

    if st.button("Run Search", use_container_width=True, disabled=not bool(SERPER_API_KEY)):
        st.session_state.stop_flag = False
        try:
            log("INFO", f"Serper search: {query} (k={k})")
            df = serper_search_paginated(query=query, total_results=int(k), timeout=settings.timeout_seconds, api_key=SERPER_API_KEY)
            st.session_state.search_df = df
            st.success(f"Returned {len(df)} results.")
        except Exception as e:
            log("ERROR", str(e))
            st.error(str(e))

    if not st.session_state.search_df.empty:
        st.markdown("### Search results")
        sdf = st.session_state.search_df.copy()
        edited = st.data_editor(
            sdf,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Select": st.column_config.CheckboxColumn("Select", default=False),
                "URL": st.column_config.LinkColumn("URL", display_text="Open"),
            },
            disabled=["Title", "URL", "Snippet"]
        )
        st.session_state.search_df = edited
        st.info("Now go to **Crawl + Download** tab to crawl selected sites/pages.")
    st.markdown("</div>", unsafe_allow_html=True)


# =========================
# Crawl + Download
# =========================
with tab_crawl:
    st.markdown('<div class="sb-panel">', unsafe_allow_html=True)
    st.markdown("### Step 1 — Provide URLs (paste pages or sites)")

    manual_urls = st.text_area(
        "URLs (one per line)",
        placeholder="Example:\nhttps://www.nsb.gov.bt/?dlm_download_category=ialc-excel\nhttps://example.com/resources",
        height=110
    )

    selected_from_search: List[str] = []
    if not st.session_state.search_df.empty:
        selected_from_search = st.session_state.search_df.loc[
            st.session_state.search_df["Select"] == True, "URL"
        ].dropna().tolist()

    manual_list = [u.strip() for u in manual_urls.splitlines() if u.strip()]
    combined = list(dict.fromkeys(selected_from_search + manual_list))

    st.write(f"Selected from search: **{len(selected_from_search)}**")
    st.write(f"Manual URLs: **{len(manual_list)}**")
    st.write(f"Total to crawl: **{len(combined)}**")

    colA, colB = st.columns([1, 1])
    with colA:
        run_crawl = st.button("Run Crawl", use_container_width=True)
    with colB:
        clear_files = st.button("Clear Results", use_container_width=True)

    if clear_files:
        st.session_state.files_df = pd.DataFrame()
        st.session_state.zip_bytes = None
        log("INFO", "Cleared crawl results.")

    if run_crawl:
        st.session_state.stop_flag = False
        if not combined:
            st.warning("Add at least one URL.")
        else:
            prog = st.progress(0, text="Crawling...")
            try:
                log("INFO", f"Crawl mode: {crawl_mode} | urls={len(combined)} | depth={settings.max_depth} | pages={settings.max_pages} | workers={settings.workers}")
                if crawl_mode == "Fast (requests + concurrency)":
                    files = crawl_requests_concurrent(
                        start_urls=combined,
                        allowed_exts=exts,
                        settings=settings,
                        stop_cb=stop_requested,
                        progress_cb=lambda p, t: prog.progress(p, text=t),
                    )
                else:
                    files = crawl_playwright(
                        start_urls=combined,
                        allowed_exts=exts,
                        settings=settings,
                        stop_cb=stop_requested,
                        progress_cb=lambda p, t: prog.progress(p, text=t),
                    )

                df_files = pd.DataFrame(files)
                st.session_state.files_df = df_files

                if df_files.empty:
                    st.warning("No files found. Try higher depth/pages OR switch to Playwright for JS sites.")
                else:
                    st.success(f"Found {len(df_files)} files.")
                log("OK", f"Found files: {len(df_files)}")
            except Exception as e:
                log("ERROR", str(e))
                st.error(str(e))
            finally:
                prog.empty()

    if not st.session_state.files_df.empty:
        st.markdown("---")
        st.markdown("### Step 2 — Select files to download")

        fdf = st.session_state.files_df.copy()
        if "Select" not in fdf.columns:
            fdf.insert(0, "Select", False)

        fedited = st.data_editor(
            fdf,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Select": st.column_config.CheckboxColumn("Select", default=False),
                "URL": st.column_config.LinkColumn("URL", display_text="Open"),
            },
            disabled=[c for c in fdf.columns if c != "Select"]
        )
        st.session_state.files_df = fedited

        selected = fedited.loc[fedited["Select"] == True, "URL"].dropna().tolist()
        st.write(f"Selected files: **{len(selected)}**")

        st.markdown("### Step 3 — Download ZIP")
        col1, col2 = st.columns([1, 1])

        with col1:
            if st.button("Prepare ZIP", use_container_width=True):
                if not selected:
                    st.warning("Select at least one file.")
                else:
                    prog = st.progress(0, text="Downloading into ZIP...")
                    try:
                        st.session_state.stop_flag = False
                        zip_bytes = download_files_as_zip(
                            urls=selected,
                            timeout=settings.timeout_seconds,
                            delay=settings.delay_seconds,
                            user_agent=settings.user_agent,
                            stop_cb=stop_requested,
                            progress_cb=lambda p, t: prog.progress(p, text=t),
                        )
                        st.session_state.zip_bytes = zip_bytes
                        st.success("ZIP ready.")
                        log("OK", f"ZIP prepared for {len(selected)} files.")
                    except Exception as e:
                        log("ERROR", str(e))
                        st.error(str(e))
                    finally:
                        prog.empty()

        with col2:
            if st.session_state.zip_bytes:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                st.download_button(
                    "Download ZIP",
                    data=st.session_state.zip_bytes,
                    file_name=f"ScrapBee_Files_{ts}.zip",
                    mime="application/zip",
                    use_container_width=True
                )

    st.markdown("</div>", unsafe_allow_html=True)
