import io
import os
import time
import zipfile
import requests
import re
from typing import List, Optional, Callable
from urllib.parse import urlparse, unquote


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


def download_files_as_zip(
    urls: List[str],
    timeout: int,
    delay: float,
    user_agent: str,
    stop_cb: Callable[[], bool],
    progress_cb: Optional[Callable[[int, str], None]] = None,
) -> bytes:
    sess = requests.Session()
    sess.headers.update({"User-Agent": user_agent})

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        total = len(urls)
        for i, u in enumerate(urls, start=1):
            if stop_cb():
                break

            if progress_cb:
                pct = int((i / max(1, total)) * 100)
                progress_cb(pct, f"Downloading {i}/{total}")

            try:
                r = sess.get(u, timeout=timeout)
                r.raise_for_status()

                cd = r.headers.get("content-disposition") or r.headers.get("Content-Disposition") or ""
                fname = parse_content_disposition_filename(cd)
                name = fname or os.path.basename(urlparse(u).path) or f"file_{int(time.time())}"

                if name in zf.namelist():
                    base, ext = os.path.splitext(name)
                    name = f"{base}_{int(time.time())}{ext}"

                zf.writestr(name, r.content)

                if delay:
                    time.sleep(delay)
            except Exception:
                continue

    buf.seek(0)
    return buf.read()
