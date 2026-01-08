from io import BytesIO
import zipfile
from typing import List, Dict

from core.models import Settings


def download_files_as_zip(file_rows: List[Dict[str, str]], session, settings: Settings, stop_flag, log_fn, progress_fn=None) -> bytes:
    bio = BytesIO()
    with zipfile.ZipFile(bio, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        total = max(1, len(file_rows))

        for i, row in enumerate(file_rows, start=1):
            if stop_flag():
                log_fn("warning", "Stop requested. Download halted.")
                break

            url = row.get("File URL", "")
            name = row.get("Filename", f"file_{i}")

            try:
                r = session.get(url, stream=True, timeout=settings.timeout_seconds)
                r.raise_for_status()

                file_buf = BytesIO()
                for chunk in r.iter_content(chunk_size=1024 * 128):
                    if stop_flag():
                        break
                    if chunk:
                        file_buf.write(chunk)

                zf.writestr(name, file_buf.getvalue())
                log_fn("success", f"Downloaded: {name}")

            except Exception as e:
                log_fn("error", f"Download failed: {url} — {e}")

            if progress_fn:
                progress_fn(int((i / total) * 100))

    bio.seek(0)
    return bio.getvalue()
