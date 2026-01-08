from __future__ import annotations

from dataclasses import asdict
from io import BytesIO
import json
import sqlite3
import tempfile
from typing import List, Dict, Any, Optional
from datetime import datetime

import pandas as pd


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def default_export_filename(platform: str, ext: str) -> str:
    safe = "".join(ch for ch in platform if ch.isalnum() or ch in (" ", "_", "-")).strip().replace(" ", "_")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"ScrapBee_{safe}_{ts}.{ext}"


def export_xlsx_bytes(
    rows: List[Dict[str, Any]],
    columns: List[str],
    platform: str,
    processed_urls: Optional[List[str]] = None
) -> bytes:
    df = pd.DataFrame(rows or [])
    for c in columns:
        if c not in df.columns:
            df[c] = "N/A"
    df = df[columns] if columns else df

    meta = {
        "generated_at": _now_str(),
        "platform": platform,
        "row_count": int(len(df)),
        "columns": ", ".join(columns),
        "processed_count": len(processed_urls or []),
    }

    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Data")

        meta_df = pd.DataFrame([meta])
        meta_df.to_excel(writer, index=False, sheet_name="Metadata")

        if processed_urls:
            pd.DataFrame({"Processed_URLs": processed_urls}).to_excel(writer, index=False, sheet_name="URLs")

        # Basic formatting
        ws = writer.sheets["Data"]
        for col_idx, col_name in enumerate(df.columns, start=1):
            max_len = max([len(str(col_name))] + [len(str(x)) for x in df[col_name].head(200).tolist()])
            ws.column_dimensions[chr(64 + col_idx)].width = min(55, max(12, max_len + 2))

    out.seek(0)
    return out.getvalue()


def export_csv_bytes(rows: List[Dict[str, Any]], columns: List[str]) -> bytes:
    df = pd.DataFrame(rows or [])
    for c in columns:
        if c not in df.columns:
            df[c] = "N/A"
    df = df[columns] if columns else df
    return df.to_csv(index=False).encode("utf-8")


def export_json_bytes(rows: List[Dict[str, Any]], platform: str, columns: List[str]) -> bytes:
    payload = {
        "generated_at": _now_str(),
        "platform": platform,
        "columns": columns,
        "rows": rows or [],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


def export_sqlite_bytes(rows: List[Dict[str, Any]], columns: List[str], platform: str) -> bytes:
    df = pd.DataFrame(rows or [])
    for c in columns:
        if c not in df.columns:
            df[c] = "N/A"
    df = df[columns] if columns else df

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        path = tmp.name

    conn = sqlite3.connect(path)
    try:
        df.to_sql("data", conn, if_exists="replace", index=False)

        meta = pd.DataFrame([{
            "generated_at": _now_str(),
            "platform": platform,
            "row_count": int(len(df)),
            "columns": ", ".join(columns),
        }])
        meta.to_sql("metadata", conn, if_exists="replace", index=False)

        conn.commit()
    finally:
        conn.close()

    with open(path, "rb") as f:
        data = f.read()
    return data


def export_pdf_bytes(rows: List[Dict[str, Any]], columns: List[str], platform: str) -> bytes:
    """
    Simple PDF export (first ~200 rows) using ReportLab.
    """
    from reportlab.lib.pagesizes import letter
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet

    df = pd.DataFrame(rows or [])
    for c in columns:
        if c not in df.columns:
            df[c] = "N/A"
    df = df[columns] if columns else df

    df_view = df.head(200)

    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter, leftMargin=24, rightMargin=24, topMargin=24, bottomMargin=24)

    styles = getSampleStyleSheet()
    story = []
    story.append(Paragraph(f"ScrapBee Pro Export — {platform}", styles["Title"]))
    story.append(Paragraph(f"Generated at: {_now_str()} | Rows: {len(df)} (showing first {len(df_view)})", styles["Normal"]))
    story.append(Spacer(1, 12))

    data = [list(df_view.columns)] + df_view.astype(str).values.tolist()
    tbl = Table(data, repeatRows=1)

    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0B3D91")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 9),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#334155")),
        ("FONTSIZE", (0, 1), (-1, -1), 8),
        ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#111827")),
        ("TEXTCOLOR", (0, 1), (-1, -1), colors.HexColor("#E5E7EB")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#0F172A"), colors.HexColor("#111827")]),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))

    story.append(tbl)
    doc.build(story)

    buf.seek(0)
    return buf.getvalue()
