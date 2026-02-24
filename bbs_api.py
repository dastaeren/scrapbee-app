# bbs_api.py
import time
import sqlite3
import html
from typing import List, Dict, Any, Optional

import requests
from bs4 import BeautifulSoup

BBS_API_BASE = "https://www.bbs.bt/wp-json/wp/v2"


def wp_text(s: str) -> str:
    """Convert WP rendered HTML into plain text."""
    if not s:
        return ""
    s = html.unescape(s)
    return BeautifulSoup(s, "html.parser").get_text(" ", strip=True)


def init_bbs_db(db_path: str):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS bbs_posts (
        id INTEGER PRIMARY KEY,
        date TEXT,
        modified TEXT,
        link TEXT,
        title TEXT,
        excerpt TEXT,
        content TEXT
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bbs_date ON bbs_posts(date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bbs_title ON bbs_posts(title)")
    con.commit()
    return con


def upsert_bbs_post(con: sqlite3.Connection, p: Dict[str, Any]):
    post_id = int(p.get("id"))
    title = wp_text((p.get("title") or {}).get("rendered", ""))
    excerpt = wp_text((p.get("excerpt") or {}).get("rendered", ""))
    content = wp_text((p.get("content") or {}).get("rendered", ""))

    con.execute("""
    INSERT INTO bbs_posts (id, date, modified, link, title, excerpt, content)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
        date=excluded.date,
        modified=excluded.modified,
        link=excluded.link,
        title=excluded.title,
        excerpt=excluded.excerpt,
        content=excluded.content
    """, (
        post_id,
        p.get("date", ""),
        p.get("modified", ""),
        p.get("link", ""),
        title,
        excerpt,
        content
    ))


def bbs_iter_all_posts(
    user_agent: str,
    timeout_seconds: int,
    delay_seconds: float,
    stop_fn=None,
    per_page: int = 100
):
    """
    Iterate through ALL posts using WP pagination.
    WP returns HTTP 400 when page exceeds max -> stop.
    """
    headers = {"User-Agent": user_agent}
    page = 1

    while True:
        if stop_fn and stop_fn():
            break

        params = {"per_page": min(max(per_page, 1), 100), "page": page}
        url = f"{BBS_API_BASE}/posts"

        r = requests.get(url, headers=headers, params=params, timeout=timeout_seconds)

        if r.status_code == 400:   # out of range page
            break

        r.raise_for_status()
        data = r.json()
        if not data:
            break

        for p in data:
            yield p

        page += 1
        time.sleep(delay_seconds)


def bbs_backfill_all_years(
    db_path: str,
    user_agent: str,
    timeout_seconds: int,
    delay_seconds: float,
    stop_fn=None,
    per_page: int = 100
) -> int:
    """
    One-time: save ALL posts (all years) into SQLite.
    Returns total rows in DB after backfill.
    """
    con = init_bbs_db(db_path)
    count = 0

    for p in bbs_iter_all_posts(
        user_agent=user_agent,
        timeout_seconds=timeout_seconds,
        delay_seconds=delay_seconds,
        stop_fn=stop_fn,
        per_page=per_page
    ):
        if stop_fn and stop_fn():
            break
        upsert_bbs_post(con, p)
        count += 1
        if count % 100 == 0:
            con.commit()

    con.commit()

    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM bbs_posts")
    total = int(cur.fetchone()[0])
    con.close()
    return total


def bbs_search_local(
    db_path: str,
    keywords: List[str],
    limit: int = 200
) -> List[Dict[str, Any]]:
    kws = [k.strip() for k in keywords if k.strip()]
    if not kws:
        return []

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    clauses = []
    params = []
    for k in kws:
        like = f"%{k.lower()}%"
        clauses.append("(LOWER(title) LIKE ? OR LOWER(excerpt) LIKE ? OR LOWER(content) LIKE ?)")
        params.extend([like, like, like])

    sql = f"""
    SELECT id, date, link, title, excerpt
    FROM bbs_posts
    WHERE {" OR ".join(clauses)}
    ORDER BY date DESC
    LIMIT ?
    """
    params.append(int(limit))

    rows = con.execute(sql, params).fetchall()
    con.close()

    out = []
    for r in rows:
        out.append({
            "Title": r["title"],
            "Date": r["date"],
            "URL": r["link"],
            "Excerpt": r["excerpt"],
        })
    return out
