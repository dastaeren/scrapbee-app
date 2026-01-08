from typing import List, Dict, Any
from core.utils import build_session
from core.models import Settings


def serper_search(query: str, api_key: str, settings: Settings, num: int = 10) -> List[Dict[str, Any]]:
    if not api_key:
        raise ValueError("Missing SERPER_API_KEY")

    session = build_session(settings.user_agent, {"http": settings.proxy_http, "https": settings.proxy_https})

    url = "https://google.serper.dev/search"
    headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}
    payload = {"q": query, "num": int(num)}

    r = session.post(url, headers=headers, json=payload, timeout=settings.timeout_seconds)
    r.raise_for_status()
    data = r.json()

    organic = data.get("organic", []) or []
    out = []
    for i, item in enumerate(organic, start=1):
        out.append({
            "Title": item.get("title", "N/A"),
            "URL": item.get("link", "N/A"),
            "Description": item.get("snippet", "N/A"),
            "Position": i,
            "Date": item.get("date", "N/A"),
        })
    return out
