from typing import Dict, Any, List

from core.models import ColumnSchema, Settings, ScrapeResult
from core.utils import build_session


class SerperGoogleScraper:
    def __init__(self, serper_api_key: str):
        self.api_key = serper_api_key

    def search(self, query: str, settings: Settings, num: int = 10) -> List[Dict[str, Any]]:
        proxies = {"http": settings.proxy_http, "https": settings.proxy_https}
        sess = build_session(settings.timeout_seconds, settings.user_agent, proxies)

        url = "https://google.serper.dev/search"
        headers = {"X-API-KEY": self.api_key, "Content-Type": "application/json"}
        payload = {"q": query, "num": num}

        r = sess.post(url, headers=headers, json=payload, timeout=settings.timeout_seconds)
        r.raise_for_status()
        data = r.json()
        organic = data.get("organic", []) or []
        results = []
        for idx, item in enumerate(organic, start=1):
            results.append({
                "Title": item.get("title", "N/A"),
                "URL": item.get("link", "N/A"),
                "Description": item.get("snippet", "N/A"),
                "Position": idx,
                "Date": item.get("date", "N/A"),
            })
        return results

    def scrape_one(self, url: str, schema: ColumnSchema, settings: Settings) -> ScrapeResult:
        # For Google Search platform, "url" is actually a query string in our app.
        try:
            results = self.search(url, settings, num=10)
            # For compatibility, we return the first result for scrape_one
            if not results:
                return ScrapeResult(url, "Google Search", {}, "warning", "No search results.")
            row = results[0]
            data = {c: row.get(c, "N/A") for c in schema.columns}
            return ScrapeResult(url, "Google Search", data, "success", "Fetched via Serper API (first result).")
        except Exception as e:
            return ScrapeResult(url, "Google Search", {}, "error", f"Serper API error: {e}")
