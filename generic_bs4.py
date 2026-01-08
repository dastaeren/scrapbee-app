from typing import Dict, Any
from bs4 import BeautifulSoup

from core.models import ColumnSchema, Settings, ScrapeResult
from core.utils import build_session, robots_allowed


class GenericWebsiteScraper:
    def scrape_one(self, url: str, schema: ColumnSchema, settings: Settings) -> ScrapeResult:
        allowed, reason = robots_allowed(url, user_agent=settings.user_agent or "*")
        if not allowed:
            return ScrapeResult(url, "Generic Website", {c: "N/A" for c in schema.columns}, "warning",
                                f"Blocked by robots.txt: {reason}")

        proxies = {"http": settings.proxy_http, "https": settings.proxy_https}
        session = build_session(settings.user_agent, proxies)

        try:
            r = session.get(url, timeout=settings.timeout_seconds)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")

            # If CSS selectors provided
            if schema.selectors:
                extracted: Dict[str, Any] = {}
                for c in schema.columns:
                    sel = (schema.selectors.get(c) or "").strip()
                    if not sel:
                        extracted[c] = "N/A"
                        continue
                    el = soup.select_one(sel)
                    extracted[c] = el.get_text(" ", strip=True) if el else "N/A"
                return ScrapeResult(url, "Generic Website", extracted, "success", "Extracted using CSS selectors.")

            # Fallback extraction
            title = soup.title.get_text(strip=True) if soup.title else "N/A"
            h1 = soup.select_one("h1")
            h2 = soup.select_one("h2")

            base = {
                "Title": title,
                "H1": h1.get_text(" ", strip=True) if h1 else "N/A",
                "H2": h2.get_text(" ", strip=True) if h2 else "N/A",
                "URL": url,
            }
            row = {c: base.get(c, "N/A") for c in schema.columns}
            return ScrapeResult(url, "Generic Website", row, "success", "Extracted from HTML (fallback).")

        except Exception as e:
            return ScrapeResult(url, "Generic Website", {c: "N/A" for c in schema.columns}, "error",
                                f"Generic scrape error: {e}")
