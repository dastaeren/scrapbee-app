from abc import ABC, abstractmethod
from core.models import ColumnSchema, Settings, ScrapeResult


class BaseScraper(ABC):
    @abstractmethod
    def scrape_one(self, url_or_query: str, schema: ColumnSchema, settings: Settings) -> ScrapeResult:
        raise NotImplementedError
