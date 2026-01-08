from dataclasses import dataclass, field
from typing import Dict, List, Any


@dataclass
class Settings:
    delay_seconds: float = 2.5
    timeout_seconds: int = 20
    max_pages: int = 5
    output_folder: str = "."
    save_format: str = "xlsx"   # "xlsx" or "csv"
    use_selenium: bool = False
    proxy_http: str = ""
    proxy_https: str = ""
    user_agent: str = ""


@dataclass
class ColumnSchema:
    platform: str
    columns: List[str] = field(default_factory=list)
    selectors: Dict[str, str] = field(default_factory=dict)  # for Generic Website


@dataclass
class ScrapeResult:
    url: str
    platform: str
    data: Dict[str, Any]
    status: str   # "success" | "warning" | "error"
    message: str = ""
