import re
import time
import urllib.parse
import urllib.robotparser
from typing import Dict, Optional, Tuple

import requests


PLATFORMS = ["YouTube", "Google Search", "Generic Website", "Facebook", "Instagram", "LinkedIn"]

TEMPLATES = {
    "YouTube": ["Title", "Upload Date", "Views", "Duration", "Likes", "Description", "Channel Name", "URL"],
    "Google Search": ["Title", "URL", "Description", "Position", "Date"],
    "Generic Website": ["Title", "H1", "H2", "URL"],
    "Facebook": ["Post Text", "Reaction Count", "Share Count", "Timestamp", "Comments", "URL"],
    "Instagram": ["Post Text", "Likes", "Comments", "Timestamp", "URL"],
    "LinkedIn": ["Profile Name", "Headline", "Company", "Location", "Connections", "URL"],
}


def is_valid_url(url: str) -> bool:
    try:
        p = urllib.parse.urlparse(url.strip())
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False


def detect_platform(value: str) -> str:
    v = value.lower().strip()
    if not is_valid_url(value):
        return "Google Search"
    if "youtube.com" in v or "youtu.be" in v:
        return "YouTube"
    if "facebook.com" in v:
        return "Facebook"
    if "instagram.com" in v:
        return "Instagram"
    if "linkedin.com" in v:
        return "LinkedIn"
    return "Generic Website"


def normalize_url(base: str, link: str) -> Optional[str]:
    if not link:
        return None
    link = link.strip()
    if link.startswith(("mailto:", "javascript:", "#")):
        return None
    try:
        return urllib.parse.urljoin(base, link)
    except Exception:
        return None


def robots_allowed(url: str, user_agent: str = "*") -> Tuple[bool, str]:
    try:
        parsed = urllib.parse.urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(robots_url)
        rp.read()
        allowed = rp.can_fetch(user_agent or "*", url)
        return allowed, ("Allowed by robots.txt" if allowed else "Blocked by robots.txt")
    except Exception as e:
        return True, f"robots.txt check failed ({e}); proceeding cautiously."


def polite_sleep(seconds: float, stop_flag=None):
    step = 0.1
    remaining = max(0.0, seconds)
    while remaining > 0:
        if stop_flag and stop_flag():
            return
        time.sleep(min(step, remaining))
        remaining -= step


def build_session(user_agent: str, proxies: Dict[str, str]) -> requests.Session:
    s = requests.Session()
    if user_agent:
        s.headers.update({"User-Agent": user_agent})
    p = {k: v for k, v in (proxies or {}).items() if v}
    if p:
        s.proxies.update(p)
    return s


def now_timestamp() -> str:
    import datetime as dt
    return dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def safe_filename(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", s).strip("_")
