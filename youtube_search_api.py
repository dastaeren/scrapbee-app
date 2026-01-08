from typing import List, Dict, Any
import requests

from core.models import Settings


def youtube_search_videos(query: str, api_key: str, settings: Settings, max_results: int = 15) -> List[str]:
    if not api_key:
        raise ValueError("Missing YOUTUBE_API_KEY")

    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "part": "snippet",
        "q": query,
        "type": "video",
        "maxResults": int(max_results),
        "key": api_key,
    }
    r = requests.get(url, params=params, timeout=settings.timeout_seconds)
    r.raise_for_status()
    data = r.json()

    ids = []
    for item in data.get("items", []) or []:
        vid = (item.get("id") or {}).get("videoId")
        if vid:
            ids.append(vid)
    return ids


def youtube_videos_details(video_ids: List[str], api_key: str, settings: Settings) -> List[Dict[str, Any]]:
    if not api_key:
        raise ValueError("Missing YOUTUBE_API_KEY")
    if not video_ids:
        return []

    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "part": "snippet,contentDetails,statistics",
        "id": ",".join(video_ids),
        "key": api_key,
    }
    r = requests.get(url, params=params, timeout=settings.timeout_seconds)
    r.raise_for_status()
    data = r.json()

    out = []
    for it in data.get("items", []) or []:
        sn = it.get("snippet", {}) or {}
        st = it.get("statistics", {}) or {}
        cd = it.get("contentDetails", {}) or {}

        out.append({
            "Video Title": sn.get("title", "N/A"),
            "Upload Date": sn.get("publishedAt", "N/A"),
            "View Count": st.get("viewCount", "N/A"),
            "Like Count": st.get("likeCount", "N/A"),
            "Comment Count": st.get("commentCount", "N/A"),
            "Duration": cd.get("duration", "N/A"),
            "Channel Name": sn.get("channelTitle", "N/A"),
            "Description": sn.get("description", "N/A"),
            "Video URL": f"https://www.youtube.com/watch?v={it.get('id','')}",
        })
    return out
