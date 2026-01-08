import requests
from typing import Dict, Any, Optional


class ApifyRunner:
    """
    Generic Apify actor runner hook.
    You must supply an actor ID and input schema that matches the actor you choose.
    """
    def __init__(self, token: str):
        self.token = token

    def run_actor(self, actor_id: str, actor_input: Dict[str, Any]) -> Dict[str, Any]:
        url = f"https://api.apify.com/v2/acts/{actor_id}/runs?token={self.token}"
        r = requests.post(url, json=actor_input, timeout=60)
        r.raise_for_status()
        run = r.json().get("data", {})
        return run
