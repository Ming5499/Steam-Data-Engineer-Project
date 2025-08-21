import random
import time
import requests

class SteamReviewAPIClient:
    def __init__(self, proxies=None, max_retries=5, backoff_factor=1.5, timeout=10):
        self.proxies = proxies or []
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.timeout = timeout
        self.proxy_index = 0 

    def _get_proxy(self):
        if not self.proxies:
            return None
        proxy = self.proxies[self.proxy_index % len(self.proxies)]
        self.proxy_index += 1
        return {"http": proxy, "https": proxy}

    def get_reviews(self, appid, cursor="*"):
        url = f"https://store.steampowered.com/appreviews/{appid}"
        params = {
            "json": 1,
            "filter": "recent",
            "language": "all",
            "day_range": 9223372036854775807,
            "review_type": "all",
            "purchase_type": "all",
            "cursor": cursor,
        }

        for attempt in range(1, self.max_retries + 1):
            proxy = self._get_proxy()
            try:
                resp = requests.get(
                    url, params=params, proxies=proxy, timeout=self.timeout
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("success") == 1:
                        reviews = data.get("reviews", [])
                        print(f"App {appid}: collected {len(reviews)} reviews.")
                        return {"reviews": reviews, "cursor": data.get("cursor", cursor)}
                    else:
                        print(f"⚠️ App {appid}: success={data.get('success')}, no reviews")
                        return None
                else:
                    print(f"❌ App {appid}: HTTP {resp.status_code}")
            except requests.exceptions.Timeout:
                wait = self.backoff_factor * attempt
                print(f"⏱️ Timeout for app {appid} -> backoff {wait:.1f}s")
                time.sleep(wait)
            except requests.exceptions.RequestException as e:
                wait = self.backoff_factor * attempt
                print(f"❌ Request exception for app {appid}: {e} -> backoff {wait:.1f}s")
                time.sleep(wait)

        print(f"App {appid}: failed after {self.max_retries} retries")
        return None
