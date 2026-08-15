from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass, field

import requests

logger = logging.getLogger("etl.extract")

DEFAULT_TIMEOUT = 10
MAX_RETRIES = 5
BASE_BACKODD = 0.5


@dataclass
class ExtractStats:
    requests_made: int = 0
    retries: int = 0
    not_modified: int = 0
    pages_by_resource: dict = field(default_factory=dict)


class ApiClient:
    """This HTTP client for mock API"""

    def __init__(self, base_url: str, timeout: int = DEFAULT_TIMEOUT):
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()
        self._etag_cache: dict[str, str] = {}
        self.stats = ExtractStats()

    def get(self, path: str, params: dict | None = None) -> dict | None:
        """GET with retry/backoff. Returns pased JSON body"""
        url = f"{self.base_url}{path}"
        cache_key = f"{url}?{sorted((params or {}).items())}"
        headers = {}
        cached_etag = self._etag_cache.get(cache_key)
        if cached_etag:
            headers["If-None-Match"] = cached_etag

        attempt = 0
        while True:
            attempt += 1
            self.stats.requests_made += 1
            try:
                resp = self.session.get(url=url, params=params, headers=headers, timeout=self.timeout)
            except requests.RequestException as e:
                if attempt > MAX_RETRIES:
                    raise
                self._sleep_backoff(attempt)
                logger.warning(f"network error on {url} (attempt {attempt}): {e}")
                continue

            if resp.status_code == 304:
                self.stats.not_modified += 1
                return None

            if resp.status_code in (429, 500,502, 503, 504):
                if attempt > MAX_RETRIES:
                    resp.raise_for_status()
                self.stats.retries += 1
                self._sleep_backoff(attempt=attempt, retry_after=resp.headers.get("Retry-After"))
                logger.warning(f"{resp.status_code} on {url} (attempt {attempt}/{MAX_RETRIES}), retrying")
                continue

            resp.raise_for_status()
            etag = resp.headers.get("Etag")
            if etag:
                self._etag_cache[cache_key] = etag
            return resp.json()


    @staticmethod
    def _sleep_backoff(attempt: int, retry_after: str | None = None):
        if retry_after:
            try:
                time.sleep(float(retry_after))
                return
            except ValueError:
                pass
        delay = BASE_BACKODD * (2 ** (attempt - 1))
        delay += random.uniform(0, delay * 0.25)
        time.sleep(delay)

    def get_all_pages(self, path: str, page_size: int = 100) -> list[dict]:
        items: list[dict] = []
        page = 1
        resource = path.strip("/")
        while True:
            body = self.get(path, params={"page": page, "page_size": page_size})
            if body is None:
                break
            items.extend(body["items"])
            self.stats.pages_by_resource[resource] = page
            if page >= body["pages"]:
                break
            page += 1
        logger.info(f"extracted {len(items)} {resource} across {self.stats.pages_by_resource.get(resource, 0)} page(s)")
        return items


def extract_all(base_url: str, page_size: int = 100) -> dict[str, list[dict]]:
    client = ApiClient(base_url)
    genes = client.get_all_pages(path="/genes", page_size=page_size)
    transcripts = client.get_all_pages(path="/transcripts", page_size=page_size)
    exons = client.get_all_pages(path="/exons", page_size=page_size)
    logger.info(f"extraction complete: {len(genes)} genes, {len(transcripts)} transcripts, {len(exons)} exons ({client.stats.requests_made} http requests, {client.stats.retries} retries)")
    return {"genes": genes, "transcripts": transcripts, "exons": exons}