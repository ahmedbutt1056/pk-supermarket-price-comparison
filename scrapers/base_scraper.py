"""
base_scraper.py — Base class all store scrapers inherit from.
Has built-in:
  - Big random delays between requests (5-12s, looks human)
  - 16 rotating user agents (desktop + mobile browsers)
  - Random referer headers (looks like coming from Google)
  - Session refresh every 15 requests (new cookies, new UA)
  - Proxy rotation support (if proxies configured)
  - Retry with exponential backoff + jitter
  - Random request ordering to avoid predictable patterns
  - Logging to file and console
"""

import time
import random
import requests
from abc import ABC, abstractmethod
from datetime import datetime

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from utils.logger_setup import get_logger, get_attempt_logger


class BaseScraper(ABC):
    """
    Every store scraper (Imtiaz, Metro, AlFatah) extends this class.
    It handles all the HTTP stuff with heavy anti-blocking measures
    so each scraper only needs to worry about parsing HTML.
    """

    def __init__(self, name):
        self.name = name
        self.log = get_logger(f"scraper.{name}")
        self.attempt_log = get_attempt_logger()  # dedicated scraping attempts log

        # proxy list (copy so we can rotate)
        self.proxies = list(config.FREE_PROXIES) if config.FREE_PROXIES else []
        self.current_proxy = None

        # create a fresh browser-like session
        self.session = self._new_session()

        # simple counters
        self.total_hits = 0       # how many requests we made
        self.fails = 0            # how many totally failed
        self.consecutive_fails = 0  # fails in a row (for fast-fail)
        self.found = 0            # products collected
        self.started_at = None
        self.site_is_down = False  # set True if site seems unreachable

    # ----------------------------------------------------------
    # SESSION MANAGEMENT — rotate user-agent, refresh cookies
    # ----------------------------------------------------------

    def _pick_agent(self):
        """Pick a random user-agent string from our list."""
        return random.choice(config.USER_AGENTS)

    def _pick_referer(self):
        """Pick a random referer (or None for direct visit)."""
        return random.choice(config.REFERERS)

    def _pick_proxy(self):
        """Pick a random proxy from the list, or None if no proxies."""
        if not self.proxies:
            return None
        return random.choice(self.proxies)

    def _new_session(self):
        """Create a brand-new requests session with random user-agent + referer."""
        s = requests.Session()

        # copy default headers and set random UA
        headers = dict(config.HEADERS)
        headers["User-Agent"] = self._pick_agent()

        # random referer
        ref = self._pick_referer()
        if ref:
            headers["Referer"] = ref

        s.headers.update(headers)

        # set proxy if available
        self.current_proxy = self._pick_proxy()
        if self.current_proxy:
            s.proxies = {
                "http": self.current_proxy,
                "https": self.current_proxy,
            }
            self.log.debug(f"Using proxy: {self.current_proxy}")

        self.log.debug("New session created with fresh UA + referer")
        return s

    def _maybe_refresh_session(self):
        """After every N requests, start a fresh session (new cookies, new UA, new proxy)."""
        if self.total_hits > 0 and self.total_hits % config.REQUESTS_BEFORE_NEW_SESSION == 0:
            self.session.close()
            self.session = self._new_session()
            self.log.info(f"Session refreshed after {self.total_hits} requests (new UA + cookies + proxy)")

    # ----------------------------------------------------------
    # RANDOM DELAY — sleep a random time so we look like a human
    # ----------------------------------------------------------

    def _wait(self):
        """Sleep random seconds between MIN_DELAY and MAX_DELAY (5-12s)."""
        pause = random.uniform(config.MIN_DELAY, config.MAX_DELAY)

        # add occasional extra-long pauses (1 in 5 chance) to look more human
        if random.random() < 0.2:
            pause += random.uniform(3, 8)
            self.log.debug(f"Extra human pause: {pause:.1f}s ...")
        else:
            self.log.debug(f"Sleeping {pause:.1f}s ...")

        time.sleep(pause)

    def _long_wait(self):
        """Longer pause after an error or between categories (15-40s)."""
        pause = random.uniform(config.MAX_DELAY * 1.5, config.MAX_DELAY * 3.5)
        self.log.debug(f"Long sleep {pause:.1f}s ...")
        time.sleep(pause)

    def _very_long_wait(self):
        """Very long pause when rate-limited or heavily blocked (30-90s)."""
        pause = random.uniform(30, 90)
        self.log.info(f"Very long cooldown: {pause:.0f}s ...")
        time.sleep(pause)

    # ----------------------------------------------------------
    # FETCH PAGE — the main HTTP call with retry + backoff
    # ----------------------------------------------------------

    def get_page(self, url, params=None):
        """
        Download one page. Retries up to RETRY_COUNT times.
        Returns response object, or None if everything failed.

        Anti-blocking features:
          1. Random delay 5-12s BEFORE every request
          2. Occasional extra-long pauses (looks human)
          3. Random user-agent on each new session
          4. Random Referer header (Google, Facebook, or direct)
          5. Session auto-refresh every 15 requests
          6. Proxy rotation (if proxies configured)
          7. Sec-Fetch headers (looks like real browser)
          8. Exponential backoff with random jitter on errors
          9. Very long cooldown on 429 / 403
        """
        # FAST FAIL: if site has failed 3+ times in a row, skip everything
        if self.site_is_down:
            self.log.debug(f"Skipping (site marked as down): {url}")
            return None

        # wait a random time first (human-like browsing)
        self._wait()

        # maybe rotate to a fresh session (new cookies, UA, proxy)
        self._maybe_refresh_session()

        # also rotate User-Agent + Referer for THIS specific request
        self.session.headers["User-Agent"] = self._pick_agent()
        ref = self._pick_referer()
        if ref:
            self.session.headers["Referer"] = ref
        elif "Referer" in self.session.headers:
            del self.session.headers["Referer"]

        for try_num in range(1, config.RETRY_COUNT + 1):
            req_start = time.time()
            try:
                self.total_hits += 1
                self.log.debug(f"GET (try {try_num}): {url}")

                resp = self.session.get(
                    url,
                    params=params,
                    timeout=config.REQUEST_TIMEOUT,
                    allow_redirects=True,
                )
                resp.raise_for_status()

                # Log successful attempt
                elapsed_ms = (time.time() - req_start) * 1000
                self.attempt_log.info(
                    f"{self.name:12s} | GET | {url[:120]} | try={try_num} | "
                    f"status={resp.status_code} | {elapsed_ms:.0f}ms | SUCCESS"
                )

                # success!
                self.consecutive_fails = 0  # reset on success
                return resp

            except requests.exceptions.HTTPError as err:
                code = err.response.status_code if err.response else "?"
                elapsed_ms = (time.time() - req_start) * 1000
                self.attempt_log.warning(
                    f"{self.name:12s} | GET | {url[:120]} | try={try_num} | "
                    f"status={code} | {elapsed_ms:.0f}ms | FAIL | HTTPError"
                )
                self.log.warning(f"HTTP {code} on try {try_num}: {url}")

                # 404 = page doesn't exist, no point retrying
                if code == 404:
                    return None

                # 429 = rate limited OR 403 = forbidden
                if code in (429, 403):
                    self.log.warning(f"Blocked ({code})! Cooling down with very long wait...")
                    self._very_long_wait()
                    # force session refresh to get new identity
                    self.session.close()
                    self.session = self._new_session()

            except requests.exceptions.ConnectionError as err:
                elapsed_ms = (time.time() - req_start) * 1000
                self.attempt_log.warning(
                    f"{self.name:12s} | GET | {url[:120]} | try={try_num} | "
                    f"status=--- | {elapsed_ms:.0f}ms | FAIL | ConnectionError: {err}"
                )
                self.log.warning(f"Connection error try {try_num}: {url}")

            except requests.exceptions.Timeout as err:
                elapsed_ms = (time.time() - req_start) * 1000
                self.attempt_log.warning(
                    f"{self.name:12s} | GET | {url[:120]} | try={try_num} | "
                    f"status=--- | {elapsed_ms:.0f}ms | FAIL | Timeout"
                )
                self.log.warning(f"Timeout try {try_num}: {url}")

            except requests.exceptions.RequestException as err:
                elapsed_ms = (time.time() - req_start) * 1000
                self.attempt_log.error(
                    f"{self.name:12s} | GET | {url[:120]} | try={try_num} | "
                    f"status=--- | {elapsed_ms:.0f}ms | FAIL | {err}"
                )
                self.log.warning(f"Request error try {try_num}: {url} — {err}")

            # backoff: wait longer each retry (8s, 16s, 24s + big random jitter)
            if try_num < config.RETRY_COUNT:
                wait_time = config.RETRY_DELAY * try_num + random.uniform(3, 10)
                self.log.debug(f"Backoff: waiting {wait_time:.1f}s before retry...")
                time.sleep(wait_time)

        # all retries failed
        self.fails += 1
        self.consecutive_fails += 1
        self.log.error(f"FAILED after {config.RETRY_COUNT} tries: {url}")

        # if 3+ consecutive fails, mark site as down to save time
        if self.consecutive_fails >= 3:
            self.site_is_down = True
            self.log.warning(f"*** {self.name} site appears to be DOWN — skipping remaining requests ***")

        return None

    # ----------------------------------------------------------
    # STATS — print a nice summary at the end
    # ----------------------------------------------------------

    def show_stats(self):
        """Print scraping stats to log."""
        if self.started_at:
            secs = (datetime.now() - self.started_at).total_seconds()
        else:
            secs = 0

        self.log.info(f"=== {self.name} DONE ===")
        self.log.info(f"  Requests made : {self.total_hits}")
        self.log.info(f"  Failed        : {self.fails}")
        self.log.info(f"  Products found: {self.found}")
        self.log.info(f"  Time          : {secs:.0f} seconds")

        # Also write summary to scraping attempts log
        self.attempt_log.info(
            f"{'='*80}\n"
            f"  SCRAPER SUMMARY: {self.name}\n"
            f"  Total requests : {self.total_hits}\n"
            f"  Failed requests: {self.fails}\n"
            f"  Products found : {self.found}\n"
            f"  Duration       : {secs:.0f}s\n"
            f"{'='*80}"
        )

    # ----------------------------------------------------------
    # ABSTRACT METHODS — each store scraper must implement these
    # ----------------------------------------------------------

    @abstractmethod
    def scrape(self, city=None):
        """Run the full scrape. Returns list of product dicts."""
        pass

    @abstractmethod
    def get_categories(self, city=None):
        """Return list of category info dicts to scrape."""
        pass

    @abstractmethod
    def parse_products(self, html, category, city):
        """Parse one listing page HTML. Return list of product dicts."""
        pass
