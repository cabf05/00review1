"""
Google Maps Review Scraper

Uses Playwright (headless Chromium) to navigate Google Maps,
scroll through reviews, and extract structured review data.

Anti-detection measures:
- Realistic User-Agent rotation
- navigator.webdriver masking
- Random human-like delays between actions
- Proper locale/timezone configuration
"""

import re
import time
import random
import subprocess
import sys
import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import requests as http_req

logger = logging.getLogger(__name__)


def install_browser():
    """Install Playwright Chromium browser (needed once per deployment)."""
    try:
        subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            capture_output=True,
            text=True,
            timeout=300,
        )
        return True
    except Exception as e:
        logger.error(f"Browser install failed: {e}")
        return False


class ReviewScraper:
    """Scrape Google Maps reviews using a headless browser."""

    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) "
        "Gecko/20100101 Firefox/127.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    ]

    def __init__(self):
        self._pw = None
        self._browser = None
        self._ctx = None
        self._page = None
        self.title = ""

    # ------------------------------------------------------------------ #
    #  Browser lifecycle                                                   #
    # ------------------------------------------------------------------ #

    def _open(self):
        from playwright.sync_api import sync_playwright

        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-gpu",
                "--disable-extensions",
                "--disable-infobars",
                "--window-size=1920,1080",
                "--lang=en-US",
            ],
        )
        self._ctx = self._browser.new_context(
            user_agent=random.choice(self.USER_AGENTS),
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="America/New_York",
        )
        self._ctx.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.chrome = {runtime: {}, loadTimes: function(){}, csi: function(){}};
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            """
        )
        self._page = self._ctx.new_page()
        self._page.set_default_timeout(30_000)
        self._page.set_default_navigation_timeout(60_000)

    def _close(self):
        for obj in (self._page, self._ctx, self._browser):
            try:
                if obj:
                    obj.close()
            except Exception:
                pass
        try:
            if self._pw:
                self._pw.stop()
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    #  URL helpers                                                         #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _resolve_url(url):
        """Follow redirects from shortened Google Maps URLs."""
        try:
            r = http_req.get(
                url,
                allow_redirects=True,
                timeout=20,
                stream=True,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/126.0.0.0 Safari/537.36"
                    )
                },
            )
            r.close()
            return r.url
        except Exception:
            return url

    @staticmethod
    def _ensure_english(url):
        """Append hl=en to force English UI."""
        p = urlparse(url)
        q = parse_qs(p.query)
        q["hl"] = ["en"]
        return urlunparse(p._replace(query=urlencode(q, doseq=True)))

    # ------------------------------------------------------------------ #
    #  Page interaction helpers                                            #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _delay(lo=1.5, hi=3.5):
        time.sleep(random.uniform(lo, hi))

    def _dismiss_popups(self):
        """Dismiss cookie-consent / sign-in popups."""
        for sel in [
            'button:has-text("Accept all")',
            'button:has-text("Reject all")',
            'button:has-text("I agree")',
            'button:has-text("Got it")',
            'form[action*="consent"] button[type="submit"]',
        ]:
            try:
                btn = self._page.query_selector(sel)
                if btn and btn.is_visible():
                    btn.click()
                    time.sleep(1.5)
                    return
            except Exception:
                continue

    def _click_reviews_tab(self):
        for sel in [
            'button[role="tab"]:has-text("Reviews")',
            'button[role="tab"]:has-text("reviews")',
        ]:
            try:
                el = self._page.query_selector(sel)
                if el and el.is_visible():
                    el.click()
                    self._delay(2, 4)
                    return True
            except Exception:
                continue
        return False

    def _sort_by_newest(self):
        # Step 1 – open the sort menu
        sort_btn = None
        for sel in [
            'button[aria-label*="Sort"]',
            'button[aria-label*="sort"]',
            'button[data-value="Sort"]',
        ]:
            sort_btn = self._page.query_selector(sel)
            if sort_btn and sort_btn.is_visible():
                break
            sort_btn = None

        if not sort_btn:
            return False

        try:
            sort_btn.click()
            self._delay(1, 2)
        except Exception:
            return False

        # Step 2 – pick "Newest"
        for sel in [
            'div[role="menuitemradio"]:has-text("Newest")',
            'li[role="menuitemradio"]:has-text("Newest")',
            '[data-index="1"][role="menuitemradio"]',
        ]:
            try:
                el = self._page.query_selector(sel)
                if el and el.is_visible():
                    el.click()
                    self._delay(2, 4)
                    return True
            except Exception:
                continue
        return False

    def _expand_reviews(self):
        """Click all 'More' buttons so full review text is visible."""
        try:
            for btn in self._page.query_selector_all("button.w8nwRe, button.M77dve"):
                try:
                    if btn.is_visible():
                        btn.click()
                        time.sleep(0.15)
                except Exception:
                    pass
        except Exception:
            pass

    def _get_scrollable(self):
        for sel in [
            "div.m6QErb.DxyBCb.kA9KIf.dS8AEf",
            "div.m6QErb.DxyBCb.kA9KIf",
            'div[role="feed"]',
            "div.m6QErb",
        ]:
            el = self._page.query_selector(sel)
            if el:
                return el
        return None

    # ------------------------------------------------------------------ #
    #  Date parsing                                                        #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _relative_to_iso(text):
        """Convert '3 months ago' → ISO-8601 datetime string."""
        now = datetime.now(timezone.utc)
        if not text:
            return now.isoformat()

        t = re.sub(r"\b(a|an)\s+", "1 ", text.lower().strip())
        m = re.search(r"(\d+)\s+(second|minute|hour|day|week|month|year)", t)
        if not m:
            return now.isoformat()

        n, unit = int(m.group(1)), m.group(2)
        deltas = {
            "second": timedelta(seconds=n),
            "minute": timedelta(minutes=n),
            "hour": timedelta(hours=n),
            "day": timedelta(days=n),
            "week": timedelta(weeks=n),
            "month": timedelta(days=n * 30),
            "year": timedelta(days=n * 365),
        }
        return (now - deltas.get(unit, timedelta())).isoformat()

    # ------------------------------------------------------------------ #
    #  Single-review DOM parser                                            #
    # ------------------------------------------------------------------ #

    def _parse_review(self, el, rid):
        """Extract all fields from one review DOM element."""
        r = {
            "title": self.title,
            "reviewerId": "",
            "reviewerUrl": "",
            "name": "",
            "reviewerNumberOfReviews": 0,
            "isLocalGuide": False,
            "reviewerPhotoUrl": "",
            "text": "",
            "textTranslated": None,
            "publishAt": "",
            "publishedAtDate": "",
            "likesCount": 0,
            "reviewId": rid or "",
            "reviewUrl": "",
            "reviewOrigin": "Google",
            "stars": 0,
            "rating": None,
            "responseFromOwnerDate": None,
            "responseFromOwnerText": None,
            "reviewImageUrls": [],
            "reviewContext": {},
            "reviewDetailedRating": {},
            "visitedIn": None,
            "originalLanguage": "en",
            "translatedLanguage": None,
        }

        try:
            # ── Reviewer name & profile URL ──
            name_el = el.query_selector(".d4r55")
            if name_el:
                link = name_el.query_selector("a")
                if link:
                    r["name"] = (link.inner_text() or "").strip()
                    href = link.get_attribute("href") or ""
                    if href.startswith("/"):
                        href = "https://www.google.com" + href
                    r["reviewerUrl"] = href
                    mid = re.search(r"/contrib/(\d+)", href)
                    if mid:
                        r["reviewerId"] = mid.group(1)
                else:
                    r["name"] = (name_el.inner_text() or "").strip()

            # ── Reviewer photo ──
            photo = el.query_selector("a[href*='contrib'] img, img.NBa7we")
            if photo:
                src = photo.get_attribute("src") or ""
                if "googleusercontent" in src:
                    r["reviewerPhotoUrl"] = src

            # ── Local Guide badge & review count ──
            badge = el.query_selector(".RfnDt")
            if badge:
                btxt = (badge.inner_text() or "").lower()
                r["isLocalGuide"] = "local guide" in btxt
                cnt = re.search(r"(\d+)\s+review", btxt)
                if cnt:
                    r["reviewerNumberOfReviews"] = int(cnt.group(1))

            # ── Star rating ──
            stars_el = el.query_selector("[role='img'][aria-label]")
            if stars_el:
                aria = stars_el.get_attribute("aria-label") or ""
                sm = re.search(r"(\d)", aria)
                if sm:
                    r["stars"] = int(sm.group(1))

            # ── Review text ──
            text_el = el.query_selector(".wiI7pd")
            if text_el:
                r["text"] = (text_el.inner_text() or "").strip()

            # ── Translated text ──
            try:
                translated_el = el.query_selector(".review-full-text")
                if translated_el and translated_el != text_el:
                    r["textTranslated"] = (translated_el.inner_text() or "").strip()
            except Exception:
                pass

            # ── Published date ──
            date_el = el.query_selector(".rsqaWe")
            if date_el:
                raw = (date_el.inner_text() or "").strip()
                r["publishAt"] = raw
                r["publishedAtDate"] = self._relative_to_iso(raw)

            # ── Likes count ──
            try:
                for sel in (".pkWtMe", ".GBkF3d"):
                    lk = el.query_selector(sel)
                    if lk:
                        lt = (lk.inner_text() or "").strip()
                        if lt.isdigit():
                            r["likesCount"] = int(lt)
                            break
            except Exception:
                pass

            # ── Owner response ──
            try:
                resp_box = el.query_selector(".CDe7pd")
                if resp_box:
                    resp_txt = resp_box.query_selector(".wiI7pd")
                    if resp_txt:
                        r["responseFromOwnerText"] = (
                            resp_txt.inner_text() or ""
                        ).strip()
                    resp_date = resp_box.query_selector(".rsqaWe")
                    if resp_date:
                        r["responseFromOwnerDate"] = self._relative_to_iso(
                            (resp_date.inner_text() or "").strip()
                        )
            except Exception:
                pass

            # ── Review images ──
            try:
                for img in el.query_selector_all("button[jsaction] img"):
                    src = img.get_attribute("src") or ""
                    if src and ("googleusercontent" in src or "ggpht" in src):
                        r["reviewImageUrls"].append(src)
            except Exception:
                pass

            # ── Detailed ratings (restaurants etc.) ──
            try:
                for item in el.query_selector_all(
                    ".PuiEXc .BHOKXe, .k4wkje .BHOKXe"
                ):
                    label_el = item.query_selector(".RfDO5c, .PbZDve")
                    value_el = item.query_selector("[aria-label]")
                    if label_el and value_el:
                        label = (label_el.inner_text() or "").strip()
                        val_aria = value_el.get_attribute("aria-label") or ""
                        vm = re.search(r"(\d)", val_aria)
                        if label and vm:
                            r["reviewDetailedRating"][label] = int(vm.group(1))
            except Exception:
                pass

            # ── Visited in ──
            try:
                for span in el.query_selector_all("span"):
                    stxt = (span.inner_text() or "").strip()
                    if stxt.lower().startswith("visited in"):
                        r["visitedIn"] = stxt.replace("Visited in ", "").strip()
                        break
            except Exception:
                pass

            # ── Review URL ──
            if rid:
                r["reviewUrl"] = (
                    "https://www.google.com/maps/reviews/data="
                    f"!4m8!14m7!1m6!2m5!1s{rid}"
                    "!2m1!1s0x0:0x0!3m1!1s2@1:CAEQAA%7C%7C?hl=en"
                )

        except Exception as e:
            logger.debug("Parse error for review %s: %s", rid, e)

        return r

    # ------------------------------------------------------------------ #
    #  Main entry point                                                    #
    # ------------------------------------------------------------------ #

    def scrape(self, url, max_days=2000, on_progress=None):
        """
        Scrape all reviews from a Google Maps URL.

        Args:
            url:          Google Maps URL (short or full).
            max_days:     Only collect reviews from the last N days.
            on_progress:  Optional callback(message: str) for status updates.

        Returns:
            List of review dicts matching the standard schema.
        """
        try:
            self._report(on_progress, "Starting browser...")
            self._open()

            # Resolve shortened URL
            self._report(on_progress, "Resolving URL...")
            resolved = self._resolve_url(url)
            target = self._ensure_english(resolved)

            # Navigate to the place page
            self._report(on_progress, "Loading Google Maps page...")
            self._page.goto(target, wait_until="domcontentloaded")
            self._delay(3, 5)
            self._dismiss_popups()
            self._delay(1, 2)

            # Detect blocks
            html = self._page.content()
            if "unusual traffic" in html.lower() or "captcha" in html.lower():
                raise RuntimeError(
                    "Google detected automated access. Please wait a few minutes "
                    "and try again."
                )

            # Wait for page content
            try:
                self._page.wait_for_selector("h1", timeout=15_000)
            except Exception:
                pass

            h1 = self._page.query_selector("h1")
            self.title = (h1.inner_text() if h1 else "").strip() or "Unknown Place"
            self._report(on_progress, f"Found: {self.title}")

            # Open reviews tab & sort by newest
            self._report(on_progress, "Opening reviews section...")
            self._click_reviews_tab()
            self._report(on_progress, "Sorting by newest...")
            self._sort_by_newest()
            self._delay(1, 2)

            # Scroll & collect
            self._report(on_progress, "Collecting reviews...")
            reviews = self._scroll_and_collect(max_days, on_progress)

            self._report(
                on_progress, f"Done! Collected {len(reviews)} reviews."
            )
            return reviews

        finally:
            self._close()

    @staticmethod
    def _report(cb, msg):
        if cb:
            cb(msg)

    def _scroll_and_collect(self, max_days, on_progress):
        all_reviews = []
        seen = set()
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_days)
        empty_rounds = 0
        scrollable = self._get_scrollable()

        while empty_rounds < 6:
            self._expand_reviews()

            els = self._page.query_selector_all("[data-review-id]")
            new_count = 0
            hit_cutoff = False

            for el in els:
                rid = el.get_attribute("data-review-id")
                if not rid or rid in seen:
                    continue
                seen.add(rid)
                new_count += 1

                review = self._parse_review(el, rid)

                # Date-based stop
                if review["publishedAtDate"]:
                    try:
                        pub = datetime.fromisoformat(review["publishedAtDate"])
                        if pub.tzinfo is None:
                            pub = pub.replace(tzinfo=timezone.utc)
                        if pub < cutoff:
                            hit_cutoff = True
                            break
                    except Exception:
                        pass

                all_reviews.append(review)

            if on_progress and new_count > 0:
                self._report(
                    on_progress, f"Collected {len(all_reviews)} reviews..."
                )

            if hit_cutoff:
                break

            empty_rounds = 0 if new_count else empty_rounds + 1

            # Scroll the review panel
            if scrollable:
                try:
                    scrollable.evaluate(
                        "el => el.scrollBy(0, el.clientHeight * 3)"
                    )
                except Exception:
                    self._page.keyboard.press("End")
            else:
                self._page.keyboard.press("End")

            self._delay(2, 4.5)

        return all_reviews
