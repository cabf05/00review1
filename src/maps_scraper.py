from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import logging
import re
import tempfile
import time
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, build_opener
from urllib.error import HTTPError, URLError

from dateutil import parser


logger = logging.getLogger(__name__)

BLOCKED_TEMPORARY = "BLOCKED_TEMPORARY"
DOM_CHANGED = "DOM_CHANGED"
TIMEOUT = "TIMEOUT"
NO_REVIEWS = "NO_REVIEWS"


class MapsScraperError(Exception):
    """Erro amigável para falhas de scraping do Google Maps."""

    def __init__(self, message: str, code: str = BLOCKED_TEMPORARY) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class ScraperConfig:
    total_timeout_seconds: int = 180
    step_retries: int = 3
    no_new_items_limit: int = 5
    scroll_pause_seconds: float = 1.2
    retry_base_delay_seconds: float = 0.6
    retry_max_delay_seconds: float = 4.0
    user_agent: str = (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    )


def scrape_reviews(maps_url: str, days: int) -> list[dict]:
    """Extrai avaliações recentes de uma página de local no Google Maps.

    Retorna uma lista normalizada de dicts com o contrato esperado por
    ``src/reviews_service.py`` (campos de review).
    """
    if not maps_url or not maps_url.strip():
        raise MapsScraperError("Informe uma URL do Google Maps.")
    if days < 1:
        raise MapsScraperError("O número de dias deve ser maior ou igual a 1.")

    config = ScraperConfig()
    started_at = time.monotonic()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    try:
        _log_event("info", "init_scrape", days=days)
        final_url = _resolve_maps_url(maps_url)
        reviews = _scrape_with_playwright(final_url, cutoff, config, started_at)
        _log_event("info", "scrape_completed", total_reviews=len(reviews))
        return reviews
    except MapsScraperError:
        raise
    except Exception as exc:  # pragma: no cover - erro de última camada
        raise MapsScraperError(
            "Não foi possível coletar avaliações automaticamente neste momento. "
            "Isso normalmente é bloqueio temporário, timeout de rede ou variação transitória do Google Maps."
            ,
            code=BLOCKED_TEMPORARY,
        ) from exc


def _resolve_maps_url(maps_url: str) -> str:
    raw_url = maps_url.strip()
    request = Request(
        raw_url,
        headers={
            "User-Agent": ScraperConfig.user_agent,
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        },
    )
    opener = build_opener()
    try:
        with opener.open(request, timeout=20) as response:
            resolved = response.geturl()
    except (HTTPError, URLError, TimeoutError) as exc:
        _log_event("warn", "resolve_maps_url_failed", reason=type(exc).__name__)
        return raw_url

    parsed = urlparse(resolved)
    if "google" not in parsed.netloc.lower() and "goo.gl" not in parsed.netloc.lower():
        original_parsed = urlparse(raw_url)
        if "google" in original_parsed.netloc.lower() or "goo.gl" in original_parsed.netloc.lower():
            _log_event("warn", "resolved_url_not_google_keep_original", resolved=resolved)
            return raw_url
        raise MapsScraperError("URL resolvida não parece ser do Google Maps.", code=BLOCKED_TEMPORARY)

    return resolved


def _scrape_with_playwright(
    final_url: str,
    cutoff: datetime,
    config: ScraperConfig,
    started_at: float,
) -> list[dict]:
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise MapsScraperError(
            "Playwright não está disponível no ambiente. "
            "Instale 'playwright' e os browsers antes de executar o scraper."
        ) from exc

    try:
        from playwright_stealth import stealth_sync
    except Exception:
        stealth_sync = None

    reviews_by_id: dict[str, dict[str, Any]] = {}
    no_new_items = 0
    stop_due_to_cutoff = False
    checkpoint = ScraperCheckpoint()

    with sync_playwright() as p:
        with tempfile.TemporaryDirectory(prefix="maps-scraper-") as user_data_dir:
            browser_context = p.chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                headless=True,
                user_agent=config.user_agent,
                locale="pt-BR",
                timezone_id="UTC",
                viewport={"width": 1366, "height": 900},
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-first-run",
                    "--no-default-browser-check",
                ],
            )
            page = browser_context.new_page()
            page.set_default_timeout(15000)
            if stealth_sync is not None:
                stealth_sync(page)

            _run_step_with_retries(
                lambda: page.goto(final_url, wait_until="domcontentloaded", timeout=30000),
                "abrir URL do local",
                config,
                started_at,
                PlaywrightTimeoutError,
                TIMEOUT,
            )
            _raise_if_temporarily_blocked(page)

            _open_reviews_panel(page, config, started_at, PlaywrightTimeoutError)
            _sort_by_most_recent(page, config, started_at, PlaywrightTimeoutError)
            container = _find_reviews_container(page)

            while True:
                _ensure_not_timed_out(started_at, config.total_timeout_seconds)
                _raise_if_temporarily_blocked(page)

                current_batch = _run_step_with_retries(
                    lambda: _extract_reviews_from_dom(page),
                    "parse do lote de reviews",
                    config,
                    started_at,
                    PlaywrightTimeoutError,
                    DOM_CHANGED,
                )
                added_count = 0
                oldest_in_batch: datetime | None = None

                for item in current_batch:
                    rid = item.get("reviewId")
                    if not rid:
                        continue
                    if rid not in reviews_by_id:
                        reviews_by_id[rid] = item
                        added_count += 1
                        checkpoint.last_review_id = rid

                    published_dt = _safe_parse_datetime(item.get("publishedAtDate"))
                    if published_dt is not None:
                        oldest_in_batch = (
                            published_dt
                            if oldest_in_batch is None
                            else min(oldest_in_batch, published_dt)
                        )
                        checkpoint.oldest_seen_date = (
                            published_dt
                            if checkpoint.oldest_seen_date is None
                            else min(checkpoint.oldest_seen_date, published_dt)
                        )

                if oldest_in_batch is not None and oldest_in_batch < cutoff:
                    stop_due_to_cutoff = True

                if added_count == 0:
                    no_new_items += 1
                else:
                    no_new_items = 0

                if stop_due_to_cutoff or no_new_items >= config.no_new_items_limit:
                    break

                _run_step_with_retries(
                    lambda: _scroll_container(container, page, config),
                    "scroll no painel de avaliações",
                    config,
                    started_at,
                    PlaywrightTimeoutError,
                    TIMEOUT,
                )

            browser_context.close()

    if stop_due_to_cutoff:
        _log_event("info", "stop_reason", reason="min_date_reached", checkpoint=checkpoint.to_dict())
    elif no_new_items >= config.no_new_items_limit:
        _log_event("info", "stop_reason", reason="no_new_items", checkpoint=checkpoint.to_dict())

    return _finalize_reviews(reviews_by_id, cutoff, checkpoint)


def _open_reviews_panel(page, config: ScraperConfig, started_at: float, timeout_exc: type[Exception]) -> None:
    selectors = [
        'button[jsaction*="pane.reviewChart.moreReviews"]',
        'button[data-value="Reviews"]',
        'button[aria-label*="reviews"]',
        'button[aria-label*="avalia"]',
        'button:has-text("avalia")',
        'button:has-text("reviews")',
        '[role="tab"]:has-text("Avaliações")',
        '[role="tab"]:has-text("Reviews")',
    ]

    def _click_first_found() -> None:
        for selector in selectors:
            locator = page.locator(selector).first
            if locator.count() > 0:
                locator.click()
                return
        raise MapsScraperError(
            "Não foi possível abrir o painel de avaliações. O layout da página pode ter mudado."
        , code=DOM_CHANGED)

    _run_step_with_retries(
        _click_first_found,
        "abrir painel de avaliações",
        config,
        started_at,
        timeout_exc,
        DOM_CHANGED,
    )


def _sort_by_most_recent(page, config: ScraperConfig, started_at: float, timeout_exc: type[Exception]) -> None:
    def _sort() -> None:
        page.locator('button[aria-label*="Sort"]').first.click()
        option_candidates = [
            page.get_by_role("menuitemradio", name=re.compile("mais recentes", re.I)).first,
            page.get_by_role("menuitemradio", name=re.compile("newest", re.I)).first,
            page.locator('div[role="menu"] [role="menuitemradio"]:has-text("Mais recentes")').first,
            page.locator('div[role="menu"] [role="menuitemradio"]:has-text("Newest")').first,
        ]
        for opt in option_candidates:
            if opt.count() > 0:
                opt.click()
                return
        raise MapsScraperError(
            "Não foi possível ordenar por 'Mais recentes'. O layout da página pode ter mudado."
        , code=DOM_CHANGED)

    _run_step_with_retries(
        _sort,
        "ordenar reviews por mais recentes",
        config,
        started_at,
        timeout_exc,
        DOM_CHANGED,
    )


def _find_reviews_container(page):
    candidates = [
        'div[role="main"] div[aria-label*="Reviews"]',
        'div[role="main"] div[aria-label*="Avaliações"]',
        'div[role="feed"]',
        'div[role="region"] div[role="feed"]',
        "div.m6QErb[aria-label]",
    ]
    for selector in candidates:
        loc = page.locator(selector).first
        if loc.count() > 0:
            return loc
    raise MapsScraperError(
        "Não foi possível localizar o container de avaliações. O layout da página pode ter mudado."
    , code=DOM_CHANGED)


def _extract_reviews_from_dom(page) -> list[dict[str, Any]]:
    js = """
    () => {
      const cards = Array.from(document.querySelectorAll(
        'div.jftiEf, div[data-review-id], div[jscontroller*="e6Mltc"], div[class*="jJc9Ad"]'
      ));
      return cards.map((card) => {
        const reviewId = card.getAttribute('data-review-id') || card.getAttribute('jslog') || '';

        const nameEl = card.querySelector('.d4r55, .TSUbDb');
        const textEl = card.querySelector('.wiI7pd, .MyEned, span[jsname="bN97Pc"], div[data-expandable-section]');
        const starEl = card.querySelector('[role="img"][aria-label*="star"], [role="img"][aria-label*="estrela"]');
        const dateEl = card.querySelector('.rsqaWe, .xRkPPb, span[class*="rsqaWe"], span[data-value="review-date"], span[jsname="rsqaWe"]');
        const likesEl = card.querySelector('.GBkF3d, .pkWtMe');
        const reviewLinkEl = card.querySelector('a[href*="/maps/reviews/"]');
        const ownerRespEl = card.querySelector('.CDe7pd, .wiI7pd + div');

        const starLabel = starEl ? (starEl.getAttribute('aria-label') || '') : '';
        const starsMatch = starLabel.match(/(\d+[\.,]?\d*)/);

        return {
          reviewId,
          name: nameEl ? nameEl.textContent.trim() : '',
          text: textEl ? textEl.textContent.trim() : '',
          stars: starsMatch ? Number(starsMatch[1].replace(',', '.')) : null,
          publishedAtDate: dateEl ? dateEl.textContent.trim() : '',
          likesCount: likesEl ? Number((likesEl.textContent || '0').replace(/\D/g, '') || '0') : 0,
          reviewUrl: reviewLinkEl ? reviewLinkEl.href : '',
          responseFromOwnerText: ownerRespEl ? ownerRespEl.textContent.trim() : '',
        };
      });
    }
    """
    raw_items = page.evaluate(js)
    normalized_items: list[dict[str, Any]] = []

    for idx, item in enumerate(raw_items or []):
        review_id = _normalize_review_id(item.get("reviewId") or "", idx)
        normalized_items.append(
            {
                "reviewId": review_id,
                "title": "",
                "name": (item.get("name") or "").strip(),
                "text": (item.get("text") or "").strip(),
                "stars": item.get("stars"),
                "publishedAtDate": _normalize_date_text(item.get("publishedAtDate") or ""),
                "likesCount": item.get("likesCount") or 0,
                "reviewUrl": item.get("reviewUrl") or "",
                "responseFromOwnerText": (item.get("responseFromOwnerText") or "").strip(),
            }
        )
    return normalized_items


def _raise_if_temporarily_blocked(page) -> None:
    page_text = (page.inner_text("body", timeout=2000) or "").lower()
    block_signals = [
        "unusual traffic",
        "detected unusual traffic",
        "verify you are human",
        "i'm not a robot",
        "não sou um robô",
        "tráfego incomum",
        "captcha",
    ]
    if any(signal in page_text for signal in block_signals):
        raise MapsScraperError(
            "O Google solicitou validação humana (captcha/tráfego incomum).",
            code=BLOCKED_TEMPORARY,
        )


def _scroll_container(container, page, config: ScraperConfig) -> None:
    container.evaluate("(el) => { el.scrollBy(0, Math.floor(el.clientHeight * 0.9)); }")
    page.wait_for_timeout(int(config.scroll_pause_seconds * 1000))


def _normalize_review_id(raw_review_id: str, idx: int) -> str:
    if not raw_review_id:
        return f"fallback-{idx}"

    candidate = raw_review_id
    if "review_id" in raw_review_id:
        parsed = parse_qs(urlparse(raw_review_id).query)
        candidate = parsed.get("review_id", [raw_review_id])[0]

    clean = re.sub(r"[^A-Za-z0-9_-]", "", candidate)
    return clean or f"fallback-{idx}"


def _normalize_date_text(value: str) -> str:
    value = (value or "").strip()
    parsed = _safe_parse_datetime(value)
    if parsed is None:
        return value
    return parsed.date().isoformat()


def _safe_parse_datetime(raw: Any) -> datetime | None:
    if not raw:
        return None

    text = str(raw).strip()
    relative_match = re.match(r"^há\s+(\d+)\s+(dia|dias|semana|semanas|mês|meses|ano|anos)$", text, re.I)
    if relative_match:
        qty = int(relative_match.group(1))
        unit = relative_match.group(2).lower()
        now = datetime.now(timezone.utc)
        if unit.startswith("dia"):
            return now - timedelta(days=qty)
        if unit.startswith("semana"):
            return now - timedelta(days=qty * 7)
        if unit in {"mês", "meses"}:
            return now - timedelta(days=qty * 30)
        if unit.startswith("ano"):
            return now - timedelta(days=qty * 365)

    try:
        dt = parser.parse(text, dayfirst=True)
    except Exception:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _finalize_reviews(
    reviews_by_id: dict[str, dict[str, Any]],
    cutoff: datetime,
    checkpoint: "ScraperCheckpoint",
) -> list[dict]:
    out: list[dict] = []
    for item in reviews_by_id.values():
        published_dt = _safe_parse_datetime(item.get("publishedAtDate"))
        if published_dt is None:
            continue
        if published_dt < cutoff:
            continue

        normalized = {
            "reviewId": item.get("reviewId") or "",
            "title": item.get("title") or "",
            "name": item.get("name") or "",
            "text": item.get("text") or "",
            "publishedAtDate": published_dt.date().isoformat(),
            "stars": item.get("stars"),
            "likesCount": item.get("likesCount") or 0,
            "reviewUrl": item.get("reviewUrl") or "",
            "responseFromOwnerText": item.get("responseFromOwnerText") or "",
        }
        out.append(normalized)

    out.sort(key=lambda r: r.get("publishedAtDate", ""), reverse=True)
    if not out:
        _log_event("warn", "stop_reason", reason="no_reviews_after_filter", checkpoint=checkpoint.to_dict())
        raise MapsScraperError(
            "Nenhuma avaliação encontrada para o período informado.",
            code=NO_REVIEWS,
        )
    return out


def _run_step_with_retries(
    fn: Callable[[], Any],
    step_name: str,
    config: ScraperConfig,
    started_at: float,
    timeout_exception_type: type[Exception],
    code_on_failure: str,
) -> Any:
    last_error: Exception | None = None
    for attempt in range(1, config.step_retries + 1):
        _ensure_not_timed_out(started_at, config.total_timeout_seconds)
        try:
            return fn()
        except (timeout_exception_type, MapsScraperError) as exc:
            last_error = exc
            _log_event(
                "warn",
                "step_retry",
                step=step_name,
                attempt=attempt,
                reason=type(exc).__name__,
            )
            if attempt >= config.step_retries:
                break
            _sleep_with_exponential_backoff(attempt, config)
        except Exception as exc:  # pragma: no cover
            last_error = exc
            _log_event(
                "warn",
                "step_retry",
                step=step_name,
                attempt=attempt,
                reason=type(exc).__name__,
            )
            if attempt >= config.step_retries:
                break
            _sleep_with_exponential_backoff(attempt, config)

    _log_event(
        "error",
        "step_failed",
        step=step_name,
        retries=config.step_retries,
        reason=type(last_error).__name__ if last_error else "unknown",
    )
    raise MapsScraperError(
        f"Falha ao {step_name} após {config.step_retries} tentativas. "
        "O layout do Google Maps pode ter mudado."
        , code=code_on_failure) from last_error


def _ensure_not_timed_out(started_at: float, total_timeout_seconds: int) -> None:
    if time.monotonic() - started_at > total_timeout_seconds:
        raise MapsScraperError(
            f"Tempo limite total excedido ({total_timeout_seconds}s) durante a coleta de reviews."
            ,
            code=TIMEOUT,
        )


def _sleep_with_exponential_backoff(attempt: int, config: ScraperConfig) -> None:
    delay = min(config.retry_base_delay_seconds * (2 ** (attempt - 1)), config.retry_max_delay_seconds)
    time.sleep(delay)


@dataclass
class ScraperCheckpoint:
    last_review_id: str | None = None
    oldest_seen_date: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "last_review_id": self.last_review_id,
            "oldest_seen_date": self.oldest_seen_date.isoformat() if self.oldest_seen_date else None,
        }


def _log_event(level: str, event: str, **payload: Any) -> None:
    message = {"event": event, **payload}
    if level == "error":
        logger.error(message)
    elif level == "warn":
        logger.warning(message)
    else:
        logger.info(message)
