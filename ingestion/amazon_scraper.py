"""
Direct Amazon HTML scraper using httpx + BeautifulSoup.
Set PROXY_URL env var to a residential proxy for reliable operation.
e.g. PROXY_URL=http://user:pass@residential-proxy-host:port

Falls back to omkarcloud API if PROXY_URL is not set.
"""
import os
import re
import random
import asyncio
import logging
import httpx
from bs4 import BeautifulSoup
from models.amazon_listing import AmazonListing

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]


def _headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }


def _parse_price(el) -> float | None:
    if not el:
        return None
    text = el.get_text()
    text = re.sub(r'[^\d.]', '', text.replace(',', ''))
    try:
        return float(text) if text else None
    except ValueError:
        return None


def _parse_int(text: str) -> int | None:
    if not text:
        return None
    cleaned = re.sub(r'[^\d]', '', text)
    return int(cleaned) if cleaned else None


def _parse_product(item) -> AmazonListing | None:
    asin = item.get("data-asin", "").strip()
    if not asin:
        return None

    # ── Title ────────────────────────────────────────────────────────────────
    title_el = (
        item.select_one("h2 a span") or
        item.select_one("h2 span.a-text-normal") or
        item.select_one("h2 span")
    )
    title = title_el.get_text(strip=True) if title_el else ""
    if not title:
        return None

    # ── Link ─────────────────────────────────────────────────────────────────
    link_el = item.select_one("h2 a[href]")
    link = ("https://www.amazon.com" + link_el["href"]) if link_el else None

    # ── Price ─────────────────────────────────────────────────────────────────
    # Prefer the "xl" sized price (main selling price)
    price_el = item.select_one('.a-price[data-a-size="xl"] .a-offscreen')
    if not price_el:
        price_el = item.select_one(".a-price .a-offscreen")
    price = _parse_price(price_el)

    # ── Original price ────────────────────────────────────────────────────────
    orig_el = item.select_one('.a-price[data-a-strike="true"] .a-offscreen')
    original_price = _parse_price(orig_el)

    # ── Rating ────────────────────────────────────────────────────────────────
    rating = None
    rating_el = item.select_one(".a-icon-alt")
    if rating_el:
        m = re.search(r"([\d.]+)\s+out of", rating_el.get_text())
        if m:
            rating = float(m.group(1))

    # ── Reviews ───────────────────────────────────────────────────────────────
    reviews = None
    # Method 1: aria-label on the ratings count link
    for a in item.select("a[href*='customerReviews'], a[href*='#customerReviews']"):
        aria = a.get("aria-label", "")
        m = re.search(r"([\d,]+)", aria)
        if m:
            reviews = _parse_int(m.group(1))
            break
    # Method 2: span immediately after the star rating span
    if reviews is None:
        for span in item.select("span.a-size-base.s-underline-text"):
            text = span.get_text(strip=True).replace(",", "")
            if text.isdigit():
                reviews = int(text)
                break
    # Method 3: any span that looks like a review count
    if reviews is None:
        for span in item.select("span.a-size-base"):
            text = span.get_text(strip=True).replace(",", "")
            if text.isdigit() and int(text) > 10:
                reviews = int(text)
                break

    # ── Badges ────────────────────────────────────────────────────────────────
    badge_texts = [b.get_text(strip=True).lower() for b in item.select(".a-badge-text")]
    is_best_seller  = any("best seller" in t for t in badge_texts)
    is_amazon_choice = any("amazon's choice" in t or "amazons choice" in t for t in badge_texts)
    is_prime = bool(
        item.select_one(".s-prime") or
        item.select_one("i.a-icon-prime") or
        item.select_one("[aria-label='Amazon Prime']")
    )

    # ── Image ─────────────────────────────────────────────────────────────────
    img_el = item.select_one("img.s-image")
    image_url = img_el["src"] if img_el else None

    # ── Sales volume ──────────────────────────────────────────────────────────
    sales_volume = None
    for span in item.select("span.a-size-base.a-color-secondary"):
        text = span.get_text(strip=True)
        if "bought" in text.lower():
            sales_volume = text
            break

    return AmazonListing(
        asin=asin,
        title=title,
        price=price,
        original_price=original_price,
        currency="USD",
        rating=rating,
        reviews=reviews,
        link=link,
        image_url=image_url,
        is_best_seller=is_best_seller,
        is_amazon_choice=is_amazon_choice,
        is_prime=is_prime,
        sales_volume=sales_volume,
    )


async def scrape_amazon_search(query: str, page: int = 1) -> list[AmazonListing]:
    """
    Scrape Amazon search results directly.
    Requires PROXY_URL env var pointing to a residential proxy.
    """
    proxy_url = os.getenv("PROXY_URL")
    url = f"https://www.amazon.com/s?k={query.replace(' ', '+')}&page={page}"

    transport = None
    if proxy_url:
        transport = httpx.AsyncHTTPTransport(proxy=proxy_url)

    async with httpx.AsyncClient(
        headers=_headers(),
        transport=transport,
        timeout=30,
        follow_redirects=True,
    ) as client:
        resp = await client.get(url)

    # Detect blocks
    if resp.status_code == 503:
        raise RuntimeError(f"Amazon returned 503 — proxy may be blocked")
    if resp.status_code == 200 and "captcha" in resp.text.lower() and len(resp.text) < 5000:
        raise RuntimeError("Amazon returned CAPTCHA — rotate proxy or slow down")

    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")

    # Check for no-results page
    no_results = soup.select_one(".s-no-outline")
    items = soup.select('[data-component-type="s-search-result"][data-asin]')

    results = []
    for item in items:
        product = _parse_product(item)
        if product:
            results.append(product)

    logger.info(f"[Scraper] '{query}' page {page}: {len(results)} results from {len(items)} items")
    return results
