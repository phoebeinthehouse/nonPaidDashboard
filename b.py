"""
Content Tracker – Daily Scraper
================================
Reads TikTok + Instagram video URLs from the organic content Google Sheet,
scrapes engagement metrics daily, and appends results to content_tracking.csv.

Requirements:
    pip install gspread google-auth playwright pandas requests
    playwright install chromium

Usage:
    python content_scraper.py          # run daily scrape
    python content_scraper.py --dry-run  # just list URLs without scraping

HOW TO GET YOUR INSTAGRAM SESSION ID:
1. Open Chrome and go to instagram.com (make sure you're logged in)
2. Press F12 (DevTools) → Application tab → Cookies → https://www.instagram.com
3. Find "sessionid" and copy its value
4. Paste it below as INSTAGRAM_SESSION_ID

Schedule (cron):
    0 9 * * * cd ~/Desktop/content-tracker && /usr/bin/python3 content_scraper.py >> logs/cron.log 2>&1
"""

import argparse
import csv
import json
import logging
import random
import re
import sys
import time
from datetime import date
from pathlib import Path

import requests
import gspread
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ─── CONFIG ──────────────────────────────────────────────────────────────────
CREDENTIALS_FILE  = "credentials.json"
SPREADSHEET_ID    = "1mqtqPdEO0WqVWTFd6f2nQzCLp-KjWcrziyaOUDl0lGY"
SHEET_TAB         = "오가닉 + 무가시딩 트레킹"
TRACKING_CSV      = "content_tracking.csv"
LOG_FILE          = "content_scraper.log"

# ── PASTE YOUR INSTAGRAM SESSION ID HERE ──────────────────────────────────────
# How to get it:
# 1. Open Chrome → instagram.com (logged in)
# 2. F12 → Application → Cookies → https://www.instagram.com
# 3. Find "sessionid" → copy the Value
INSTAGRAM_SESSION_ID = "YOUR_SESSION_ID_HERE"
# ─────────────────────────────────────────────────────────────────────────────

COL_NUMBER    = 1
COL_YEAR      = 2
COL_MONTH     = 3
COL_TYPE      = 4
COL_HANDLE    = 5
COL_FOLLOWERS = 6
COL_TIER      = 7
COL_DATE      = 8
COL_PRODUCT   = 9
COL_CHANNEL   = 10
COL_URL       = 11
COL_VIEWS     = 12
COL_LIKES     = 13
COL_COMMENTS  = 14
COL_SHARES    = 15

DATA_START_ROW = 5
MIN_DELAY      = 1.5
MAX_DELAY      = 3.5

# ─── LOGGING ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ─── 1. READ MOTHER SHEET ────────────────────────────────────────────────────
def read_urls_from_sheet() -> list[dict]:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds  = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    sheet  = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_TAB)
    all_rows = sheet.get_all_values()

    records = []
    for row in all_rows[DATA_START_ROW - 1:]:
        if len(row) <= COL_URL:
            continue
        url     = row[COL_URL].strip() if len(row) > COL_URL else ""
        channel = row[COL_CHANNEL].strip() if len(row) > COL_CHANNEL else ""

        if not url or ("tiktok.com" not in url and "instagram.com" not in url):
            continue

        def get_override(col):
            try:
                val = row[col].strip().replace(",", "") if len(row) > col else ""
                return int(float(val)) if val else None
            except (ValueError, IndexError):
                return None

        raw_followers = row[COL_FOLLOWERS].strip() if len(row) > COL_FOLLOWERS else ""

        records.append({
            "content_type":    row[COL_TYPE]    if len(row) > COL_TYPE    else "",
            "handle":          row[COL_HANDLE]  if len(row) > COL_HANDLE  else "",
            "sheet_followers": raw_followers,
            "tier":            row[COL_TIER]    if len(row) > COL_TIER    else "",
            "uploaded_date":   row[COL_DATE]    if len(row) > COL_DATE    else "",
            "product":         row[COL_PRODUCT] if len(row) > COL_PRODUCT else "",
            "channel":         channel,
            "url":             url,
            "manual_views":    get_override(COL_VIEWS),
            "manual_likes":    get_override(COL_LIKES),
            "manual_comments": get_override(COL_COMMENTS),
            "manual_shares":   get_override(COL_SHARES),
        })

    log.info(f"Loaded {len(records)} URLs from Google Sheet ({SHEET_TAB})")
    return records


# ─── 2. SCRAPING ─────────────────────────────────────────────────────────────
def parse_num(text: str) -> int:
    text = str(text).strip().upper().replace(",", "")
    try:
        if "M" in text:
            return int(float(text.replace("M", "")) * 1_000_000)
        if "K" in text:
            return int(float(text.replace("K", "")) * 1_000)
        return int(float(text))
    except (ValueError, AttributeError):
        return 0


def _extract_tiktok_json(json_text: str) -> dict | None:
    """Parse __NEXT_DATA__ or SIGI_STATE and return stats if views > 0."""
    try:
        data = json.loads(json_text)

        # Path 1: __NEXT_DATA__
        try:
            video_data   = data["props"]["pageProps"]["itemInfo"]["itemStruct"]
            stats        = video_data.get("stats", {})
            author_stats = video_data.get("authorStats", {})
            views        = stats.get("playCount", 0)
            if views and views > 0:
                return {
                    "views":     views,
                    "likes":     stats.get("diggCount", 0),
                    "comments":  stats.get("commentCount", 0),
                    "shares":    stats.get("shareCount", 0),
                    "saves":     stats.get("collectCount", 0),
                    "followers": author_stats.get("followerCount", None),
                    "method":    "tiktok_json",
                }
        except (KeyError, TypeError):
            pass

        # Path 2: SIGI_STATE ItemModule
        item_module = data.get("ItemModule", {})
        if item_module:
            first_item = next(iter(item_module.values()), None)
            if first_item:
                stats        = first_item.get("stats", {})
                author_stats = first_item.get("authorStats", {})
                views        = stats.get("playCount", 0)
                if views and views > 0:
                    return {
                        "views":     views,
                        "likes":     stats.get("diggCount", 0),
                        "comments":  stats.get("commentCount", 0),
                        "shares":    stats.get("shareCount", 0),
                        "saves":     stats.get("collectCount", 0),
                        "followers": author_stats.get("followerCount", None),
                        "method":    "tiktok_sigi",
                    }
    except (json.JSONDecodeError, Exception):
        pass
    return None


def _extract_tiktok_page_data(page) -> dict | None:
    """Try all JSON sources on the page."""
    # Try __NEXT_DATA__
    for el_id in ["__NEXT_DATA__", "SIGI_STATE"]:
        json_text = page.evaluate(f"""
            () => {{
                const el = document.getElementById('{el_id}');
                return el ? el.textContent : null;
            }}
        """)
        if json_text:
            result = _extract_tiktok_json(json_text)
            if result:
                return result

    # Try all script tags for any JSON containing playCount
    json_text = page.evaluate("""
        () => {
            const scripts = document.querySelectorAll('script');
            for (const s of scripts) {
                const text = s.textContent || '';
                if (text.includes('playCount') && text.includes('diggCount')) {
                    // Find JSON object containing playCount
                    const match = text.match(/[{][^{}]*"playCount"\s*:\s*\d+[^{}]*[}]/);
                    if (match) return match[0];
                }
            }
            return null;
        }
    """)
    if json_text:
        try:
            data  = json.loads(json_text)
            views = data.get("playCount", 0)
            if views > 0:
                return {
                    "views":     views,
                    "likes":     data.get("diggCount", 0),
                    "comments":  data.get("commentCount", 0),
                    "shares":    data.get("shareCount", 0),
                    "saves":     data.get("collectCount", 0),
                    "followers": None,
                    "method":    "tiktok_script",
                }
        except Exception:
            pass

    return None


def _extract_tiktok_css(page) -> dict:
    """Wait for view count element then extract via CSS selectors."""
    def get_num(selector):
        try:
            el = page.query_selector(selector)
            return parse_num(el.inner_text()) if el else 0
        except Exception:
            return 0

    # Wait up to 8 seconds for view count to appear
    try:
        page.wait_for_selector('[data-e2e="video-views"]', timeout=8_000)
    except Exception:
        pass

    views    = get_num('[data-e2e="video-views"]')
    likes    = get_num('[data-e2e="like-count"]')
    comments = get_num('[data-e2e="comment-count"]')
    shares   = get_num('[data-e2e="share-count"]')

    # Try alternative selectors if still zero
    if views == 0:
        for sel in ['strong[data-e2e="video-views"]', 'span[data-e2e="video-views"]',
                    '[class*="video-count"]', '[class*="view-count"]']:
            views = get_num(sel)
            if views > 0:
                break

    return {
        "views":     views,
        "likes":     likes,
        "comments":  comments,
        "shares":    shares,
        "saves":     0,
        "followers": None,
        "method":    "tiktok_css",
    }


def scrape_tiktok(page, url: str) -> dict | None:
    try:
        page.goto(url, wait_until="networkidle", timeout=40_000)
        time.sleep(random.uniform(1.5, 2.5))

        # Try JSON sources first (most reliable)
        result = _extract_tiktok_page_data(page)
        if result:
            return result

        # Try CSS with wait
        css_result = _extract_tiktok_css(page)
        if css_result["views"] > 0:
            return css_result

        # Scroll down to trigger lazy loading then retry
        page.evaluate("window.scrollBy(0, 300)")
        time.sleep(random.uniform(1.5, 2.5))

        result2 = _extract_tiktok_page_data(page)
        if result2:
            return result2

        css_result2 = _extract_tiktok_css(page)
        if css_result2["views"] > 0:
            css_result2["method"] = "tiktok_css_scroll"
            return css_result2

        # Full reload as last resort
        log.warning(f"Zero views after scroll, reloading: {url}")
        page.reload(wait_until="networkidle", timeout=40_000)
        time.sleep(random.uniform(3, 5))

        result3 = _extract_tiktok_page_data(page)
        if result3:
            result3["method"] += "_reload"
            return result3

        css_result3 = _extract_tiktok_css(page)
        css_result3["method"] = "tiktok_css_reload"
        return css_result3

    except PWTimeout:
        log.warning(f"Timeout: {url}")
    except Exception as e:
        log.warning(f"TikTok error {url}: {e}")
    return None


def inject_instagram_cookies(context):
    """Inject Instagram session cookies so we scrape as logged-in user."""
    if not INSTAGRAM_SESSION_ID or INSTAGRAM_SESSION_ID == "YOUR_SESSION_ID_HERE":
        log.warning("No Instagram session ID set — scraping without login")
        return

    context.add_cookies([
        {
            "name":   "sessionid",
            "value":  INSTAGRAM_SESSION_ID,
            "domain": ".instagram.com",
            "path":   "/",
            "secure": True,
            "httpOnly": True,
            "sameSite": "Lax",
        },
    ])
    log.info("Instagram session cookie injected ✅")


def scrape_instagram(page, url: str) -> dict | None:
    try:
        page.goto(url, wait_until="networkidle", timeout=40_000)
        time.sleep(random.uniform(2, 4))

        views    = 0
        likes    = 0
        comments = 0
        saves    = 0
        scraped_followers = None

        # ── Method 1: Extract from all script tags (works logged in) ──
        all_scripts = page.evaluate("""
            () => {
                const results = [];
                document.querySelectorAll('script').forEach(s => {
                    const t = s.textContent || '';
                    if (t.includes('like_count') || t.includes('play_count') ||
                        t.includes('video_view_count') || t.includes('edge_media_preview_like')) {
                        results.push(t);
                    }
                });
                return results;
            }
        """)

        for script in (all_scripts or []):
            if views == 0:
                for pattern in [r'"video_view_count"\s*:\s*(\d+)', r'"play_count"\s*:\s*(\d+)',
                                 r'"view_count"\s*:\s*(\d+)']:
                    m = re.search(pattern, script)
                    if m:
                        views = int(m.group(1))
                        break

            if likes == 0:
                for pattern in [r'"like_count"\s*:\s*(\d+)',
                                 r'"edge_media_preview_like"[^}]*"count"\s*:\s*(\d+)',
                                 r'"edge_liked_by"[^}]*"count"\s*:\s*(\d+)']:
                    m = re.search(pattern, script)
                    if m:
                        likes = int(m.group(1))
                        break

            if comments == 0:
                for pattern in [r'"comment_count"\s*:\s*(\d+)',
                                 r'"edge_media_to_comment"[^}]*"count"\s*:\s*(\d+)']:
                    m = re.search(pattern, script)
                    if m:
                        comments = int(m.group(1))
                        break

            if saves == 0:
                m = re.search(r'"saved_count"\s*:\s*(\d+)', script)
                if m:
                    saves = int(m.group(1))

            if views > 0 and likes > 0:
                break

        # ── Method 2: JSON-LD fallback ──
        if views == 0 or likes == 0:
            json_ld = page.evaluate("""
                () => {
                    const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                    for (const s of scripts) {
                        try {
                            const d = JSON.parse(s.textContent);
                            if (d.interactionStatistic) return JSON.stringify(d);
                        } catch(e) {}
                    }
                    return null;
                }
            """)
            if json_ld:
                try:
                    data = json.loads(json_ld)
                    for stat in data.get("interactionStatistic", []):
                        itype = stat.get("interactionType", "")
                        count = stat.get("userInteractionCount", 0)
                        if ("Watch" in itype or "View" in itype) and views == 0:
                            views = count
                        elif "Like" in itype and likes == 0:
                            likes = count
                        elif "Comment" in itype and comments == 0:
                            comments = count
                except (json.JSONDecodeError, KeyError):
                    pass

        # ── Method 3: Meta tags for views ──
        if views == 0:
            views_meta = page.evaluate("""
                () => {
                    const sels = [
                        'meta[property="og:video:view_count"]',
                        'meta[name="twitter:data1"]',
                    ];
                    for (const sel of sels) {
                        const el = document.querySelector(sel);
                        if (el) return el.getAttribute('content');
                    }
                    return null;
                }
            """)
            if views_meta:
                parsed = parse_num(views_meta)
                if parsed > 0:
                    views = parsed

        # ── Method 4: CSS selectors for likes (logged-in UI) ──
        if likes == 0:
            try:
                # Wait for likes section to render
                page.wait_for_selector('section', timeout=3_000)
                like_spans = page.query_selector_all('section span')
                for el in like_spans:
                    try:
                        text = el.inner_text().strip().replace(",", "")
                        if text and text.isdigit():
                            num = int(text)
                            if num > 0:
                                likes = num
                                break
                    except Exception:
                        pass
            except Exception:
                pass

        # ── Followers from meta description ──
        followers_meta = page.evaluate("""
            () => {
                const meta = document.querySelector('meta[name="description"]');
                return meta ? meta.getAttribute('content') : null;
            }
        """)
        if followers_meta:
            match = re.search(r'([\d,\.]+[KkMm]?)\s*Followers', followers_meta, re.IGNORECASE)
            if match:
                scraped_followers = parse_num(match.group(1))

        method = "instagram_loggedin" if (likes > 0 or comments > 0) else "instagram_meta"

        return {
            "views":     views,
            "likes":     likes,
            "comments":  comments,
            "shares":    0,
            "saves":     saves,
            "followers": scraped_followers,
            "method":    method,
        }

    except PWTimeout:
        log.warning(f"Timeout: {url}")
    except Exception as e:
        log.warning(f"Instagram error {url}: {e}")
    return None


def scrape_video(page, rec: dict) -> dict:
    channel = rec["channel"].lower()

    if "tiktok" in channel or "tiktok.com" in rec["url"]:
        metrics = scrape_tiktok(page, rec["url"])
    elif "instagram" in channel or "instagram.com" in rec["url"]:
        metrics = scrape_instagram(page, rec["url"])
    else:
        metrics = None

    if metrics is None:
        metrics = {
            "views": None, "likes": None, "comments": None,
            "shares": None, "saves": None, "followers": None,
            "method": "failed",
        }

    # Manual overrides take priority
    if rec["manual_views"]    is not None: metrics["views"]    = rec["manual_views"]
    if rec["manual_likes"]    is not None: metrics["likes"]    = rec["manual_likes"]
    if rec["manual_comments"] is not None: metrics["comments"] = rec["manual_comments"]
    if rec["manual_shares"]   is not None: metrics["shares"]   = rec["manual_shares"]

    return metrics


# ─── 3. RESOLVE FOLLOWERS ────────────────────────────────────────────────────
def resolve_followers(scraped: int | None, sheet_value: str) -> int:
    if scraped is not None and scraped > 0:
        return scraped
    try:
        val = int(float(str(sheet_value).replace(",", "")))
        if 0 < val < 1000:
            return val * 1000
        return val
    except (ValueError, TypeError):
        return 0


# ─── 4. ENGAGEMENT RATE ──────────────────────────────────────────────────────
def calc_er(likes, comments, shares, saves, views) -> float:
    try:
        views = int(views) if views else 0
        if not views:
            return 0.0
        return round((int(likes or 0) + int(comments or 0) + int(shares or 0) + int(saves or 0)) / views * 100, 4)
    except (ValueError, TypeError):
        return 0.0


# ─── 5. WRITE TO CSV ─────────────────────────────────────────────────────────
FIELDNAMES = [
    "date", "url", "channel", "content_type", "handle", "followers", "tier",
    "product", "uploaded_date", "views", "likes", "comments", "shares", "saves",
    "engagement_rate", "delta_views", "delta_likes", "delta_comments", "delta_er",
    "trend", "scrape_method",
]

def load_yesterday_map() -> dict:
    if not Path(TRACKING_CSV).exists():
        return {}

    rows_by_date = {}
    with open(TRACKING_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            d = row.get("date", "")
            if d not in rows_by_date:
                rows_by_date[d] = []
            rows_by_date[d].append(row)

    if not rows_by_date:
        return {}

    latest_date = sorted(rows_by_date.keys())[-1]
    yesterday_map = {}
    for row in rows_by_date[latest_date]:
        yesterday_map[row["url"]] = {
            "views":    int(float(row["views"] or 0)),
            "likes":    int(float(row["likes"] or 0)),
            "comments": int(float(row["comments"] or 0)),
            "er":       float(row["engagement_rate"] or 0),
        }

    log.info(f"Loaded {len(yesterday_map)} entries from previous scrape ({latest_date})")
    return yesterday_map


def calc_deltas(today: dict, yesterday: dict | None) -> dict:
    if yesterday is None:
        return {
            "delta_views":    None,
            "delta_likes":    None,
            "delta_comments": None,
            "delta_er":       None,
            "trend":          "–",
        }

    delta_views    = int(today.get("views") or 0)    - yesterday["views"]
    delta_likes    = int(today.get("likes") or 0)    - yesterday["likes"]
    delta_comments = int(today.get("comments") or 0) - yesterday["comments"]
    delta_er       = round(float(today.get("engagement_rate") or 0) - yesterday["er"], 4)

    if delta_views > 0:
        trend = "📈 Growing"
    elif delta_views < 0:
        trend = "📉 Dropping"
    else:
        trend = "➡ Stable"

    return {
        "delta_views":    delta_views,
        "delta_likes":    delta_likes,
        "delta_comments": delta_comments,
        "delta_er":       delta_er,
        "trend":          trend,
    }


def append_to_csv(rows: list[dict]):
    write_header = not Path(TRACKING_CSV).exists()
    with open(TRACKING_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(rows)
    log.info(f"Appended {len(rows)} rows → {TRACKING_CSV}")


# ─── 6. MAIN ─────────────────────────────────────────────────────────────────
def run(dry_run: bool = False):
    today         = date.today().isoformat()
    records       = read_urls_from_sheet()
    yesterday_map = load_yesterday_map()

    if dry_run:
        log.info("DRY RUN – first 5 URLs:")
        for r in records[:5]:
            log.info(f"  [{r['channel']}] [{r['content_type']}] {r['url']}")
        return

    results = []
    rec_map = {r["url"]: r for r in records}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox"],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            viewport={"width": 1280, "height": 800},
        )

        # Inject Instagram session cookies
        inject_instagram_cookies(context)

        page = context.new_page()

        # ── MAIN PASS ──
        for i, rec in enumerate(records):
            log.info(f"[{i+1}/{len(records)}] [{rec['channel']}] [{rec['content_type']}] {rec['handle']}")
            metrics = scrape_video(page, rec)

            followers = resolve_followers(
                metrics.get("followers"),
                rec["sheet_followers"],
            )

            er = calc_er(
                metrics.get("likes"),
                metrics.get("comments"),
                metrics.get("shares"),
                metrics.get("saves"),
                metrics.get("views"),
            )

            today_data = {
                "views":           metrics.get("views"),
                "likes":           metrics.get("likes"),
                "comments":        metrics.get("comments"),
                "engagement_rate": er,
            }

            prev   = yesterday_map.get(rec["url"])
            deltas = calc_deltas(today_data, prev)

            results.append({
                "date":            today,
                "url":             rec["url"],
                "channel":         rec["channel"],
                "content_type":    rec["content_type"],
                "handle":          rec["handle"],
                "followers":       followers,
                "tier":            rec["tier"],
                "product":         rec["product"],
                "uploaded_date":   rec["uploaded_date"],
                "views":           metrics.get("views"),
                "likes":           metrics.get("likes"),
                "comments":        metrics.get("comments"),
                "shares":          metrics.get("shares"),
                "saves":           metrics.get("saves"),
                "engagement_rate": er,
                "delta_views":     deltas["delta_views"],
                "delta_likes":     deltas["delta_likes"],
                "delta_comments":  deltas["delta_comments"],
                "delta_er":        deltas["delta_er"],
                "trend":           deltas["trend"],
                "scrape_method":   metrics.get("method", "unknown"),
            })

            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

        # ── RETRY PASS — up to 3 attempts for zeros ──
        MAX_RETRIES = 3
        for attempt in range(1, MAX_RETRIES + 1):
            zeros = [
                r for r in results
                if (not r["views"] or r["views"] == 0)
                or (not r["likes"] or r["likes"] == 0)
            ]

            if not zeros:
                log.info("✅ No zero-view/like rows remaining, skipping retry.")
                break

            log.info(f"🔄 Retry pass {attempt}/{MAX_RETRIES} — {len(zeros)} videos with 0 views or likes")
            time.sleep(random.uniform(5, 10))

            for result in zeros:
                url = result["url"]
                rec = rec_map.get(url)
                if not rec:
                    continue

                log.info(f"  Retrying [{rec['channel']}] {rec['handle']} — views={result['views']}, likes={result['likes']}")
                metrics = scrape_video(page, rec)

                new_views = metrics.get("views") or 0
                new_likes = metrics.get("likes") or 0

                if new_views > 0 or new_likes > 0:
                    followers = resolve_followers(
                        metrics.get("followers"),
                        rec["sheet_followers"],
                    )
                    er = calc_er(
                        metrics.get("likes"),
                        metrics.get("comments"),
                        metrics.get("shares"),
                        metrics.get("saves"),
                        metrics.get("views"),
                    )
                    today_data = {
                        "views":           new_views,
                        "likes":           new_likes,
                        "comments":        metrics.get("comments"),
                        "engagement_rate": er,
                    }
                    prev   = yesterday_map.get(url)
                    deltas = calc_deltas(today_data, prev)

                    result.update({
                        "views":           new_views,
                        "likes":           new_likes,
                        "comments":        metrics.get("comments"),
                        "shares":          metrics.get("shares"),
                        "saves":           metrics.get("saves"),
                        "followers":       followers,
                        "engagement_rate": er,
                        "delta_views":     deltas["delta_views"],
                        "delta_likes":     deltas["delta_likes"],
                        "delta_comments":  deltas["delta_comments"],
                        "delta_er":        deltas["delta_er"],
                        "trend":           deltas["trend"],
                        "scrape_method":   metrics.get("method", "unknown") + f"_retry{attempt}",
                    })
                    log.info(f"    ✅ Got views={new_views}, likes={new_likes}")
                else:
                    log.info(f"    ❌ Still zero after retry {attempt}")

                time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

        page.close()
        context.close()
        browser.close()

    append_to_csv(results)

    # Summary
    zero_views = sum(1 for r in results if not r["views"])
    zero_likes = sum(1 for r in results if not r["likes"])
    log.info(f"✅ Done! {len(results)} videos scraped for {today}.")
    log.info(f"   Still zero views: {zero_views} | Still zero likes: {zero_likes}")

    for ctype in ["오가닉", "무가씨딩"]:
        subset = [r for r in results if r["content_type"] == ctype]
        if subset:
            avg_er  = round(sum(r["engagement_rate"] for r in subset) / len(subset), 2)
            growing = sum(1 for r in subset if r["trend"] == "📈 Growing")
            log.info(f"  {ctype}: {len(subset)} videos | Avg ER: {avg_er}% | 📈 Growing: {growing}")


# ─── CLI ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Content Tracker Daily Scraper")
    parser.add_argument("--dry-run", action="store_true", help="List URLs without scraping")
    args = parser.parse_args()
    run(args.dry_run)
