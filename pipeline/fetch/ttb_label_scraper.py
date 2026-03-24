#!/usr/bin/env python3
"""
TTB COLA Label Image Scraper + Detail Extractor

Fetches label images (front, back, strip) AND extracts structured data
(grape varietals, appellation, vintage, ABV, applicant info) from TTB's
Public COLA Registry printable version pages using Playwright to bypass
F5 WAF/Shape Security.

Images are intercepted via route handlers as the browser naturally loads
them from <img> tags on the printable page. Extracted fields are batched
and written back to source_ttb_colas.

Scope: 2005+, approved, core wine types (80/81/80A/84/88) — ~2M records.

Usage:
    python -m pipeline.fetch.ttb_label_scraper [options]

    --output-dir PATH    Where to save images (default: ~/Desktop/Loam Cowork/data/imports/ttb_cola_labels)
    --workers N          Concurrent browser pages (default: 10)
    --rate-limit N       Max pages/sec (default: 5)
    --limit N            Process only N records (for testing)
    --start-year YYYY    Start from this year (default: 2005)
    --refresh-list       Re-fetch record list from DB
    --no-images          Skip image download, extract data only
"""

import argparse
import asyncio
import html as html_lib
import json
import os
import re
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import unquote

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase

# ─── Constants ───────────────────────────────────────────────────────────────

TTB_PRINTABLE_URL = "https://ttbonline.gov/colasonline/viewColaDetails.do?action=publicFormDisplay&ttbid={ttb_id}"
TTB_BASE_URL = "https://ttbonline.gov"
# Regex matches image tags in the browser-rendered HTML
IMG_TAG_REGEX = re.compile(
    r'<img\s[^>]*src="(/colasonline/publicViewAttachment\.do\?filename=[^"]+(?:&amp;|&)filetype=l)"[^>]*>',
    re.IGNORECASE | re.DOTALL,
)
ALT_ATTR_REGEX = re.compile(r'alt="([^"]*)"', re.IGNORECASE)

DEFAULT_OUTPUT = Path.home() / "Desktop" / "Loam Cowork" / "data" / "imports" / "ttb_cola_labels"

IMAGE_TYPE_MAP = {
    "brand (front) or keg collar": "front",
    "front": "front",
    "back": "back",
    "strip": "strip",
    "neck": "neck",
}

# DB update batch size
DB_BATCH_SIZE = 50


# ─── Field Extraction ────────────────────────────────────────────────────────

def extract_field(html: str, label_pattern: str) -> str | None:
    """Extract text from <div class="data"> following a label div matching pattern."""
    # Match: <div class="label">...pattern...</div> ... <div class="data">VALUE</div>
    # Also handles <div class="boldlabel">
    pattern = re.compile(
        r'<div\s+class="(?:bold)?label">.*?' + label_pattern + r'.*?</div>'
        r'\s*'
        r'(?:<br\s*/?>|\s)*'
        r'<div\s+class="data">\s*(.*?)\s*</div>',
        re.IGNORECASE | re.DOTALL,
    )
    m = pattern.search(html)
    if not m:
        return None
    raw = m.group(1)
    # Strip tags
    text = re.sub(r'<[^>]+>', ' ', raw)
    # Decode HTML entities
    text = html_lib.unescape(text)
    # Collapse whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    # Return None for empty/nbsp
    if not text or text == '\xa0' or text == '&nbsp;':
        return None
    return text


def extract_applicant(html: str) -> dict:
    """Extract applicant name and address from the printable page."""
    result = {
        "applicant_name": None,
        "applicant_address": None,
        "applicant_city": None,
        "applicant_state": None,
        "applicant_zip": None,
    }

    # Find the applicant data block
    pattern = re.compile(
        r'NAME AND ADDRESS OF APPLICANT.*?<div\s+class="data">\s*(.*?)\s*</div>',
        re.IGNORECASE | re.DOTALL,
    )
    m = pattern.search(html)
    if not m:
        return result

    raw = m.group(1)
    # Split on <br> tags
    parts = re.split(r'<br\s*/?>', raw)
    parts = [html_lib.unescape(re.sub(r'<[^>]+>', '', p)).strip() for p in parts]
    parts = [p for p in parts if p]

    if not parts:
        return result

    # First non-empty part is company name
    result["applicant_name"] = parts[0] if parts else None

    # Address line(s) — everything between name and city/state/zip
    # Last line with a state abbreviation + zip is the city/state/zip line
    city_state_zip_re = re.compile(r'^(.+?)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$')
    address_lines = []
    for i, p in enumerate(parts[1:], 1):
        csz = city_state_zip_re.match(p)
        if csz:
            result["applicant_city"] = csz.group(1).strip().rstrip(',')
            result["applicant_state"] = csz.group(2)
            result["applicant_zip"] = csz.group(3)
            break
        elif p:
            address_lines.append(p)

    if address_lines:
        result["applicant_address"] = ", ".join(address_lines)

    return result


def extract_all_fields(html: str) -> dict:
    """Extract all structured fields from a TTB printable page."""
    data = {}

    # Grape varietals — field 10 (new form) or 13 (old form)
    data["grape_varietals"] = extract_field(html, r'GRAPE VARIETAL')

    # Wine appellation — field 11 (new) or 13 (old)
    data["wine_appellation"] = (
        extract_field(html, r'WINE APPELLATION')
        or extract_field(html, r'APPELLATION')
    )

    # Vintage — field 14 (old form has "WINE VINTAGE DATE IF ON LABEL")
    data["wine_vintage"] = (
        extract_field(html, r'WINE VINTAGE DATE')
        or extract_field(html, r'VINTAGE DATE')
    )

    # ABV / Alcohol content
    data["abv"] = extract_field(html, r'ALCOHOL CONTENT')

    # Phone
    data["phone"] = extract_field(html, r'PHONE NUMBER')

    # Email
    data["email"] = extract_field(html, r'EMAIL')

    # Qualifications
    data["qualifications"] = extract_field(html, r'QUALIFICATIONS')

    # Applicant info
    applicant = extract_applicant(html)
    data.update(applicant)

    # Clean up: remove None values and empty strings
    return {k: v for k, v in data.items() if v}


# ─── Rate Limiter ────────────────────────────────────────────────────────────

class RateLimiter:
    """Token bucket rate limiter for async operations."""

    def __init__(self, rate: float, burst: int = 10):
        self.rate = rate
        self.tokens = float(burst)
        self.max_tokens = float(burst)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_refill
                self.tokens = min(self.max_tokens, self.tokens + elapsed * self.rate)
                self._last_refill = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
            await asyncio.sleep(0.02)


# ─── Stats ───────────────────────────────────────────────────────────────────

@dataclass
class Stats:
    """Scraper statistics — updated by workers, read by dashboard."""
    total_records: int = 0
    completed: int = 0
    no_images: int = 0
    errors: int = 0
    retries: int = 0
    timeouts: int = 0

    images_front: int = 0
    images_back: int = 0
    images_strip: int = 0
    images_other: int = 0

    bytes_downloaded: int = 0
    largest_image: int = 0
    image_dl_failures: int = 0  # images lost to ECONNRESET after retries
    waf_blocks: int = 0  # pages that returned WAF challenge instead of content

    # Data extraction stats
    fields_extracted: int = 0  # records with at least one field
    grapes_found: int = 0
    appellations_found: int = 0
    vintages_found: int = 0

    # Throughput tracking
    start_time: float = 0.0
    _recent_times: deque = field(default_factory=lambda: deque(maxlen=200))
    _throughput_history: deque = field(default_factory=lambda: deque(maxlen=60))
    _last_throughput_update: float = 0.0
    peak_throughput: float = 0.0

    # Latency tracking
    _recent_latencies: deque = field(default_factory=lambda: deque(maxlen=500))

    # Recent activity
    recent: deque = field(default_factory=lambda: deque(maxlen=8))

    # Era tracking
    era_totals: dict = field(default_factory=dict)
    era_done: dict = field(default_factory=dict)

    @property
    def total_images(self) -> int:
        return self.images_front + self.images_back + self.images_strip + self.images_other

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.start_time if self.start_time else 0

    @property
    def avg_throughput(self) -> float:
        if self.elapsed < 1:
            return 0
        return self.completed / self.elapsed

    @property
    def current_throughput(self) -> float:
        now = time.monotonic()
        cutoff = now - 10  # last 10 seconds
        recent = [t for t in self._recent_times if t > cutoff]
        if not recent:
            return 0
        span = now - recent[0]
        return len(recent) / span if span > 0.1 else 0

    @property
    def avg_latency_ms(self) -> float:
        if not self._recent_latencies:
            return 0
        return sum(self._recent_latencies) / len(self._recent_latencies) * 1000

    @property
    def avg_image_size_kb(self) -> float:
        if self.total_images == 0:
            return 0
        return (self.bytes_downloaded / self.total_images) / 1024

    @property
    def eta_seconds(self) -> float:
        rate = self.avg_throughput
        if rate <= 0:
            return 0
        remaining = self.total_records - self.completed
        return remaining / rate

    def record_completion(self, ttb_id: str, brand: str, n_images: int, duration: float):
        now = time.monotonic()
        self._recent_times.append(now)
        self._recent_latencies.append(duration)
        self.completed += 1

        # Update throughput history every second
        if now - self._last_throughput_update >= 1.0:
            ct = self.current_throughput
            self._throughput_history.append(ct)
            if ct > self.peak_throughput:
                self.peak_throughput = ct
            self._last_throughput_update = now

        # Recent activity
        self.recent.appendleft({
            "ttb_id": ttb_id,
            "brand": (brand or "—")[:35],
            "images": n_images,
            "time": f"{duration:.1f}s",
        })

    def sparkline(self) -> str:
        if not self._throughput_history:
            return ""
        bars = " ▁▂▃▄▅▆▇█"
        vals = list(self._throughput_history)
        if not vals:
            return ""
        mx = max(vals) if max(vals) > 0 else 1
        return "".join(bars[min(8, int(v / mx * 8))] for v in vals[-40:])

    def save(self, path: Path):
        """Save stats snapshot to JSON."""
        path.write_text(json.dumps({
            "completed": self.completed,
            "total": self.total_records,
            "no_images": self.no_images,
            "errors": self.errors,
            "images": self.total_images,
            "bytes": self.bytes_downloaded,
            "elapsed_s": int(self.elapsed),
            "avg_throughput": round(self.avg_throughput, 1),
            "peak_throughput": round(self.peak_throughput, 1),
            "fields_extracted": self.fields_extracted,
            "grapes_found": self.grapes_found,
            "appellations_found": self.appellations_found,
            "vintages_found": self.vintages_found,
        }, indent=2))


# ─── Dashboard ───────────────────────────────────────────────────────────────

def format_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    elif n < 1024 ** 2:
        return f"{n / 1024:.1f} KB"
    elif n < 1024 ** 3:
        return f"{n / (1024**2):.1f} MB"
    else:
        return f"{n / (1024**3):.2f} GB"


def format_duration(s: float) -> str:
    if s <= 0:
        return "—"
    h = int(s // 3600)
    m = int((s % 3600) // 60)
    if h > 0:
        return f"{h}h {m:02d}m"
    return f"{m}m"


def build_dashboard(stats: Stats, workers: int) -> Panel:
    """Build the Rich dashboard panel."""

    # ── Main progress ──
    pct = (stats.completed / stats.total_records * 100) if stats.total_records else 0
    bar_width = 50
    filled = int(bar_width * pct / 100)
    bar = f"[green]{'█' * filled}[/green][dim]{'░' * (bar_width - filled)}[/dim]"
    progress_text = (
        f"  {bar}  {pct:5.1f}%  {stats.completed:,}\n"
        f"  of {stats.total_records:,} records"
        f"{'':>20}ETA: {format_duration(stats.eta_seconds)}"
    )

    # ── Throughput panel ──
    tp = Table.grid(padding=(0, 1))
    tp.add_row("[bold]Current:[/bold]", f"{stats.current_throughput:>6.1f} rec/s")
    tp.add_row("[bold]Average:[/bold]", f"{stats.avg_throughput:>6.1f} rec/s")
    tp.add_row("[bold]Peak:[/bold]", f"{stats.peak_throughput:>6.1f} rec/s")
    tp.add_row("", "")
    tp.add_row("", f"[dim]{stats.sparkline()}[/dim]")
    throughput_panel = Panel(tp, title="Throughput", border_style="blue", width=30)

    # ── Images panel ──
    ip = Table.grid(padding=(0, 1))
    ip.add_row("[bold]Front:[/bold]", f"{stats.images_front:>12,}")
    ip.add_row("[bold]Back:[/bold]", f"{stats.images_back:>12,}")
    ip.add_row("[bold]Strip:[/bold]", f"{stats.images_strip:>12,}")
    ip.add_row(f"[bold]Total:[/bold]", f"[green]{stats.total_images:>12,}[/green]")
    no_img_pct = (stats.no_images / stats.completed * 100) if stats.completed else 0
    ip.add_row("[bold]No image:[/bold]", f"{stats.no_images:>8,}  [dim]({no_img_pct:.1f}%)[/dim]")
    if stats.image_dl_failures > 0:
        ip.add_row("[bold red]Failed:[/bold red]", f"[red]{stats.image_dl_failures:>12,}[/red]")
    images_panel = Panel(ip, title="Images", border_style="green", width=34)

    # ── Data panel (replaces storage) ──
    dp = Table.grid(padding=(0, 1))
    dp.add_row("[bold]Grapes:[/bold]", f"{stats.grapes_found:>12,}")
    dp.add_row("[bold]Appellations:[/bold]", f"{stats.appellations_found:>12,}")
    dp.add_row("[bold]Vintages:[/bold]", f"{stats.vintages_found:>12,}")
    extract_pct = (stats.fields_extracted / stats.completed * 100) if stats.completed else 0
    dp.add_row("[bold]Has data:[/bold]", f"{stats.fields_extracted:>8,}  [dim]({extract_pct:.1f}%)[/dim]")
    data_panel = Panel(dp, title="Extracted", border_style="magenta", width=30)

    # ── Network + Storage panel ──
    np_table = Table.grid(padding=(0, 1))
    np_table.add_row("[bold]Avg latency:[/bold]", f"{stats.avg_latency_ms:>8.0f}ms")
    err_pct = (stats.errors / stats.completed * 100) if stats.completed else 0
    np_table.add_row("[bold]Errors:[/bold]", f"{stats.errors:>8,}  [dim]({err_pct:.3f}%)[/dim]")
    if stats.waf_blocks > 0:
        np_table.add_row("[bold red]WAF blocks:[/bold red]", f"[red]{stats.waf_blocks:>8,}[/red]")
    np_table.add_row("[bold]Retries:[/bold]", f"{stats.retries:>8,}")
    np_table.add_row("[bold]Storage:[/bold]", f"{format_bytes(stats.bytes_downloaded):>12}")
    network_panel = Panel(np_table, title="Network", border_style="red", width=34)

    # ── Era progress ──
    era_table = Table.grid(padding=(0, 1))
    for era_name in sorted(stats.era_totals.keys()):
        total = stats.era_totals[era_name]
        done = stats.era_done.get(era_name, 0)
        era_pct = (done / total * 100) if total else 0
        era_bar_w = 30
        era_filled = int(era_bar_w * era_pct / 100)
        bar_str = f"[green]{'█' * era_filled}[/green][dim]{'░' * (era_bar_w - era_filled)}[/dim]"
        check = " [green]✓[/green]" if era_pct >= 99.9 else ""
        era_table.add_row(
            f"[bold]{era_name}[/bold]",
            bar_str,
            f"{era_pct:>5.1f}%",
            f"{done:>9,}",
            check,
        )
    era_panel = Panel(era_table, title="By Era", border_style="cyan")

    # ── Recent activity ──
    recent_table = Table.grid(padding=(0, 2))
    for entry in list(stats.recent)[:6]:
        recent_table.add_row(
            f"[dim]{entry['ttb_id']}[/dim]",
            f"{entry['brand'][:30]}",
            f"[green]{entry['images']}img[/green]" if entry['images'] > 0 else "[red]0img[/red]",
            f"[dim]{entry['time']}[/dim]",
        )
    recent_panel = Panel(recent_table, title="Recent", border_style="dim")

    # ── Footer ──
    elapsed_str = format_duration(stats.elapsed)
    started = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time() - stats.elapsed))
    footer = f"  Started: {started}  |  Elapsed: {elapsed_str}  |  Workers: {workers}  |  Ctrl+C to stop (saves progress)"

    # ── Compose ──
    top_row = Table.grid(padding=(0, 2))
    top_row.add_row(throughput_panel, images_panel)

    mid_row = Table.grid(padding=(0, 2))
    mid_row.add_row(data_panel, network_panel)

    layout_table = Table.grid(padding=(1, 0))
    layout_table.add_row(Text(progress_text, style="bold"))
    layout_table.add_row(top_row)
    layout_table.add_row(mid_row)
    layout_table.add_row(era_panel)
    layout_table.add_row(recent_panel)
    layout_table.add_row(Text(footer, style="dim"))

    return Panel(layout_table, title="[bold white] TTB COLA Label Scraper [/bold white]", border_style="bold white")


# ─── Checkpoint ──────────────────────────────────────────────────────────────

class Checkpoint:
    """Track completed TTB IDs for resume support."""

    def __init__(self, path: Path):
        self.path = path
        self.completed: set[str] = set()
        self._file = None

    def load(self):
        if self.path.exists():
            with open(self.path, "r") as f:
                for line in f:
                    ttb_id = line.strip()
                    if ttb_id:
                        self.completed.add(ttb_id)
        self._file = open(self.path, "a", buffering=1)

    def add(self, ttb_id: str):
        self.completed.add(ttb_id)
        if self._file:
            self._file.write(ttb_id + "\n")

    def close(self):
        if self._file:
            self._file.flush()
            self._file.close()

    def __contains__(self, ttb_id: str) -> bool:
        return ttb_id in self.completed


# ─── DB Writer ───────────────────────────────────────────────────────────────

class DbWriter:
    """Batches extracted field updates to source_ttb_colas."""

    def __init__(self):
        self._buffer: list[dict] = []
        self._lock = asyncio.Lock()
        self._sb = None
        self.total_written = 0
        self.write_errors = 0

    def _get_sb(self):
        if self._sb is None:
            self._sb = get_supabase()
        return self._sb

    async def add(self, ttb_id: str, fields: dict, image_urls: list[str]):
        """Queue a record for DB update."""
        if not fields and not image_urls:
            return

        row = {"ttb_id": ttb_id, "detail_scraped_at": "now()"}
        row.update(fields)
        if image_urls:
            row["label_image_urls"] = image_urls

        async with self._lock:
            self._buffer.append(row)
            if len(self._buffer) >= DB_BATCH_SIZE:
                await self._flush()

    async def _flush(self):
        """Write buffered updates to DB."""
        if not self._buffer:
            return

        batch = self._buffer[:]
        self._buffer.clear()

        try:
            sb = self._get_sb()
            # Use upsert on ttb_id to update existing rows
            sb.table("source_ttb_colas").upsert(
                batch, on_conflict="ttb_id"
            ).execute()
            self.total_written += len(batch)
        except Exception as e:
            self.write_errors += 1
            # Retry one by one
            sb = self._get_sb()
            for row in batch:
                try:
                    sb.table("source_ttb_colas").upsert(
                        row, on_conflict="ttb_id"
                    ).execute()
                    self.total_written += 1
                except Exception:
                    self.write_errors += 1

    async def flush(self):
        """Force flush remaining buffer."""
        async with self._lock:
            await self._flush()


# ─── Image helpers ───────────────────────────────────────────────────────────

def classify_image_from_url(url: str) -> str | None:
    """Try to classify image type from the filename in the URL."""
    m = re.search(r'filename=([^&]+)', url)
    if not m:
        return None
    fname = unquote(m.group(1)).upper()
    if "FRONT" in fname:
        return "front"
    if "BACK" in fname:
        return "back"
    if "STRIP" in fname:
        return "strip"
    if "NECK" in fname:
        return "neck"
    return None


def classify_image(alt_text: str, url: str = "") -> str:
    """Classify image type from URL filename first, then alt text fallback."""
    if url:
        from_url = classify_image_from_url(url)
        if from_url:
            return from_url

    alt_lower = alt_text.lower().strip()
    for key, label in IMAGE_TYPE_MAP.items():
        if key in alt_lower:
            return label
    return "other"


def image_filename(ttb_id: str, img_type: str, index: int, content_type: str = "") -> str:
    """Generate filename for a label image."""
    ext = "jpg"
    if "png" in (content_type or ""):
        ext = "png"
    elif "gif" in (content_type or ""):
        ext = "gif"

    suffix = img_type
    if index > 1:
        suffix = f"{img_type}{index}"
    return f"{ttb_id}_{suffix}.{ext}"


# ─── Playwright Worker ───────────────────────────────────────────────────────

async def process_record_pw(
    page,
    rate_limiter: RateLimiter,
    ttb_id: str,
    brand_name: str,
    year_str: str,
    output_dir: Path,
    stats: Stats,
    checkpoint: Checkpoint,
    db_writer: DbWriter,
    era_name: str,
    skip_images: bool = False,
):
    """Load printable page, extract data fields, and capture label images.

    Optimized wait strategy:
    - domcontentloaded for HTML (0.2s) — TTB pages are server-rendered
    - If images needed AND HTML has <img> tags, wait for networkidle (~0.5s more)
    - If no images on page or --no-images, move on immediately
    """
    t0 = time.monotonic()
    n_images = 0
    captured_images: list[tuple[str, bytes, str]] = []  # (url, data, content_type)
    image_failures: list[str] = []  # URLs that failed all retries

    async def intercept_image(route):
        """Route handler that captures image responses with retry."""
        for attempt in range(3):
            try:
                resp = await route.fetch()
                body = await resp.body()
                ct = resp.headers.get("content-type", "")
                captured_images.append((route.request.url, body, ct))
                await route.fulfill(response=resp)
                return
            except Exception:
                if attempt < 2:
                    await asyncio.sleep(0.5 * (attempt + 1))  # 0.5s, 1s backoff
                    continue
                # All retries exhausted — track the failure
                image_failures.append(route.request.url)
                try:
                    await route.abort()
                except Exception:
                    pass

    try:
        await rate_limiter.acquire()

        # Set up route interception for label images BEFORE navigation
        if not skip_images:
            await page.route("**/publicViewAttachment.do*filetype=l*", intercept_image)

        url = TTB_PRINTABLE_URL.format(ttb_id=ttb_id)

        # Fast initial load — HTML is server-rendered, no JS needed for data
        resp = await page.goto(url, wait_until="domcontentloaded", timeout=30000)

        if not resp or resp.status != 200:
            stats.errors += 1
            stats.record_completion(ttb_id, brand_name, 0, time.monotonic() - t0)
            stats.era_done[era_name] = stats.era_done.get(era_name, 0) + 1
            if not skip_images:
                await page.unroute("**/publicViewAttachment.do*filetype=l*")
            return

        # Get page HTML immediately (data is already in the DOM)
        page_html = await page.content()

        # ── WAF detection: Shape Security re-challenges mid-run ──
        if "bobcmn" in page_html or 'class="data"' not in page_html:
            # WAF challenge page — wait for JS to solve it
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
                page_html = await page.content()
            except Exception:
                pass
            # Still WAF? Skip but do NOT checkpoint — retry on next run
            if "bobcmn" in page_html or 'class="data"' not in page_html:
                stats.waf_blocks += 1
                stats.errors += 1
                stats.record_completion(ttb_id, brand_name, 0, time.monotonic() - t0)
                stats.era_done[era_name] = stats.era_done.get(era_name, 0) + 1
                if not skip_images:
                    await page.unroute("**/publicViewAttachment.do*filetype=l*")
                return

        # ── Extract structured data ──
        extracted = extract_all_fields(page_html)
        if extracted:
            stats.fields_extracted += 1
            if "grape_varietals" in extracted:
                stats.grapes_found += 1
            if "wine_appellation" in extracted:
                stats.appellations_found += 1
            if "wine_vintage" in extracted:
                stats.vintages_found += 1

        # ── Process images (only wait if page has images) ──
        image_urls = []
        has_img_tags = not skip_images and "publicViewAttachment" in page_html

        if has_img_tags:
            # Images are still loading in background — wait for them
            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass  # timeout is fine, capture what we got

        # Remove route handler
        if not skip_images:
            await page.unroute("**/publicViewAttachment.do*filetype=l*")

        if has_img_tags and captured_images:
            # Build alt text map from HTML
            alt_map: dict[str, str] = {}
            for tag_match in IMG_TAG_REGEX.finditer(page_html):
                tag = tag_match.group(0)
                img_path = tag_match.group(1).replace("&amp;", "&")
                full_url = TTB_BASE_URL + img_path
                alt_match = ALT_ATTR_REGEX.search(tag)
                alt_text = alt_match.group(1) if alt_match else ""
                alt_map[full_url] = alt_text

            # Create year directory
            year_dir = output_dir / year_str
            year_dir.mkdir(parents=True, exist_ok=True)

            # Save captured images
            type_counts: dict[str, int] = {}
            for img_url, img_data, content_type in captured_images:
                image_urls.append(img_url)

                alt_text = alt_map.get(img_url, "")
                if not alt_text:
                    alt_text = alt_map.get(img_url.replace("&", "&amp;"), "")

                img_type = classify_image(alt_text, img_url)
                type_counts[img_type] = type_counts.get(img_type, 0) + 1
                count = type_counts[img_type]

                fname = image_filename(ttb_id, img_type, count, content_type)
                (year_dir / fname).write_bytes(img_data)

                img_size = len(img_data)
                stats.bytes_downloaded += img_size
                if img_size > stats.largest_image:
                    stats.largest_image = img_size
                n_images += 1

                if img_type == "front":
                    stats.images_front += 1
                elif img_type == "back":
                    stats.images_back += 1
                elif img_type == "strip":
                    stats.images_strip += 1
                else:
                    stats.images_other += 1

        if not skip_images and not has_img_tags:
            stats.no_images += 1

        # ── Track image failures for re-scrape ──
        if image_failures:
            stats.image_dl_failures = getattr(stats, 'image_dl_failures', 0) + len(image_failures)
            # Log to file so we can re-scrape these later
            fail_path = output_dir / "image_failures.jsonl"
            try:
                with open(fail_path, "a") as ff:
                    ff.write(json.dumps({"ttb_id": ttb_id, "failed_urls": image_failures}) + "\n")
            except Exception:
                pass

        # ── Queue DB update ──
        await db_writer.add(ttb_id, extracted, image_urls)

        # Only checkpoint on SUCCESS (data written to DB)
        duration = time.monotonic() - t0
        stats.record_completion(ttb_id, brand_name, n_images, duration)
        stats.era_done[era_name] = stats.era_done.get(era_name, 0) + 1
        checkpoint.add(ttb_id)

    except Exception:
        stats.errors += 1
        if not skip_images:
            try:
                await page.unroute("**/publicViewAttachment.do*filetype=l*")
            except Exception:
                pass
        # Record completion for stats/dashboard but DO NOT checkpoint
        duration = time.monotonic() - t0
        stats.record_completion(ttb_id, brand_name, n_images, duration)
        stats.era_done[era_name] = stats.era_done.get(era_name, 0) + 1


async def pw_worker(
    worker_id: int,
    context,
    queue: asyncio.Queue,
    rate_limiter: RateLimiter,
    output_dir: Path,
    stats: Stats,
    checkpoint: Checkpoint,
    db_writer: DbWriter,
    shutdown_event: asyncio.Event,
    skip_images: bool = False,
):
    """Worker coroutine that owns one Playwright page and processes records."""
    page = await context.new_page()
    pages_processed = 0

    while not shutdown_event.is_set():
        try:
            record = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        ttb_id = record["ttb_id"]
        brand = record.get("brand_name", "")
        year_str = record["year_str"]
        era_name = record["era_name"]

        for attempt in range(3):
            try:
                await process_record_pw(
                    page, rate_limiter, ttb_id, brand, year_str,
                    output_dir, stats, checkpoint, db_writer, era_name,
                    skip_images=skip_images,
                )
                pages_processed += 1
                break
            except Exception as e:
                err_name = type(e).__name__
                if "TargetClosedError" in err_name or "closed" in str(e).lower():
                    # Browser page died — get a fresh one
                    try:
                        page = await context.new_page()
                    except Exception:
                        # Context itself is dead — nothing we can do
                        stats.errors += 1
                        # DO NOT checkpoint — record was not scraped
                        stats.era_done[era_name] = stats.era_done.get(era_name, 0) + 1
                        queue.task_done()
                        return
                    stats.retries += 1
                    continue
                stats.retries += 1
                if attempt < 2:
                    await asyncio.sleep(1.0 * (attempt + 1))
                else:
                    stats.errors += 1
                    # DO NOT checkpoint — record was not scraped
                    stats.era_done[era_name] = stats.era_done.get(era_name, 0) + 1

        # Recycle page every 500 records to prevent memory buildup
        if pages_processed % 500 == 0 and pages_processed > 0:
            try:
                await page.close()
            except Exception:
                pass
            page = await context.new_page()

        queue.task_done()

    try:
        await page.close()
    except Exception:
        pass


# ─── Main ────────────────────────────────────────────────────────────────────

def get_era(year: int) -> str:
    if year < 2010:
        return "2005-09"
    elif year < 2015:
        return "2010-14"
    elif year < 2020:
        return "2015-19"
    else:
        return "2020+"


def fetch_record_list(start_year: int, limit: int | None = None) -> list[dict]:
    """Fetch eligible TTB IDs from the database, chunked by year to avoid timeout."""
    sb = get_supabase()
    all_records = []
    batch_size = 1000
    current_year = time.localtime().tm_year

    for year in range(start_year, current_year + 1):
        year_start = f"{year}-01-01"
        year_end = f"{year}-12-31"
        offset = 0

        while True:
            try:
                result = (
                    sb.table("source_ttb_colas")
                    .select("ttb_id,brand_name,completed_date")
                    .eq("status", "APPROVED")
                    .in_("class_type", ["80", "81", "80A", "84", "88"])
                    .gte("completed_date", year_start)
                    .lte("completed_date", year_end)
                    .order("ttb_id")
                    .range(offset, offset + batch_size - 1)
                ).execute()
            except Exception as e:
                print(f"\n  Retry {year} offset {offset}: {str(e)[:80]}")
                time.sleep(2)
                continue

            if not result.data:
                break
            all_records.extend(result.data)
            if len(result.data) < batch_size:
                break
            offset += batch_size

        print(f"  {year}: {len(all_records):,} total", end="\r")

        if limit and len(all_records) >= limit:
            all_records = all_records[:limit]
            break

    print(f"  Fetched {len(all_records):,} records across {current_year - start_year + 1} years")
    return all_records


async def run_scraper(args):
    """Main scraper orchestration with Playwright."""
    from playwright.async_api import async_playwright

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    console = Console()

    # ── Fetch record list ──
    console.print(f"\n[bold]Fetching record list from database (since {args.start_year})...[/bold]")
    cache_file = output_dir / "record_list.json"

    if cache_file.exists() and not args.refresh_list:
        console.print(f"  Loading cached list from {cache_file.name}...")
        records = json.loads(cache_file.read_text(encoding="utf-8"))
        console.print(f"  Loaded {len(records):,} records from cache.")
    else:
        records = fetch_record_list(args.start_year, args.limit)
        cache_file.write_text(json.dumps(records), encoding="utf-8")
        console.print(f"  Fetched and cached {len(records):,} records.")

    # Annotate with year_str and era_name
    for r in records:
        date_str = r.get("completed_date", "")
        year = int(date_str[:4]) if date_str and len(date_str) >= 4 else 2005
        r["year_str"] = str(year)
        r["era_name"] = get_era(year)

    # ── Load checkpoint ──
    checkpoint = Checkpoint(output_dir / "checkpoint.txt")
    checkpoint.load()
    already_done = len(checkpoint.completed)

    # ── Rebuild checkpoint from DB truth ──
    if args.rebuild_checkpoint:
        console.print(f"  [yellow]Rebuilding checkpoint from DB (old: {already_done:,} entries)...[/yellow]")
        sb = get_supabase()
        real_done = set()
        batch_size = 1000
        current_year = time.localtime().tm_year

        # Query by year to avoid statement timeout on large table
        for year in range(args.start_year, current_year + 1):
            year_start = f"{year}-01-01"
            year_end = f"{year}-12-31"
            offset = 0
            year_count = 0

            while True:
                try:
                    result = (sb.table("source_ttb_colas")
                              .select("ttb_id")
                              .not_.is_("detail_scraped_at", "null")
                              .gte("completed_date", year_start)
                              .lte("completed_date", year_end)
                              .order("ttb_id")
                              .range(offset, offset + batch_size - 1)
                              .execute())
                except Exception as e:
                    console.print(f"    Retry {year} offset {offset}: {str(e)[:60]}")
                    time.sleep(2)
                    continue
                if not result.data:
                    break
                for row in result.data:
                    real_done.add(row["ttb_id"])
                year_count += len(result.data)
                if len(result.data) < batch_size:
                    break
                offset += batch_size

            if year_count > 0:
                console.print(f"    {year}: {year_count:,} verified ({len(real_done):,} total)")

        # Rewrite checkpoint with only DB-verified records
        checkpoint.close()
        with open(checkpoint.path, "w") as f:
            for ttb_id in sorted(real_done):
                f.write(ttb_id + "\n")
        checkpoint = Checkpoint(output_dir / "checkpoint.txt")
        checkpoint.load()
        already_done = len(checkpoint.completed)
        console.print(f"  [green]Checkpoint rebuilt: {already_done:,} verified records[/green]")

    # ── Retry-images mode: only re-scrape records with failed images ──
    if args.retry_images:
        fail_path = output_dir / "image_failures.jsonl"
        if not fail_path.exists():
            console.print("[green]No image failures to retry![/green]")
            return
        retry_ids = set()
        with open(fail_path, "r") as ff:
            for line in ff:
                try:
                    retry_ids.add(json.loads(line)["ttb_id"])
                except Exception:
                    pass
        console.print(f"  [yellow]Retry mode: {len(retry_ids):,} records with image failures[/yellow]")
        # Remove these from checkpoint so they get re-processed
        checkpoint.completed -= retry_ids
        # Clear the failure log — will be re-populated if they fail again
        fail_path.rename(fail_path.with_suffix(".jsonl.bak"))
        already_done = len(checkpoint.completed)

    # ── Reset mode: clear checkpoint to start fresh ──
    if args.reset:
        console.print(f"  [yellow]Reset mode: clearing checkpoint ({already_done:,} entries)[/yellow]")
        checkpoint.completed.clear()
        checkpoint.close()
        # Rewrite empty checkpoint
        checkpoint = Checkpoint(output_dir / "checkpoint.txt")
        open(checkpoint.path, "w").close()  # truncate
        checkpoint.load()
        already_done = 0
        # Also clear failure log
        fail_path = output_dir / "image_failures.jsonl"
        if fail_path.exists():
            fail_path.unlink()
        # Clear detail_scraped_at in DB so we know what's fresh
        console.print(f"  Clearing detail_scraped_at in DB...")
        try:
            sb = get_supabase()
            sb.table("source_ttb_colas").update({
                "detail_scraped_at": None,
                "wine_appellation": None,
                "wine_vintage": None,
                "abv": None,
                "phone": None,
                "email": None,
                "label_image_urls": None,
                "applicant_name": None,
                "applicant_address": None,
                "applicant_city": None,
                "applicant_state": None,
                "applicant_zip": None,
            }).not_.is_("detail_scraped_at", "null").execute()
            console.print(f"  [green]DB cleared.[/green]")
        except Exception as e:
            console.print(f"  [yellow]Could not clear DB (non-fatal): {e}[/yellow]")

    remaining = [r for r in records if r["ttb_id"] not in checkpoint]
    console.print(f"  Already completed: {already_done:,}")
    console.print(f"  Remaining: {len(remaining):,}\n")

    if not remaining:
        console.print("[green]All records already processed![/green]")
        return

    # ── Initialize stats ──
    stats = Stats()
    stats.total_records = len(records)
    stats.completed = already_done
    stats.start_time = time.monotonic()

    for r in records:
        era = r["era_name"]
        stats.era_totals[era] = stats.era_totals.get(era, 0) + 1

    for r in records:
        if r["ttb_id"] in checkpoint:
            era = r["era_name"]
            stats.era_done[era] = stats.era_done.get(era, 0) + 1

    # ── Restore data/image stats from DB for previously completed records ──
    if already_done > 0:
        console.print(f"  Restoring stats from DB for {already_done:,} prior records...")
        try:
            sb = get_supabase()
            def _count_scraped(col):
                """Count non-null values among detail-scraped records."""
                r = (sb.table("source_ttb_colas")
                     .select("ttb_id", count="exact", head=True)
                     .not_.is_("detail_scraped_at", "null")
                     .not_.is_(col, "null")
                     .execute())
                return r.count or 0
            scraped_total = (sb.table("source_ttb_colas")
                            .select("ttb_id", count="exact", head=True)
                            .not_.is_("detail_scraped_at", "null")
                            .execute()).count or 0
            stats.grapes_found = _count_scraped("grape_varietals")
            stats.appellations_found = _count_scraped("wine_appellation")
            stats.vintages_found = _count_scraped("wine_vintage")
            n_with_images = _count_scraped("label_image_urls")
            stats.fields_extracted = max(stats.appellations_found, stats.grapes_found, stats.vintages_found)
            stats.no_images = scraped_total - n_with_images
            console.print(f"  Restored: {stats.grapes_found:,} grapes, {stats.appellations_found:,} appellations, "
                          f"{stats.vintages_found:,} vintages, {n_with_images:,} with images")
        except Exception as e:
            console.print(f"  [yellow]Could not restore stats (non-fatal): {e}[/yellow]")

    # ── Build queue ──
    queue: asyncio.Queue = asyncio.Queue()
    for r in remaining:
        await queue.put(r)

    # ── Setup ──
    rate_limiter = RateLimiter(rate=args.rate_limit, burst=args.workers)
    shutdown_event = asyncio.Event()
    db_writer = DbWriter()
    stats_file = output_dir / "stats.json"
    last_stats_save = time.monotonic()

    console.print(f"[bold]Launching {args.workers} Playwright workers...[/bold]")
    if args.no_images:
        console.print("[yellow]  Image download disabled (--no-images)[/yellow]")
    console.print()

    async with async_playwright() as pw:

        async def launch_browser():
            """Launch browser + context with WAF warmup."""
            b = await pw.chromium.launch(
                headless=True,
                args=[
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-background-timer-throttling",
                    "--disable-renderer-backgrounding",
                ],
            )
            ctx = await b.new_context(
                ignore_https_errors=True,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            )
            await ctx.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            # Warmup — solve WAF challenge
            warmup_page = await ctx.new_page()
            try:
                await warmup_page.goto(
                    "https://ttbonline.gov/colasonline/publicSearchColasBasic.do",
                    wait_until="networkidle",
                    timeout=60000,
                )
                sample_id = remaining[0]["ttb_id"] if remaining else "04338001000001"
                await warmup_page.goto(
                    TTB_PRINTABLE_URL.format(ttb_id=sample_id),
                    wait_until="domcontentloaded",
                    timeout=30000,
                )
                test_html = await warmup_page.content()
                if "bobcmn" in test_html:
                    await warmup_page.wait_for_load_state("networkidle", timeout=30000)
                cookies = await ctx.cookies()
                console.print(f"  [green]WAF solved, session ready ({len(cookies)} cookies)[/green]")
            except Exception as e:
                console.print(f"  [yellow]Warmup issue: {e} — continuing[/yellow]")
            await warmup_page.close()
            return b, ctx

        # Initial launch
        console.print("  Warming up browser (solving WAF challenge)...")
        browser, context = await launch_browser()
        browser_restarts = 0

        # Outer loop: restarts browser on crash, continues from queue
        with Live(build_dashboard(stats, args.workers), console=console, refresh_per_second=2) as live:
            while not queue.empty() and not shutdown_event.is_set():

                # Launch workers
                worker_tasks = []
                for i in range(args.workers):
                    w = asyncio.create_task(
                        pw_worker(
                            i, context, queue, rate_limiter, output_dir,
                            stats, checkpoint, db_writer, shutdown_event,
                            skip_images=args.no_images,
                        )
                    )
                    worker_tasks.append(w)

                # Dashboard loop — also monitors for browser crash
                while not shutdown_event.is_set():
                    # Check if all workers finished (normally or crashed)
                    if all(w.done() for w in worker_tasks):
                        break
                    await asyncio.sleep(0.5)
                    live.update(build_dashboard(stats, args.workers))

                    now = time.monotonic()
                    if now - last_stats_save > 60:
                        stats.save(stats_file)
                        last_stats_save = now

                # Collect worker results
                await asyncio.gather(*worker_tasks, return_exceptions=True)
                live.update(build_dashboard(stats, args.workers))

                # If queue is not empty, workers died (browser crash) — restart
                if not queue.empty() and not shutdown_event.is_set():
                    browser_restarts += 1
                    await db_writer.flush()
                    console.print(f"\n  [yellow]Browser crashed — restarting (attempt {browser_restarts})...[/yellow]")
                    try:
                        await browser.close()
                    except Exception:
                        pass
                    await asyncio.sleep(2)
                    browser, context = await launch_browser()

        try:
            await context.close()
            await browser.close()
        except Exception:
            pass

    # ── Final DB flush ──
    await db_writer.flush()

    # ── Cleanup ──
    checkpoint.close()
    stats.save(stats_file)

    # ── Check for image failures ──
    fail_path = output_dir / "image_failures.jsonl"
    n_fail_records = 0
    if fail_path.exists():
        with open(fail_path, "r") as ff:
            n_fail_records = sum(1 for _ in ff)

    console.print(f"\n[bold green]Complete![/bold green]")
    console.print(f"  Records processed: {stats.completed:,} / {stats.total_records:,}")
    console.print(f"  Images downloaded: {stats.total_images:,}")
    if stats.image_dl_failures > 0:
        console.print(f"  [red]Image download failures: {stats.image_dl_failures:,} images across {n_fail_records:,} records[/red]")
        console.print(f"  [yellow]Run with --retry-images to re-scrape failed records[/yellow]")
    console.print(f"  Data extracted: {stats.fields_extracted:,} records")
    console.print(f"    Grapes: {stats.grapes_found:,}  |  Appellations: {stats.appellations_found:,}  |  Vintages: {stats.vintages_found:,}")
    console.print(f"  DB rows updated: {db_writer.total_written:,} ({db_writer.write_errors} errors)")
    console.print(f"  Storage used: {format_bytes(stats.bytes_downloaded)}")
    console.print(f"  Checkpoint: {checkpoint.path}")
    console.print(f"  Stats: {stats_file}\n")


def main():
    parser = argparse.ArgumentParser(description="TTB COLA Label Image Scraper + Detail Extractor")
    parser.add_argument("--output-dir", type=str, default=str(DEFAULT_OUTPUT),
                        help=f"Output directory (default: {DEFAULT_OUTPUT})")
    parser.add_argument("--workers", type=int, default=20, help="Concurrent browser pages (default: 20)")
    parser.add_argument("--rate-limit", type=float, default=30, help="Max pages/sec (default: 30)")
    parser.add_argument("--start-year", type=int, default=2005, help="Start year (default: 2005)")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of records (for testing)")
    parser.add_argument("--refresh-list", action="store_true", help="Re-fetch record list from DB")
    parser.add_argument("--no-images", action="store_true", help="Skip images, extract data fields only")
    parser.add_argument("--retry-images", action="store_true",
                        help="Re-scrape only records that had image download failures")
    parser.add_argument("--rebuild-checkpoint", action="store_true",
                        help="Rebuild checkpoint from DB — only keeps records with actual data")
    parser.add_argument("--reset", action="store_true",
                        help="Start completely fresh — clears checkpoint and DB detail fields")
    args = parser.parse_args()

    # Windows: use ProactorEventLoop (default) — needed for Playwright subprocesses
    if sys.platform == "win32":
        import codecs
        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, errors="replace")
        sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, errors="replace")
        os.environ["PYTHONIOENCODING"] = "utf-8"

    asyncio.run(run_scraper(args))


if __name__ == "__main__":
    main()
