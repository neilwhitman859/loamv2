#!/usr/bin/env python3
"""
TTB COLA Detail Scraper v2 — aiohttp + session cookie approach.

Strategy:
1. Use undetected-chromedriver to open TTB search page (solves Shape Security WAF)
2. Extract session cookies from the browser
3. Use aiohttp with those cookies to hit detail pages at high speed
4. Parse structured fields from the HTML detail page
5. Write extracted data to source_ttb_colas in Supabase

The detail page URL pattern:
  viewColaDetails.do?action=publicDisplaySearchAdvanced&ttbid={TTB_ID}

This requires a valid session (cookies from visiting the search page first).
The detail page has all structured fields: grape varietals, vintage, appellation,
ABV, applicant info, qualifications, etc.

Usage:
    python -m pipeline.fetch.ttb_detail_scraper [options]
"""

import argparse
import asyncio
import html as html_lib
import json
import os
import re
import ssl
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

import aiohttp
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.lib.db import get_supabase

# ─── Constants ───────────────────────────────────────────────────────────────

DETAIL_URL = "https://ttbonline.gov/colasonline/viewColaDetails.do?action=publicDisplaySearchAdvanced&ttbid={ttb_id}"
SEARCH_URL = "https://ttbonline.gov/colasonline/publicSearchColasBasic.do"
IMAGE_URL_PATTERN = "https://ttbonline.gov/colasonline/publicViewImage.do?id={image_id}"

DEFAULT_OUTPUT = Path.home() / "Desktop" / "Loam Cowork" / "data" / "imports" / "ttb_cola_labels"
DB_BATCH_SIZE = 100

# ─── Field Extraction (detail page format) ───────────────────────────────────

def extract_detail_fields(html: str) -> dict:
    """Extract structured fields from the non-printable detail page.

    The detail page uses <strong> tags with this pattern:
        <strong>Label:</strong> <a...help icon...></a> &nbsp; VALUE </td>
    Or simpler:
        <strong>Label:</strong> &nbsp; VALUE </td>
    """
    data = {}

    def get_field(label_pattern: str) -> str | None:
        """Extract value after a <strong> label."""
        # Pattern: <strong>Label:</strong> ... (help icon) ... VALUE ... </td>
        pattern = re.compile(
            r'<strong>' + label_pattern + r'[^<]*</strong>'
            r'(.*?)</td>',
            re.IGNORECASE | re.DOTALL,
        )
        m = pattern.search(html)
        if not m:
            return None
        raw = m.group(1)
        # Strip tags (removes help icon <a><img></a>)
        text = re.sub(r'<[^>]+>', ' ', raw)
        text = html_lib.unescape(text)
        text = re.sub(r'\s+', ' ', text).strip()
        if not text or text == '\xa0' or text.upper() == 'N/A':
            return None
        return text

    # Grape varietals
    data["grape_varietals"] = get_field(r'Grape Varietal')

    # Wine vintage
    data["wine_vintage"] = get_field(r'Wine Vintage')

    # Wine appellation — may appear as "Appellation" or "Wine Appellation"
    data["wine_appellation"] = get_field(r'(?:Wine )?Appellation')

    # ABV — appears as "Alcohol Content" on some pages
    data["abv"] = get_field(r'Alcohol Content')

    # Phone — no <strong> wrapper, just raw text "Phone Number:&nbsp; VALUE"
    phone_m = re.search(r'Phone Number:[&nbsp;\s]*([\d\(\)\-\s]+)', html)
    if phone_m:
        phone = phone_m.group(1).strip()
        if phone:
            data["phone"] = phone

    # Email
    email_m = re.search(r'Email[^:]*:[&nbsp;\s]*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', html)
    if email_m:
        data["email"] = email_m.group(1)

    # Qualifications — value is in a separate row after the label
    quals_m = re.search(
        r'<strong>Qualifications.*?</td>\s*</tr>\s*<tr[^>]*>\s*<td[^>]*>(.*?)</td>',
        html, re.IGNORECASE | re.DOTALL,
    )
    if quals_m:
        quals_text = re.sub(r'<[^>]+>', ' ', quals_m.group(1))
        quals_text = html_lib.unescape(quals_text).strip()
        quals_text = re.sub(r'\s+', ' ', quals_text)
        if quals_text and quals_text != '\xa0':
            data["qualifications"] = quals_text

    # Applicant info from Plant Registry section
    permit_pattern = re.compile(
        r'Plant Registry.*?Principal Place of Business.*?</strong>\s*'
        r'(?:</td>\s*<td[^>]*>\s*)?'
        r'(.*?)(?:</td>|<hr|While the Alcohol)',
        re.IGNORECASE | re.DOTALL,
    )
    pm = permit_pattern.search(html)
    if pm:
        raw_block = pm.group(1)
        # Split on <br> or newlines
        parts = re.split(r'<br\s*/?>|\n', raw_block)
        parts = [html_lib.unescape(re.sub(r'<[^>]+>', '', p)).strip() for p in parts]
        parts = [p for p in parts if p and p != '\xa0']

        # First line after permit number is typically company name
        # Permit is like "TX-I-1277"
        permit_re = re.compile(r'^[A-Z]{2}-[A-Z]-\d+$')
        name_idx = None
        for i, p in enumerate(parts):
            if permit_re.match(p):
                continue
            if not name_idx:
                name_idx = i
                data["applicant_name"] = p
                continue
            # Look for city/state/zip pattern
            csz = re.match(r'^(.+?),?\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$', p)
            if csz:
                data["applicant_city"] = csz.group(1).strip().rstrip(',')
                data["applicant_state"] = csz.group(2)
                data["applicant_zip"] = csz.group(3)
                break
            elif name_idx and not data.get("applicant_address"):
                data["applicant_address"] = p

    # Extract image IDs from imageWindow() calls
    image_ids = re.findall(r'imageWindow\([\'"]?(\d+)[\'"]?\)', html)
    if image_ids:
        data["_image_ids"] = image_ids

    # Clean: remove None/empty values
    return {k: v for k, v in data.items() if v}


# ─── Cookie Acquisition ──────────────────────────────────────────────────────

def get_session_cookies() -> dict[str, str]:
    """Hit TTB search page with requests to get session cookies.

    No browser needed! The Shape Security cookies from the HTTP response
    are sufficient for subsequent detail page requests via aiohttp.
    """
    import requests

    console = Console()
    console.print("[bold]Acquiring session cookies...[/bold]")

    s = requests.Session()
    s.verify = False
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    })

    r = s.get(SEARCH_URL, timeout=15)
    console.print(f"  Search page: {r.status_code}, {len(s.cookies)} cookies")

    if r.status_code != 200:
        console.print(f"[red]  Failed to load search page (status {r.status_code})[/red]")
        return {}

    cookie_dict = {c.name: c.value for c in s.cookies}

    # Verify detail page access
    console.print("  Testing detail page...")
    r2 = s.get(DETAIL_URL.format(ttb_id="19333001000116"), timeout=15)
    if "Application Detail" in r2.text and "<strong>" in r2.text:
        console.print("  [green]✓ Detail page access confirmed![/green]")
        # Update cookies after second request (may have more)
        cookie_dict = {c.name: c.value for c in s.cookies}
    else:
        console.print(f"  [yellow]Detail page may not work ({len(r2.text)} bytes)[/yellow]")

    console.print(f"  [green]Got {len(cookie_dict)} cookies[/green]")
    return cookie_dict


# ─── Rate Limiter ────────────────────────────────────────────────────────────

class RateLimiter:
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
            await asyncio.sleep(0.01)


# ─── Stats ───────────────────────────────────────────────────────────────────

@dataclass
class Stats:
    total_records: int = 0
    completed: int = 0
    errors: int = 0
    retries: int = 0
    waf_blocks: int = 0
    session_refreshes: int = 0

    grapes_found: int = 0
    appellations_found: int = 0
    vintages_found: int = 0
    fields_extracted: int = 0

    start_time: float = 0.0
    _recent_times: deque = field(default_factory=lambda: deque(maxlen=200))
    _throughput_history: deque = field(default_factory=lambda: deque(maxlen=60))
    _last_throughput_update: float = 0.0
    peak_throughput: float = 0.0
    _recent_latencies: deque = field(default_factory=lambda: deque(maxlen=500))
    recent: deque = field(default_factory=lambda: deque(maxlen=8))
    era_totals: dict = field(default_factory=dict)
    era_done: dict = field(default_factory=dict)

    @property
    def elapsed(self): return time.monotonic() - self.start_time if self.start_time else 0
    @property
    def avg_throughput(self): return self.completed / self.elapsed if self.elapsed > 1 else 0
    @property
    def current_throughput(self):
        now = time.monotonic()
        recent = [t for t in self._recent_times if t > now - 10]
        if not recent: return 0
        span = now - recent[0]
        return len(recent) / span if span > 0.1 else 0
    @property
    def avg_latency_ms(self):
        if not self._recent_latencies: return 0
        return sum(self._recent_latencies) / len(self._recent_latencies) * 1000
    @property
    def eta_seconds(self):
        rate = self.avg_throughput
        if rate <= 0: return 0
        return (self.total_records - self.completed) / rate

    def record_completion(self, ttb_id: str, brand: str, has_data: bool, duration: float):
        now = time.monotonic()
        self._recent_times.append(now)
        self._recent_latencies.append(duration)
        self.completed += 1
        if now - self._last_throughput_update >= 1.0:
            ct = self.current_throughput
            self._throughput_history.append(ct)
            if ct > self.peak_throughput: self.peak_throughput = ct
            self._last_throughput_update = now
        self.recent.appendleft({"ttb_id": ttb_id, "brand": (brand or "—")[:30], "data": has_data, "time": f"{duration:.2f}s"})

    def sparkline(self) -> str:
        if not self._throughput_history: return ""
        bars = " ▁▂▃▄▅▆▇█"
        vals = list(self._throughput_history)
        mx = max(vals) if vals and max(vals) > 0 else 1
        return "".join(bars[min(8, int(v / mx * 8))] for v in vals[-40:])


# ─── Dashboard ───────────────────────────────────────────────────────────────

def format_duration(s: float) -> str:
    if s <= 0: return "—"
    h, m = int(s // 3600), int((s % 3600) // 60)
    return f"{h}h {m:02d}m" if h > 0 else f"{m}m"

def build_dashboard(stats: Stats, workers: int) -> Panel:
    pct = (stats.completed / stats.total_records * 100) if stats.total_records else 0
    bar_w = 50
    filled = int(bar_w * pct / 100)
    bar = f"[green]{'█' * filled}[/green][dim]{'░' * (bar_w - filled)}[/dim]"
    progress = f"  {bar}  {pct:5.1f}%  {stats.completed:,}\n  of {stats.total_records:,} records{'':>20}ETA: {format_duration(stats.eta_seconds)}"

    tp = Table.grid(padding=(0, 1))
    tp.add_row("[bold]Current:[/bold]", f"{stats.current_throughput:>6.1f} rec/s")
    tp.add_row("[bold]Average:[/bold]", f"{stats.avg_throughput:>6.1f} rec/s")
    tp.add_row("[bold]Peak:[/bold]", f"{stats.peak_throughput:>6.1f} rec/s")
    tp.add_row("", f"[dim]{stats.sparkline()}[/dim]")
    tp_panel = Panel(tp, title="Throughput", border_style="blue", width=30)

    dp = Table.grid(padding=(0, 1))
    dp.add_row("[bold]Grapes:[/bold]", f"{stats.grapes_found:>12,}")
    dp.add_row("[bold]Appellations:[/bold]", f"{stats.appellations_found:>12,}")
    dp.add_row("[bold]Vintages:[/bold]", f"{stats.vintages_found:>12,}")
    ext_pct = (stats.fields_extracted / stats.completed * 100) if stats.completed else 0
    dp.add_row("[bold]Has data:[/bold]", f"{stats.fields_extracted:>8,}  [dim]({ext_pct:.1f}%)[/dim]")
    dp_panel = Panel(dp, title="Extracted", border_style="magenta", width=34)

    np_t = Table.grid(padding=(0, 1))
    np_t.add_row("[bold]Avg latency:[/bold]", f"{stats.avg_latency_ms:>8.0f}ms")
    err_pct = (stats.errors / stats.completed * 100) if stats.completed else 0
    np_t.add_row("[bold]Errors:[/bold]", f"{stats.errors:>8,}  [dim]({err_pct:.3f}%)[/dim]")
    if stats.waf_blocks > 0:
        np_t.add_row("[bold red]WAF blocks:[/bold red]", f"[red]{stats.waf_blocks:>8,}[/red]")
    np_t.add_row("[bold]Retries:[/bold]", f"{stats.retries:>8,}")
    np_t.add_row("[bold]Session:[/bold]", f"{stats.session_refreshes:>8,} refreshes")
    np_panel = Panel(np_t, title="Network", border_style="red", width=34)

    era_t = Table.grid(padding=(0, 1))
    for era in sorted(stats.era_totals.keys()):
        total = stats.era_totals[era]
        done = stats.era_done.get(era, 0)
        ep = (done / total * 100) if total else 0
        ew = 30
        ef = int(ew * ep / 100)
        era_t.add_row(f"[bold]{era}[/bold]", f"[green]{'█' * ef}[/green][dim]{'░' * (ew - ef)}[/dim]", f"{ep:>5.1f}%", f"{done:>9,}")
    era_panel = Panel(era_t, title="By Era", border_style="cyan")

    rec_t = Table.grid(padding=(0, 2))
    for e in list(stats.recent)[:6]:
        data_str = "[green]✓[/green]" if e["data"] else "[dim]—[/dim]"
        rec_t.add_row(f"[dim]{e['ttb_id']}[/dim]", e["brand"], data_str, f"[dim]{e['time']}[/dim]")
    rec_panel = Panel(rec_t, title="Recent", border_style="dim")

    elapsed_str = format_duration(stats.elapsed)
    started = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time() - stats.elapsed))
    footer = f"  Started: {started}  |  Elapsed: {elapsed_str}  |  Workers: {workers}  |  Ctrl+C to stop"

    layout = Table.grid(padding=(1, 0))
    layout.add_row(Text(progress, style="bold"))
    row1 = Table.grid(padding=(0, 2))
    row1.add_row(tp_panel, dp_panel)
    row2 = Table.grid(padding=(0, 2))
    row2.add_row(Panel("", width=30, border_style="dim"), np_panel)  # placeholder for symmetry
    layout.add_row(row1)
    layout.add_row(np_panel)
    layout.add_row(era_panel)
    layout.add_row(rec_panel)
    layout.add_row(Text(footer, style="dim"))

    return Panel(layout, title="[bold white] TTB COLA Detail Scraper v2 (aiohttp) [/bold white]", border_style="bold white")


# ─── Checkpoint ──────────────────────────────────────────────────────────────

class Checkpoint:
    def __init__(self, path: Path):
        self.path = path
        self.completed: set[str] = set()
        self._file = None

    def load(self):
        if self.path.exists():
            with open(self.path, "r") as f:
                for line in f:
                    t = line.strip()
                    if t: self.completed.add(t)
        self._file = open(self.path, "a", buffering=1)

    def add(self, ttb_id: str):
        self.completed.add(ttb_id)
        if self._file: self._file.write(ttb_id + "\n")

    def close(self):
        if self._file:
            self._file.flush()
            self._file.close()

    def __contains__(self, ttb_id): return ttb_id in self.completed


# ─── DB Writer ───────────────────────────────────────────────────────────────

class DbWriter:
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

    async def add(self, ttb_id: str, fields: dict):
        if not fields: return
        row = {"ttb_id": ttb_id, "detail_scraped_at": "now()"}
        # Remove internal fields
        clean = {k: v for k, v in fields.items() if not k.startswith("_")}
        row.update(clean)

        async with self._lock:
            self._buffer.append(row)
            if len(self._buffer) >= DB_BATCH_SIZE:
                await self._flush()

    async def _flush(self):
        if not self._buffer: return
        batch = self._buffer[:]
        self._buffer.clear()
        try:
            sb = self._get_sb()
            sb.table("source_ttb_colas").upsert(batch, on_conflict="ttb_id").execute()
            self.total_written += len(batch)
        except Exception:
            self.write_errors += 1
            sb = self._get_sb()
            for row in batch:
                try:
                    sb.table("source_ttb_colas").upsert(row, on_conflict="ttb_id").execute()
                    self.total_written += 1
                except Exception:
                    self.write_errors += 1

    async def flush(self):
        async with self._lock:
            await self._flush()


# ─── Worker ──────────────────────────────────────────────────────────────────

async def process_record(
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    ttb_id: str,
    brand_name: str,
    stats: Stats,
    checkpoint: Checkpoint,
    db_writer: DbWriter,
    era_name: str,
) -> bool:
    """Fetch and parse one detail page. Returns True if WAF blocked (need session refresh)."""
    t0 = time.monotonic()

    await rate_limiter.acquire()

    url = DETAIL_URL.format(ttb_id=ttb_id)

    for attempt in range(3):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    stats.errors += 1
                    if attempt < 2:
                        stats.retries += 1
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    duration = time.monotonic() - t0
                    stats.record_completion(ttb_id, brand_name, False, duration)
                    stats.era_done[era_name] = stats.era_done.get(era_name, 0) + 1
                    return False

                html = await resp.text()

                # WAF detection
                if "Application Detail" not in html and ("bobcmn" in html or "UiwV" in html):
                    stats.waf_blocks += 1
                    return True  # Signal WAF block — need cookie refresh

                # Error page detection
                if "Error Message" in html:
                    stats.errors += 1
                    if attempt < 2:
                        stats.retries += 1
                        await asyncio.sleep(1.0)
                        continue
                    duration = time.monotonic() - t0
                    stats.record_completion(ttb_id, brand_name, False, duration)
                    stats.era_done[era_name] = stats.era_done.get(era_name, 0) + 1
                    return False

                # Extract fields
                extracted = extract_detail_fields(html)
                has_data = bool(extracted)

                if has_data:
                    stats.fields_extracted += 1
                    if "grape_varietals" in extracted: stats.grapes_found += 1
                    if "wine_appellation" in extracted: stats.appellations_found += 1
                    if "wine_vintage" in extracted: stats.vintages_found += 1

                # Write to DB
                await db_writer.add(ttb_id, extracted)

                # Checkpoint on success
                duration = time.monotonic() - t0
                stats.record_completion(ttb_id, brand_name, has_data, duration)
                stats.era_done[era_name] = stats.era_done.get(era_name, 0) + 1
                checkpoint.add(ttb_id)
                return False

        except asyncio.TimeoutError:
            stats.retries += 1
            if attempt < 2:
                await asyncio.sleep(1.0 * (attempt + 1))
                continue
            stats.errors += 1
            duration = time.monotonic() - t0
            stats.record_completion(ttb_id, brand_name, False, duration)
            stats.era_done[era_name] = stats.era_done.get(era_name, 0) + 1
            return False
        except Exception:
            stats.retries += 1
            if attempt < 2:
                await asyncio.sleep(0.5)
                continue
            stats.errors += 1
            duration = time.monotonic() - t0
            stats.record_completion(ttb_id, brand_name, False, duration)
            stats.era_done[era_name] = stats.era_done.get(era_name, 0) + 1
            return False

    return False


async def worker(
    worker_id: int,
    session: aiohttp.ClientSession,
    queue: asyncio.Queue,
    rate_limiter: RateLimiter,
    stats: Stats,
    checkpoint: Checkpoint,
    db_writer: DbWriter,
    shutdown_event: asyncio.Event,
    waf_event: asyncio.Event,
):
    """Worker coroutine that processes records from the queue."""
    while not shutdown_event.is_set():
        try:
            record = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        ttb_id = record["ttb_id"]
        brand = record.get("brand_name", "")
        era_name = record["era_name"]

        waf_blocked = await process_record(
            session, rate_limiter, ttb_id, brand,
            stats, checkpoint, db_writer, era_name,
        )

        if waf_blocked:
            # Put record back and signal WAF block
            await queue.put(record)
            waf_event.set()
            return  # Stop this worker

        queue.task_done()


# ─── Record List ─────────────────────────────────────────────────────────────

def get_era(year: int) -> str:
    if year < 2010: return "2005-09"
    elif year < 2015: return "2010-14"
    elif year < 2020: return "2015-19"
    else: return "2020+"


def fetch_record_list(start_year: int, limit: int | None = None) -> list[dict]:
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
                result = (sb.table("source_ttb_colas")
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
            if not result.data: break
            all_records.extend(result.data)
            if len(result.data) < batch_size: break
            offset += batch_size
        print(f"  {year}: {len(all_records):,} total", end="\r")
        if limit and len(all_records) >= limit:
            all_records = all_records[:limit]
            break

    print(f"  Fetched {len(all_records):,} records")
    return all_records


# ─── Main ────────────────────────────────────────────────────────────────────

async def run_scraper(args):
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    console = Console()

    # ── Fetch record list ──
    console.print(f"\n[bold]Fetching record list (since {args.start_year})...[/bold]")
    cache_file = output_dir / "record_list.json"
    if cache_file.exists() and not args.refresh_list:
        console.print(f"  Loading cached list...")
        records = json.loads(cache_file.read_text(encoding="utf-8"))
        console.print(f"  Loaded {len(records):,} records from cache.")
    else:
        records = fetch_record_list(args.start_year, args.limit)
        cache_file.write_text(json.dumps(records), encoding="utf-8")

    for r in records:
        date_str = r.get("completed_date", "")
        year = int(date_str[:4]) if date_str and len(date_str) >= 4 else 2005
        r["year_str"] = str(year)
        r["era_name"] = get_era(year)

    # ── Load checkpoint ──
    checkpoint_file = output_dir / "checkpoint_v2.txt"
    checkpoint = Checkpoint(checkpoint_file)
    checkpoint.load()
    already_done = len(checkpoint.completed)

    if args.reset:
        console.print(f"  [yellow]Reset: clearing checkpoint ({already_done:,} entries)[/yellow]")
        checkpoint.completed.clear()
        checkpoint.close()
        open(checkpoint_file, "w").close()
        checkpoint = Checkpoint(checkpoint_file)
        checkpoint.load()
        already_done = 0

    remaining = [r for r in records if r["ttb_id"] not in checkpoint]
    console.print(f"  Already completed: {already_done:,}")
    console.print(f"  Remaining: {len(remaining):,}\n")

    if not remaining:
        console.print("[green]All records processed![/green]")
        return

    # ── Stats ──
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

    # ── Get session cookies ──
    cookies = get_session_cookies()
    if not cookies:
        console.print("[red]Failed to get session cookies. Exiting.[/red]")
        return

    # ── Build queue ──
    queue: asyncio.Queue = asyncio.Queue()
    for r in remaining:
        await queue.put(r)

    rate_limiter = RateLimiter(rate=args.rate_limit, burst=args.workers)
    shutdown_event = asyncio.Event()
    db_writer = DbWriter()

    console.print(f"[bold]Launching {args.workers} aiohttp workers...[/bold]\n")

    # SSL context that ignores cert issues
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    connector = aiohttp.TCPConnector(limit=args.workers + 5, ssl=ssl_ctx)

    async def run_with_session(cookie_dict: dict):
        """Run workers with given cookies. Returns when all done or WAF blocked."""
        waf_event = asyncio.Event()

        async with aiohttp.ClientSession(
            connector=connector,
            cookies=cookie_dict,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Referer": "https://ttbonline.gov/colasonline/publicSearchColasAdvancedProcess.do?action=search",
            },
        ) as session:
            worker_tasks = []
            for i in range(args.workers):
                w = asyncio.create_task(
                    worker(i, session, queue, rate_limiter, stats, checkpoint, db_writer, shutdown_event, waf_event)
                )
                worker_tasks.append(w)

            # Monitor loop
            while not shutdown_event.is_set():
                if all(w.done() for w in worker_tasks): break
                if waf_event.is_set(): break
                await asyncio.sleep(0.5)
                live.update(build_dashboard(stats, args.workers))

            # Cancel remaining workers if WAF blocked
            if waf_event.is_set():
                for w in worker_tasks:
                    if not w.done(): w.cancel()

            await asyncio.gather(*worker_tasks, return_exceptions=True)
            live.update(build_dashboard(stats, args.workers))

        return waf_event.is_set()

    with Live(build_dashboard(stats, args.workers), console=console, refresh_per_second=2) as live:
        while not queue.empty() and not shutdown_event.is_set():
            waf_blocked = await run_with_session(cookies)

            if waf_blocked and not queue.empty():
                stats.session_refreshes += 1
                await db_writer.flush()
                console.print(f"\n  [yellow]WAF detected — refreshing cookies (attempt {stats.session_refreshes})...[/yellow]")
                await asyncio.sleep(5)
                cookies = get_session_cookies()
                if not cookies:
                    console.print("[red]Failed to refresh cookies. Stopping.[/red]")
                    break

    # ── Cleanup ──
    await db_writer.flush()
    checkpoint.close()

    console.print(f"\n[bold green]Complete![/bold green]")
    console.print(f"  Records processed: {stats.completed:,} / {stats.total_records:,}")
    console.print(f"  Data extracted: {stats.fields_extracted:,}")
    console.print(f"    Grapes: {stats.grapes_found:,}  |  Appellations: {stats.appellations_found:,}  |  Vintages: {stats.vintages_found:,}")
    console.print(f"  DB rows updated: {db_writer.total_written:,} ({db_writer.write_errors} errors)")
    console.print(f"  Errors: {stats.errors:,}  |  WAF blocks: {stats.waf_blocks:,}")
    console.print(f"  Session refreshes: {stats.session_refreshes:,}")


def main():
    parser = argparse.ArgumentParser(description="TTB COLA Detail Scraper v2 (aiohttp)")
    parser.add_argument("--output-dir", type=str, default=str(DEFAULT_OUTPUT))
    parser.add_argument("--workers", type=int, default=50, help="Concurrent requests (default: 50)")
    parser.add_argument("--rate-limit", type=float, default=30, help="Max requests/sec (default: 30)")
    parser.add_argument("--start-year", type=int, default=2005)
    parser.add_argument("--limit", type=int, default=None, help="Limit records (testing)")
    parser.add_argument("--refresh-list", action="store_true")
    parser.add_argument("--reset", action="store_true", help="Start fresh")
    args = parser.parse_args()

    if sys.platform == "win32":
        import codecs
        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, errors="replace")
        sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, errors="replace")
        os.environ["PYTHONIOENCODING"] = "utf-8"

    asyncio.run(run_scraper(args))


if __name__ == "__main__":
    main()
