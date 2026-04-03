#!/usr/bin/env python3
"""
TTB COLA Label Image Downloader

Downloads label images from URLs stored in source_ttb_colas.

Two image types with different download strategies:
  - labels: publicViewAttachment.do URLs from label_image_urls
    -> Requires headed Playwright (Shape Security WAF blocks headless + direct HTTP)
    -> Navigates to the printable form page, intercepts <img> sub-resource responses
  - scans:  publicViewImage.do URLs from application_scan_urls
    -> Works via aiohttp with session cookies harvested from Playwright
    -> Much faster (direct HTTP, high concurrency)

Output structure:
  {output_dir}/labels/{year}/{ttb_id}/{original_filename.ext}
  {output_dir}/scans/{year}/{ttb_id}/scan.ext

Usage:
    python -m pipeline.fetch.ttb_image_downloader --type all
    python -m pipeline.fetch.ttb_image_downloader --type labels
    python -m pipeline.fetch.ttb_image_downloader --type scans
    python -m pipeline.fetch.ttb_image_downloader --type labels --limit 100
    python -m pipeline.fetch.ttb_image_downloader --type labels --concurrent 8
    python -m pipeline.fetch.ttb_image_downloader --type scans --concurrent 50
    python -m pipeline.fetch.ttb_image_downloader --type labels --year-min 2020 --year-max 2026
"""

import argparse
import asyncio
import logging
import os
import re
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import unquote, parse_qs, urlparse

# Force UTF-8 for Rich on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")

import aiohttp
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.lib.db import get_supabase

# ── Constants ────────────────────────────────────────────────────────────────

DEFAULT_OUTPUT = Path(r"D:\TTB Label Images")
LOG_FILE = DEFAULT_OUTPUT / "download.log"
DEFAULT_LABEL_CONCURRENT = 8   # Playwright pages (headed browser is heavy)
DEFAULT_SCAN_CONCURRENT = 35   # aiohttp connections (lightweight)

# Image magic bytes for format detection
IMAGE_MAGIC = {
    b'\xff\xd8\xff': ('jpeg', '.jpg'),
    b'\x89PNG':      ('png',  '.png'),
    b'GIF8':         ('gif',  '.gif'),
    b'RIFF':         ('webp', '.webp'),
    b'BM':           ('bmp',  '.bmp'),
    b'%PDF':         ('pdf',  '.pdf'),
}

UNSAFE_FILENAME_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


# ── Stats ────────────────────────────────────────────────────────────────────

@dataclass
class Stats:
    total_images: int = 0
    downloaded: int = 0
    skipped: int = 0    # already on disk (resume)
    errors: int = 0
    html_blocked: int = 0
    too_small: int = 0
    not_image: int = 0
    total_bytes: int = 0
    start_time: float = 0.0

    errors_by_type: dict = field(default_factory=dict)
    year_ok: dict = field(default_factory=dict)
    year_err: dict = field(default_factory=dict)
    year_bytes: dict = field(default_factory=dict)

    _recent_times: deque = field(default_factory=lambda: deque(maxlen=500))
    _throughput_history: deque = field(default_factory=lambda: deque(maxlen=60))
    _last_tp_update: float = 0.0
    peak_throughput: float = 0.0
    _eta_window: deque = field(default_factory=lambda: deque(maxlen=300))

    phase: str = ""
    phase_index: int = 0
    total_phases: int = 1
    concurrent: int = 0

    @property
    def elapsed(self):
        return time.monotonic() - self.start_time if self.start_time else 0

    @property
    def done(self):
        return self.downloaded + self.skipped + self.errors + self.html_blocked + self.too_small + self.not_image

    @property
    def remaining(self):
        return max(0, self.total_images - self.done)

    @property
    def pct(self):
        return (self.done / self.total_images * 100) if self.total_images else 0

    @property
    def current_throughput(self):
        now = time.monotonic()
        recent = [t for t in self._recent_times if t > now - 10]
        if not recent:
            return 0
        span = now - recent[0]
        return len(recent) / span if span > 0.1 else 0

    @property
    def avg_throughput(self):
        return self.done / self.elapsed if self.elapsed > 1 else 0

    @property
    def eta_throughput(self):
        if len(self._eta_window) < 2:
            return self.current_throughput
        window = list(self._eta_window)
        span = window[-1][0] - window[0][0]
        count = window[-1][1] - window[0][1]
        return count / span if span > 1 else self.current_throughput

    @property
    def eta_seconds(self):
        rate = self.eta_throughput
        return self.remaining / rate if rate > 0 else 0

    @property
    def avg_image_kb(self):
        return (self.total_bytes / self.downloaded / 1024) if self.downloaded else 400

    @property
    def projected_gb(self):
        return self.total_images * self.avg_image_kb / (1024 * 1024)

    @property
    def downloaded_gb(self):
        return self.total_bytes / (1024 ** 3)

    def record_done(self, count: int = 1):
        now = time.monotonic()
        for _ in range(count):
            self._recent_times.append(now)
        self._eta_window.append((now, self.done))
        if now - self._last_tp_update >= 1.0:
            ct = self.current_throughput
            self._throughput_history.append(ct)
            if ct > self.peak_throughput:
                self.peak_throughput = ct
            self._last_tp_update = now

    def sparkline(self) -> str:
        if not self._throughput_history:
            return ""
        bars = " _._._._._"  # simple ASCII sparkline
        bars = " ........X"
        vals = list(self._throughput_history)
        mx = max(vals) if vals and max(vals) > 0 else 1
        chars = []
        blocks = " .oO@"
        for v in vals[-40:]:
            idx = min(4, int(v / mx * 4))
            chars.append(blocks[idx])
        return "".join(chars)

    def add_error(self, err_key: str):
        self.errors_by_type[err_key] = self.errors_by_type.get(err_key, 0) + 1

    def record_year(self, year: str, ok: bool, size: int = 0):
        if ok:
            self.year_ok[year] = self.year_ok.get(year, 0) + 1
            self.year_bytes[year] = self.year_bytes.get(year, 0) + size
        else:
            self.year_err[year] = self.year_err.get(year, 0) + 1


# ── Dashboard ────────────────────────────────────────────────────────────────

def format_duration(s: float) -> str:
    if s <= 0:
        return "--"
    h, m = int(s // 3600), int((s % 3600) // 60)
    return f"{h}h {m:02d}m" if h > 0 else f"{m}m {int(s % 60):02d}s"


def build_dashboard(stats: Stats) -> Panel:
    pct = stats.pct
    bar_w = 50
    filled = int(bar_w * pct / 100)
    bar = f"[green]{'#' * filled}[/green][dim]{'.' * (bar_w - filled)}[/dim]"

    eta_str = format_duration(stats.eta_seconds)
    finish_str = ""
    if stats.eta_seconds > 0:
        finish_time = time.time() + stats.eta_seconds
        finish_str = f"  (finishes ~{time.strftime('%H:%M', time.localtime(finish_time))})"

    phase_str = f"[bold]{stats.phase.upper()}[/bold]"
    if stats.total_phases > 1:
        phase_str += f"  ({stats.phase_index}/{stats.total_phases})"

    method = "Playwright headed" if stats.phase == "labels" else "Playwright direct"

    progress = (
        f"  {phase_str}  [dim]({method}, {stats.concurrent} concurrent)[/dim]\n"
        f"  {bar}  {pct:5.1f}%\n"
        f"  {stats.done:,} / {stats.total_images:,}  |  "
        f"Remaining: {stats.remaining:,}  |  ETA: {eta_str}{finish_str}"
    )

    # Throughput
    tp = Table.grid(padding=(0, 1))
    tp.add_row("[bold]Current:[/bold]", f"{stats.current_throughput:>6.1f} img/s")
    tp.add_row("[bold]Average:[/bold]", f"{stats.avg_throughput:>6.1f} img/s")
    tp.add_row("[bold]ETA rate:[/bold]", f"{stats.eta_throughput:>6.1f} img/s")
    tp.add_row("[bold]Peak:[/bold]", f"{stats.peak_throughput:>6.1f} img/s")
    tp.add_row("", f"[dim]{stats.sparkline()}[/dim]")
    tp_panel = Panel(tp, title="Throughput", border_style="blue", width=34)

    # Downloads
    dp = Table.grid(padding=(0, 1))
    dp.add_row("[bold green]Downloaded:[/bold green]", f"[green]{stats.downloaded:>12,}[/green]")
    if stats.skipped > 0:
        dp.add_row("[bold cyan]Skipped:[/bold cyan]", f"[cyan]{stats.skipped:>12,}[/cyan]")
    dp.add_row("[bold]Size:[/bold]", f"{stats.downloaded_gb:>9.2f} GB")
    dp.add_row("[bold]Avg image:[/bold]", f"{stats.avg_image_kb:>8.1f} KB")
    dp.add_row("[bold]Projected:[/bold]", f"{stats.projected_gb:>9.0f} GB")
    dp.add_row("[bold]Elapsed:[/bold]", f"{format_duration(stats.elapsed):>12}")
    dp_panel = Panel(dp, title="Downloads", border_style="green", width=34)

    # Errors
    ep = Table.grid(padding=(0, 1))
    total_err = stats.errors + stats.html_blocked + stats.too_small + stats.not_image
    err_pct = total_err / max(1, stats.done) * 100
    ep.add_row("[bold]Total errors:[/bold]", f"{total_err:>8,}  [dim]({err_pct:.2f}%)[/dim]")
    if stats.html_blocked > 0:
        ep.add_row("[bold red]WAF/HTML:[/bold red]", f"[red]{stats.html_blocked:>8,}[/red]")
    else:
        ep.add_row("[bold]WAF/HTML:[/bold]", f"[green]{stats.html_blocked:>8,}[/green]")
    ep.add_row("[bold]Too small:[/bold]", f"{stats.too_small:>8,}")
    ep.add_row("[bold]Not image:[/bold]", f"{stats.not_image:>8,}")
    ep.add_row("[bold]Network:[/bold]", f"{stats.errors:>8,}")
    if stats.errors_by_type:
        ep.add_row("", "")
        sorted_errs = sorted(stats.errors_by_type.items(), key=lambda x: -x[1])
        for err_key, cnt in sorted_errs[:5]:
            label = err_key[:28] if len(err_key) > 28 else err_key
            ep.add_row(f"  [dim]{label}[/dim]", f"[dim]{cnt:>6,}[/dim]")
    ep_panel = Panel(ep, title="Errors", border_style="red", width=34)

    # By Year
    all_years = sorted(set(list(stats.year_ok.keys()) + list(stats.year_err.keys())), reverse=True)
    if all_years:
        yt = Table.grid(padding=(0, 1))
        shown = 0
        for year in all_years:
            ok = stats.year_ok.get(year, 0)
            err = stats.year_err.get(year, 0)
            gb = stats.year_bytes.get(year, 0) / (1024 ** 3)
            if ok + err == 0:
                continue
            err_str = f"  [red]err {err}[/red]" if err > 0 else ""
            yt.add_row(f"[bold]{year}[/bold]", f"{ok:>8,} OK", f"{gb:>6.2f} GB", err_str)
            shown += 1
            if shown >= 8:
                if len(all_years) > 8:
                    yt.add_row(f"[dim]... +{len(all_years) - 8} more[/dim]", "", "", "")
                break
        yt_panel = Panel(yt, title="By Year", border_style="yellow")
    else:
        yt_panel = Panel("[dim]No data yet[/dim]", title="By Year", border_style="yellow")

    # Layout
    started = time.strftime("%H:%M:%S", time.localtime(time.time() - stats.elapsed))
    footer = f"  Started: {started}  |  Elapsed: {format_duration(stats.elapsed)}  |  Ctrl+C to stop"

    layout = Table.grid(padding=(1, 0))
    layout.add_row(Text(progress))
    row1 = Table.grid(padding=(0, 2))
    row1.add_row(tp_panel, dp_panel)
    layout.add_row(row1)
    row2 = Table.grid(padding=(0, 2))
    row2.add_row(ep_panel, yt_panel)
    layout.add_row(row2)
    layout.add_row(Text(footer, style="dim"))

    return Panel(layout, title="[bold white] TTB COLA Image Downloader [/bold white]", border_style="bold white")


# ── Helpers ──────────────────────────────────────────────────────────────────

def ttb_id_to_year(ttb_id: str) -> str:
    prefix = ttb_id[:2]
    try:
        num = int(prefix)
        return f"19{prefix}" if num >= 55 else f"20{prefix}"
    except ValueError:
        return "unknown"


def detect_image_format(data: bytes) -> tuple[str, str] | None:
    """Returns (format_name, extension) or None if not a recognized image."""
    for magic, (name, ext) in IMAGE_MAGIC.items():
        if data[:len(magic)] == magic:
            return (name, ext)
    return None


def is_html(data: bytes) -> bool:
    lower = data[:500].lower()
    return b'<html' in lower or b'<!doctype' in lower or b'<script' in lower


def extract_attachment_filename(url: str) -> str | None:
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        filenames = params.get("filename", [])
        if not filenames:
            return None
        raw = unquote(filenames[0]).strip()
        if not raw:
            return None
        safe = UNSAFE_FILENAME_RE.sub("_", raw)
        safe = re.sub(r'[_ ]{2,}', ' ', safe).strip()
        if len(safe) > 180:
            name, ext = os.path.splitext(safe)
            safe = name[:176] + ext
        return safe
    except Exception:
        return None


def get_label_save_path(output_dir: Path, ttb_id: str, url: str,
                        data: bytes, index: int) -> Path:
    """Build the save path for a label image, detecting format from magic bytes."""
    year = ttb_id_to_year(ttb_id)
    ttb_dir = output_dir / "labels" / year / ttb_id

    # Detect actual format
    fmt = detect_image_format(data)
    actual_ext = fmt[1] if fmt else ".bin"

    filename = extract_attachment_filename(url)
    if filename:
        # Replace the extension with the detected one
        name_part, _ = os.path.splitext(filename)
        filename = f"{name_part}{actual_ext}"
    else:
        filename = f"label_{index}{actual_ext}"

    return ttb_dir / filename


def get_scan_save_path(output_dir: Path, ttb_id: str, data: bytes) -> Path:
    """Build the save path for a scan image, detecting format from magic bytes."""
    year = ttb_id_to_year(ttb_id)
    fmt = detect_image_format(data)
    ext = fmt[1] if fmt else ".bin"
    return output_dir / "scans" / year / ttb_id / f"scan{ext}"


def build_existing_set(output_dir: Path, image_type: str, console: Console = None) -> set[str]:
    """Pre-scan disk once and return set of TTB IDs already downloaded.
    Just checks if COLA directory exists (doesn't verify files inside — too slow on USB)."""
    subdir = output_dir / ("labels" if image_type == "labels" else "scans")
    existing = set()
    if not subdir.exists():
        return existing

    if console:
        console.print(f"  Scanning existing downloads on disk...")

    for year_dir in sorted(subdir.iterdir()):
        if not year_dir.is_dir():
            continue
        count = 0
        try:
            for cola_dir in year_dir.iterdir():
                try:
                    if cola_dir.is_dir():
                        existing.add(cola_dir.name)
                        count += 1
                except OSError:
                    continue
        except OSError as e:
            if console:
                console.print(f"    [red]{year_dir.name}: scan error: {e}[/red]")
            continue
        if console and count > 0:
            console.print(f"    {year_dir.name}: {count:,} COLAs on disk")

    if console:
        console.print(f"  [cyan]Total on disk: {len(existing):,} COLAs[/cyan]")

    return existing


def check_label_exists(output_dir: Path, ttb_id: str) -> bool:
    """Check if this COLA already has downloaded label images."""
    year = ttb_id_to_year(ttb_id)
    ttb_dir = output_dir / "labels" / year / ttb_id
    return ttb_dir.exists()


def check_scan_exists(output_dir: Path, ttb_id: str) -> bool:
    """Check if this COLA already has a downloaded scan."""
    year = ttb_id_to_year(ttb_id)
    ttb_dir = output_dir / "scans" / year / ttb_id
    return ttb_dir.exists()


# ── DB Fetch ─────────────────────────────────────────────────────────────────

def fetch_records(image_type: str, year_min: int | None, year_max: int | None,
                  limit: int | None, console: Console,
                  logger: logging.Logger = None) -> list[dict]:
    """Fetch records from source_ttb_colas that have image URLs."""
    column = "label_image_urls" if image_type == "labels" else "application_scan_urls"
    sb = get_supabase()

    start_year = year_min or 2003  # No label images before 2003
    end_year = year_max or 2026

    if logger:
        logger.info(f"FETCH START: {column}, years {start_year}-{end_year}")

    all_records = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            m_start = f"{year}-{month:02d}-01"
            if month == 12:
                m_end = f"{year}-12-31"
            else:
                m_end = f"{year}-{month + 1:02d}-01"
            offset = 0
            retries = 0
            while True:
                try:
                    q = sb.table("source_ttb_colas") \
                        .select(f"ttb_id, {column}") \
                        .gte("completed_date", m_start) \
                        .not_.is_(column, "null") \
                        .range(offset, offset + 499)
                    if month == 12:
                        q = q.lte("completed_date", m_end)
                    else:
                        q = q.lt("completed_date", m_end)
                    result = q.execute()
                    retries = 0
                except Exception as e:
                    retries += 1
                    msg = f"Error {year}-{month:02d} offset={offset}: {type(e).__name__}: {str(e)[:200]}"
                    console.print(f"  [red]{msg}[/red]")
                    if logger:
                        logger.error(msg)
                    if retries < 3:
                        import time
                        time.sleep(5)
                        continue
                    if logger:
                        logger.error(f"Giving up on {year}-{month:02d} after 3 retries")
                    break

                if not result.data:
                    break

                batch = [r for r in result.data
                         if r.get(column) and len(r[column]) > 0]
                all_records.extend(batch)

                if len(result.data) < 500:
                    break
                offset += 500

        if all_records:
            console.print(f"  {year}: {len(all_records):,} records so far")

        if limit and len(all_records) >= limit:
            all_records = all_records[:limit]
            break

    if logger:
        logger.info(f"FETCH COMPLETE: {len(all_records):,} records")

    return all_records


# ── Label Download (Playwright Headed) ───────────────────────────────────────

async def download_labels(records: list[dict], output_dir: Path, concurrent: int,
                          stats: Stats, console: Console, live: Live,
                          logger: logging.Logger = None,
                          existing_set: set[str] = None):
    """Download label images using headed Playwright.

    Shape Security WAF blocks headless browsers and direct HTTP requests to
    publicViewAttachment.do. The only working approach is to navigate a real
    (headed) browser to the printable form page, which loads label images as
    <img> sub-resources that we intercept.
    """
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(ignore_https_errors=True)

        # Establish session by visiting the public search page
        setup_page = await context.new_page()
        console.print("[dim]  Establishing browser session...[/dim]")
        await setup_page.goto(
            "https://ttbonline.gov/colasonline/publicSearchColasBasic.do",
            wait_until="networkidle"
        )
        await asyncio.sleep(3)  # Let Shape Security JS run
        await setup_page.close()
        console.print("[green]  Session established[/green]")

        # Process records using a semaphore for concurrency control
        sem = asyncio.Semaphore(concurrent)

        async def process_record(rec: dict):
            ttb_id = rec["ttb_id"]
            expected_urls = rec.get("label_image_urls", [])
            year = ttb_id_to_year(ttb_id)

            # Resume: skip if already downloaded
            if check_label_exists(output_dir, ttb_id):
                stats.skipped += len(expected_urls)
                stats.record_done(len(expected_urls))
                live.update(build_dashboard(stats))
                return

            popup_url = (
                f"https://ttbonline.gov/colasonline/viewColaDetails.do"
                f"?action=publicFormDisplay&ttbid={ttb_id}"
            )

            async with sem:
                pg = await context.new_page()
                captured = []

                async def on_response(response):
                    url = response.url
                    if 'publicViewAttachment' not in url:
                        return
                    try:
                        body = await response.body()
                        fmt = detect_image_format(body)
                        if fmt and len(body) > 500:
                            captured.append((url, body))
                    except Exception:
                        pass

                pg.on("response", on_response)

                try:
                    await pg.goto(popup_url, wait_until="networkidle", timeout=20000)
                except Exception as e:
                    err_name = str(type(e).__name__)[:28]
                    stats.errors += 1
                    stats.add_error(err_name)
                    stats.record_year(year, False)
                    if logger:
                        logger.warning(f"NAV_ERR {ttb_id}: {err_name}: {str(e)[:200]}")

                pg.remove_listener("response", on_response)
                await pg.close()

                # Save captured images
                if captured:
                    for i, (url, body) in enumerate(captured):
                        try:
                            save_path = get_label_save_path(output_dir, ttb_id, url, body, i)
                            save_path.parent.mkdir(parents=True, exist_ok=True)
                            save_path.write_bytes(body)
                            stats.downloaded += 1
                            stats.total_bytes += len(body)
                            stats.record_year(year, True, len(body))
                        except OSError as e:
                            stats.errors += 1
                            stats.add_error(f"OS: {getattr(e, 'strerror', 'write')}"[:28])
                            stats.record_year(year, False)
                            if logger:
                                logger.warning(f"SAVE_ERR {ttb_id}: {e}")
                else:
                    # No images captured — count each expected URL as an error
                    for _ in expected_urls:
                        stats.not_image += 1
                        stats.add_error("No attachment loaded")
                        stats.record_year(year, False)
                    if logger:
                        logger.warning(f"NO_IMG {ttb_id}: {len(expected_urls)} expected URLs, 0 captured")

                stats.record_done(max(len(captured), len(expected_urls)))
                live.update(build_dashboard(stats))

        # Process all records with bounded concurrency
        tasks = [process_record(rec) for rec in records]

        # Process in chunks to avoid overwhelming the browser
        chunk_size = concurrent * 4
        for i in range(0, len(tasks), chunk_size):
            chunk = tasks[i:i + chunk_size]
            await asyncio.gather(*chunk, return_exceptions=True)

        await browser.close()


# ── Scan Download (Playwright Headed) ────────────────────────────────────────

async def download_scans(records: list[dict], output_dir: Path, concurrent: int,
                         stats: Stats, console: Console, live: Live,
                         logger: logging.Logger = None,
                         existing_set: set[str] = None):
    """Download scan images using headed Playwright with direct navigation.

    publicViewImage.do is also WAF-protected. We navigate to each image URL
    directly — Chrome handles the Shape Security challenge, then we capture
    the response body.
    """
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(ignore_https_errors=True)

        # Establish session
        setup_page = await context.new_page()
        console.print("[dim]  Establishing browser session...[/dim]")
        await setup_page.goto(
            "https://ttbonline.gov/colasonline/publicSearchColasBasic.do",
            wait_until="networkidle"
        )
        await asyncio.sleep(3)
        await setup_page.close()
        console.print("[green]  Session established[/green]")

        # Build flat list of (ttb_id, url) pairs
        tasks = []
        for rec in records:
            ttb_id = rec["ttb_id"]
            urls = rec.get("application_scan_urls", [])
            for url in urls:
                tasks.append((ttb_id, url))

        stats.total_images = len(tasks)

        sem = asyncio.Semaphore(concurrent)

        async def download_one(ttb_id: str, url: str):
            year = ttb_id_to_year(ttb_id)

            # Resume: skip if already downloaded
            if check_scan_exists(output_dir, ttb_id):
                stats.skipped += 1
                stats.record_done()
                live.update(build_dashboard(stats))
                return

            async with sem:
                pg = await context.new_page()
                try:
                    resp = await pg.goto(url, wait_until="load", timeout=20000)
                    if resp is None:
                        stats.errors += 1
                        stats.add_error("No response")
                        stats.record_year(year, False)
                        stats.record_done()
                        live.update(build_dashboard(stats))
                        await pg.close()
                        return

                    data = await resp.body()

                    # Check for HTML (WAF block or error page)
                    if is_html(data):
                        stats.html_blocked += 1
                        stats.add_error("WAF/HTML")
                        stats.record_year(year, False)
                        if logger:
                            logger.warning(f"WAF {ttb_id}: HTML response ({len(data)} bytes)")
                        stats.record_done()
                        live.update(build_dashboard(stats))
                        await pg.close()
                        return

                    # Check if it's a real image
                    fmt = detect_image_format(data)
                    if not fmt:
                        stats.not_image += 1
                        stats.add_error("Not image")
                        stats.record_year(year, False)
                        if logger:
                            logger.warning(f"NOT_IMG {ttb_id}: magic={data[:8].hex()} ({len(data)} bytes)")
                        stats.record_done()
                        live.update(build_dashboard(stats))
                        await pg.close()
                        return

                    # Check minimum size
                    if len(data) < 500:
                        stats.too_small += 1
                        stats.add_error("Too small")
                        stats.record_year(year, False)
                        stats.record_done()
                        live.update(build_dashboard(stats))
                        await pg.close()
                        return

                    # Save
                    save_path = get_scan_save_path(output_dir, ttb_id, data)
                    save_path.parent.mkdir(parents=True, exist_ok=True)
                    save_path.write_bytes(data)

                    stats.downloaded += 1
                    stats.total_bytes += len(data)
                    stats.record_year(year, True, len(data))

                except Exception as e:
                    err_name = str(type(e).__name__)[:28]
                    stats.errors += 1
                    stats.add_error(err_name)
                    stats.record_year(year, False)
                    if logger:
                        logger.warning(f"SCAN_ERR {ttb_id}: {err_name}: {str(e)[:200]}")

                finally:
                    await pg.close()

                stats.record_done()
                live.update(build_dashboard(stats))

        # Process in chunks
        chunk_size = concurrent * 4
        all_coros = [download_one(tid, url) for tid, url in tasks]
        for i in range(0, len(all_coros), chunk_size):
            chunk = all_coros[i:i + chunk_size]
            await asyncio.gather(*chunk, return_exceptions=True)

        await browser.close()


# ── Main ─────────────────────────────────────────────────────────────────────

async def run_phase(image_type: str, output_dir: Path, args, console: Console,
                    phase_index: int, total_phases: int, logger: logging.Logger = None):
    column = "label_image_urls" if image_type == "labels" else "application_scan_urls"
    concurrent = args.concurrent or (
        DEFAULT_LABEL_CONCURRENT if image_type == "labels" else DEFAULT_SCAN_CONCURRENT
    )

    console.print(f"\n[bold]Phase {phase_index}/{total_phases}: {image_type.upper()}[/bold]")
    console.print(f"  Method: {'Playwright headed' if image_type == 'labels' else 'Playwright direct'}")
    console.print(f"  Concurrent: {concurrent}")
    console.print(f"  Fetching records from DB...")

    records = fetch_records(image_type, args.year_min, args.year_max, args.limit, console, logger)
    console.print(f"  Found {len(records):,} records with {column}")

    if not records:
        console.print("  [yellow]No records found. Skipping.[/yellow]")
        return

    # Count total URLs
    total_urls = sum(len(r.get(column) or []) for r in records)
    console.print(f"  Total image URLs: {total_urls:,}")

    # Skip expensive pre-scan on USB drives — resume check happens per-COLA during download
    existing_set = None
    console.print(f"  [dim]Resume check will happen per-COLA during download[/dim]")

    est_gb = total_urls * 400 / (1024 * 1024)  # ~400KB average
    console.print(f"  Estimated total size: ~{est_gb:.0f} GB")

    if logger:
        logger.info(f"Phase {phase_index}/{total_phases}: {image_type.upper()}")
        logger.info(f"  Records: {len(records):,}, URLs: {total_urls:,}, "
                     f"Concurrent: {concurrent}")

    stats = Stats(
        total_images=total_urls,
        start_time=time.monotonic(),
        phase=image_type,
        phase_index=phase_index,
        total_phases=total_phases,
        concurrent=concurrent,
    )

    # Periodic log snapshot task
    async def log_snapshot_loop():
        while True:
            await asyncio.sleep(300)  # every 5 minutes
            if logger:
                log_stats_snapshot(logger, stats)

    console.print()
    try:
        with Live(build_dashboard(stats), console=console, refresh_per_second=2) as live:
            log_task = asyncio.create_task(log_snapshot_loop())
            try:
                if image_type == "labels":
                    await download_labels(records, output_dir, concurrent, stats, console, live, logger, existing_set)
                else:
                    await download_scans(records, output_dir, concurrent, stats, console, live, logger, existing_set)
            finally:
                log_task.cancel()
                try:
                    await log_task
                except asyncio.CancelledError:
                    pass
            # Final dashboard update
            live.update(build_dashboard(stats))
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted![/yellow]")

    # Final log
    if logger:
        logger.info(f"Phase {image_type.upper()} complete")
        log_stats_snapshot(logger, stats)

    # Print final summary
    console.print()
    console.print(build_dashboard(stats))


def setup_logging(output_dir: Path):
    """Set up file logging to output_dir/download.log"""
    log_file = output_dir / "download.log"
    logger = logging.getLogger("ttb_downloader")
    logger.setLevel(logging.INFO)
    # Remove existing handlers
    logger.handlers.clear()
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(fh)
    return logger


def log_stats_snapshot(logger: logging.Logger, stats: Stats):
    """Write a periodic stats line to the log file."""
    total_err = stats.errors + stats.html_blocked + stats.too_small + stats.not_image
    err_pct = total_err / max(1, stats.done) * 100
    logger.info(
        f"[{stats.phase.upper()}] "
        f"{stats.done:,}/{stats.total_images:,} ({stats.pct:.1f}%) | "
        f"OK {stats.downloaded:,} | Skip {stats.skipped:,} | "
        f"Err {total_err:,} ({err_pct:.2f}%) "
        f"[WAF:{stats.html_blocked} NoImg:{stats.not_image} Net:{stats.errors}] | "
        f"{stats.downloaded_gb:.2f} GB | "
        f"{stats.current_throughput:.1f} img/s avg {stats.avg_throughput:.1f} img/s | "
        f"ETA {format_duration(stats.eta_seconds)}"
    )
    if stats.errors_by_type:
        for err_key, cnt in sorted(stats.errors_by_type.items(), key=lambda x: -x[1])[:5]:
            logger.info(f"  error: {err_key}: {cnt:,}")


async def async_main():
    parser = argparse.ArgumentParser(description="Download TTB COLA label images")
    parser.add_argument("--type", choices=["labels", "scans", "all"], required=True,
                        help="Image type to download")
    parser.add_argument("--year-min", type=int, help="Minimum completed_date year")
    parser.add_argument("--year-max", type=int, help="Maximum completed_date year")
    parser.add_argument("--limit", type=int, help="Max records to process")
    parser.add_argument("--concurrent", "-c", type=int,
                        help=f"Concurrent downloads (default: {DEFAULT_LABEL_CONCURRENT} labels, "
                             f"{DEFAULT_SCAN_CONCURRENT} scans)")
    parser.add_argument("--output-dir", type=str, default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger = setup_logging(output_dir)
    logger.info("=" * 60)
    logger.info(f"TTB COLA Image Downloader started")
    logger.info(f"  Output: {output_dir}")
    logger.info(f"  Type: {args.type}, Concurrent: {args.concurrent or 'default'}")
    logger.info(f"  Year range: {args.year_min or 'all'}-{args.year_max or 'all'}")

    console = Console()
    console.print("[bold white]TTB COLA Image Downloader[/bold white]")
    console.print(f"  Output: {output_dir}")
    console.print(f"  Log: {output_dir / 'download.log'}")

    types_to_run = ["labels", "scans"] if args.type == "all" else [args.type]

    try:
        for i, image_type in enumerate(types_to_run):
            await run_phase(image_type, output_dir, args, console, i + 1, len(types_to_run), logger)

        logger.info("All downloads complete!")
        console.print("\n[bold green]All downloads complete![/bold green]")
    except KeyboardInterrupt:
        logger.error("INTERRUPTED by user (Ctrl+C)")
        console.print("\n[yellow]Interrupted![/yellow]")
    except Exception as e:
        logger.error(f"FATAL CRASH: {type(e).__name__}: {str(e)[:500]}")
        import traceback
        logger.error(traceback.format_exc())
        console.print(f"\n[red]FATAL: {e}[/red]")
        raise


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
