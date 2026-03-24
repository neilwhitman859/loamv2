#!/usr/bin/env python3
"""
TTB COLA Chrome-Based Scraper

Runs a local HTTP server that receives extracted data from JavaScript
running inside the user's Chrome browser tab. The browser's native
fetch() bypasses Shape Security WAF completely.

Architecture:
  1. This script starts a local server on port 8765
  2. JavaScript injected into a Chrome tab batch-fetches TTB detail pages
  3. JS extracts fields and POSTs results to localhost:8765
  4. This script writes to Supabase source_ttb_colas

Usage:
    python -m pipeline.fetch.ttb_chrome_scraper [options]

    Then inject the JavaScript into a Chrome tab that has TTB open.
"""

import argparse
import asyncio
import json
import os
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.lib.db import get_supabase

# ─── Constants ───────────────────────────────────────────────────────────────

DEFAULT_OUTPUT = Path.home() / "Desktop" / "Loam Cowork" / "data" / "imports" / "ttb_cola_labels"
DB_BATCH_SIZE = 100
SERVER_PORT = 8765

# ─── Stats ───────────────────────────────────────────────────────────────────

@dataclass
class Stats:
    total_records: int = 0
    completed: int = 0
    errors: int = 0
    waf_blocks: int = 0

    grapes_found: int = 0
    appellations_found: int = 0
    vintages_found: int = 0
    abv_found: int = 0
    applicant_found: int = 0
    fields_extracted: int = 0
    images_found: int = 0

    start_time: float = 0.0
    _recent_times: deque = field(default_factory=lambda: deque(maxlen=500))
    _throughput_history: deque = field(default_factory=lambda: deque(maxlen=60))
    _last_throughput_update: float = 0.0
    peak_throughput: float = 0.0
    recent: deque = field(default_factory=lambda: deque(maxlen=8))

    era_totals: dict = field(default_factory=dict)
    era_done: dict = field(default_factory=dict)

    batches_received: int = 0
    js_errors: int = 0

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
    def eta_seconds(self):
        rate = self.avg_throughput
        if rate <= 0: return 0
        return (self.total_records - self.completed) / rate

    def record_batch(self, count: int):
        now = time.monotonic()
        for _ in range(count):
            self._recent_times.append(now)
        self.batches_received += 1
        if now - self._last_throughput_update >= 1.0:
            ct = self.current_throughput
            self._throughput_history.append(ct)
            if ct > self.peak_throughput: self.peak_throughput = ct
            self._last_throughput_update = now

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

def build_dashboard(stats: Stats) -> Panel:
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
    dp.add_row("[bold]ABV:[/bold]", f"{stats.abv_found:>12,}")
    dp.add_row("[bold]Applicant:[/bold]", f"{stats.applicant_found:>12,}")
    dp.add_row("[bold]Images:[/bold]", f"{stats.images_found:>12,}")
    ext_pct = (stats.fields_extracted / stats.completed * 100) if stats.completed else 0
    dp.add_row("[bold]Has data:[/bold]", f"{stats.fields_extracted:>8,}  [dim]({ext_pct:.1f}%)[/dim]")
    dp_panel = Panel(dp, title="Extracted", border_style="magenta", width=34)

    np_t = Table.grid(padding=(0, 1))
    np_t.add_row("[bold]Batches:[/bold]", f"{stats.batches_received:>8,}")
    err_pct = (stats.errors / stats.completed * 100) if stats.completed else 0
    np_t.add_row("[bold]Errors:[/bold]", f"{stats.errors:>8,}  [dim]({err_pct:.3f}%)[/dim]")
    if stats.waf_blocks > 0:
        np_t.add_row("[bold red]WAF blocks:[/bold red]", f"[red]{stats.waf_blocks:>8,}[/red]")
    np_t.add_row("[bold]JS errors:[/bold]", f"{stats.js_errors:>8,}")
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
        data_str = "[green]✓[/green]" if e.get("has_data") else "[dim]—[/dim]"
        rec_t.add_row(f"[dim]{e.get('ttb_id','')}[/dim]", f"{e.get('brand','')[:30]}", data_str)
    rec_panel = Panel(rec_t, title="Recent", border_style="dim")

    elapsed_str = format_duration(stats.elapsed)
    started = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time() - stats.elapsed))
    footer = f"  Started: {started}  |  Elapsed: {elapsed_str}  |  Batches: {stats.batches_received}  |  Ctrl+C to stop"

    layout = Table.grid(padding=(1, 0))
    layout.add_row(Text(progress, style="bold"))
    row1 = Table.grid(padding=(0, 2))
    row1.add_row(tp_panel, dp_panel)
    layout.add_row(row1)
    layout.add_row(np_panel)
    layout.add_row(era_panel)
    layout.add_row(rec_panel)
    layout.add_row(Text(footer, style="dim"))

    return Panel(layout, title="[bold white] TTB COLA Chrome Scraper [/bold white]", border_style="bold white")


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

    def add_batch(self, ids: list[str]):
        for ttb_id in ids:
            self.completed.add(ttb_id)
            if self._file:
                self._file.write(ttb_id + "\n")

    def close(self):
        if self._file:
            self._file.flush()
            self._file.close()

    def __contains__(self, ttb_id): return ttb_id in self.completed


# ─── DB Writer ───────────────────────────────────────────────────────────────

class DbWriter:
    def __init__(self):
        self._sb = None
        self.total_written = 0
        self.write_errors = 0
        self._lock = threading.Lock()

    def _get_sb(self):
        if self._sb is None:
            self._sb = get_supabase()
        return self._sb

    def write_batch(self, rows: list[dict]):
        with self._lock:
            try:
                sb = self._get_sb()
                sb.table("source_ttb_colas").upsert(
                    rows, on_conflict="ttb_id"
                ).execute()
                self.total_written += len(rows)
            except Exception:
                self.write_errors += 1
                # Retry one by one
                sb = self._get_sb()
                for row in rows:
                    try:
                        sb.table("source_ttb_colas").upsert(
                            row, on_conflict="ttb_id"
                        ).execute()
                        self.total_written += 1
                    except Exception:
                        self.write_errors += 1


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


# ─── HTTP Server ─────────────────────────────────────────────────────────────

def make_handler(stats, checkpoint, db_writer, record_queue, queue_lock):
    """Create HTTP handler with access to shared state."""

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass  # Suppress request logging

        def do_OPTIONS(self):
            """Handle CORS preflight."""
            self.send_response(200)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()

        def do_GET(self):
            """Serve the next batch of TTB IDs to scrape."""
            if self.path == "/batch":
                with queue_lock:
                    batch = []
                    while len(batch) < 50 and record_queue:
                        batch.append(record_queue.pop(0))

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps(batch).encode())

            elif self.path == "/stats":
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "completed": stats.completed,
                    "total": stats.total_records,
                    "remaining": len(record_queue),
                }).encode())

            else:
                self.send_response(404)
                self.end_headers()

        def do_POST(self):
            """Receive extracted data from Chrome JS."""
            if self.path == "/results":
                content_length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_length)

                try:
                    data = json.loads(body)
                    results = data.get("results", [])
                    errors = data.get("errors", [])

                    # Build DB rows
                    db_rows = []
                    checkpoint_ids = []

                    for r in results:
                        ttb_id = r.get("ttb_id")
                        if not ttb_id:
                            continue

                        row = {
                            "ttb_id": ttb_id,
                            "detail_scraped_at": "now()",
                        }

                        fields = r.get("fields", {})
                        if fields.get("grape_varietals"):
                            row["grape_varietals"] = fields["grape_varietals"]
                            stats.grapes_found += 1
                        if fields.get("wine_appellation"):
                            row["wine_appellation"] = fields["wine_appellation"]
                            stats.appellations_found += 1
                        if fields.get("wine_vintage"):
                            row["wine_vintage"] = fields["wine_vintage"]
                            stats.vintages_found += 1
                        if fields.get("abv"):
                            row["abv"] = fields["abv"]
                            stats.abv_found += 1
                        if fields.get("phone"):
                            row["phone"] = fields["phone"]
                        if fields.get("email"):
                            row["email"] = fields["email"]
                        if fields.get("applicant_name"):
                            row["applicant_name"] = fields["applicant_name"]
                            stats.applicant_found += 1
                        if fields.get("applicant_address"):
                            row["applicant_address"] = fields["applicant_address"]
                        if fields.get("applicant_city"):
                            row["applicant_city"] = fields["applicant_city"]
                        if fields.get("applicant_state"):
                            row["applicant_state"] = fields["applicant_state"]
                        if fields.get("applicant_zip"):
                            row["applicant_zip"] = fields["applicant_zip"]
                        if fields.get("qualifications"):
                            row["qualifications"] = fields["qualifications"]
                        if fields.get("image_ids"):
                            row["label_image_urls"] = [
                                f"https://ttbonline.gov/colasonline/publicViewImage.do?id={img_id}"
                                for img_id in fields["image_ids"]
                            ]
                            stats.images_found += len(fields["image_ids"])

                        has_data = len(row) > 2  # More than ttb_id + detail_scraped_at
                        if has_data:
                            stats.fields_extracted += 1

                        db_rows.append(row)
                        checkpoint_ids.append(ttb_id)

                        era = r.get("era", "2020+")
                        stats.era_done[era] = stats.era_done.get(era, 0) + 1
                        stats.recent.appendleft({
                            "ttb_id": ttb_id,
                            "brand": r.get("brand", "")[:30],
                            "has_data": has_data,
                        })

                    # Write to DB
                    if db_rows:
                        db_writer.write_batch(db_rows)
                        checkpoint.add_batch(checkpoint_ids)

                    stats.completed += len(results)
                    stats.errors += len(errors)
                    stats.record_batch(len(results))

                    if errors:
                        stats.js_errors += len(errors)
                        for e in errors:
                            if "waf" in str(e).lower():
                                stats.waf_blocks += 1

                except Exception as e:
                    stats.errors += 1

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": True}).encode())

            else:
                self.send_response(404)
                self.end_headers()

    return Handler


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="TTB COLA Chrome-Based Scraper")
    parser.add_argument("--output-dir", type=str, default=str(DEFAULT_OUTPUT))
    parser.add_argument("--start-year", type=int, default=2005)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--refresh-list", action="store_true")
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--rebuild-checkpoint", action="store_true",
                        help="Rebuild checkpoint from DB — skips already-scraped records")
    parser.add_argument("--port", type=int, default=SERVER_PORT)
    parser.add_argument("--concurrency", type=int, default=20,
                        help="JS fetch concurrency (default: 20)")
    args = parser.parse_args()

    if sys.platform == "win32":
        import codecs
        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, errors="replace")
        sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, errors="replace")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    console = Console()

    # ── Record list ──
    console.print(f"\n[bold]Fetching record list (since {args.start_year})...[/bold]")
    cache_file = output_dir / "record_list.json"
    if cache_file.exists() and not args.refresh_list:
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

    # ── Checkpoint ──
    checkpoint_file = output_dir / "checkpoint_v3.txt"
    checkpoint = Checkpoint(checkpoint_file)
    checkpoint.load()
    already_done = len(checkpoint.completed)

    if args.rebuild_checkpoint:
        console.print(f"  [yellow]Rebuilding checkpoint from DB (old: {already_done:,})...[/yellow]")
        sb = get_supabase()
        real_done = set()
        current_year = time.localtime().tm_year
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
                              .range(offset, offset + 1000 - 1)
                              .execute())
                except Exception as e:
                    console.print(f"    Retry {year} offset {offset}: {str(e)[:60]}")
                    time.sleep(2)
                    continue
                if not result.data: break
                for row in result.data:
                    real_done.add(row["ttb_id"])
                year_count += len(result.data)
                if len(result.data) < 1000: break
                offset += 1000
            if year_count > 0:
                console.print(f"    {year}: {year_count:,} verified ({len(real_done):,} total)")
        checkpoint.close()
        with open(checkpoint_file, "w") as f:
            for ttb_id in sorted(real_done):
                f.write(ttb_id + "\n")
        checkpoint = Checkpoint(checkpoint_file)
        checkpoint.load()
        already_done = len(checkpoint.completed)
        console.print(f"  [green]Checkpoint rebuilt: {already_done:,} verified records[/green]")

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

    # ── Build queue ──
    record_queue = [
        {"ttb_id": r["ttb_id"], "brand": r.get("brand_name", ""), "era": r["era_name"]}
        for r in remaining
    ]
    queue_lock = threading.Lock()

    db_writer = DbWriter()

    # ── Logging ──
    log_file = output_dir / "scraper.log"
    status_file = output_dir / "status.json"
    log_fh = open(log_file, "a", buffering=1, encoding="utf-8")

    def log(msg: str):
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {msg}"
        log_fh.write(line + "\n")

    def write_status():
        status = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "completed": stats.completed,
            "total": stats.total_records,
            "remaining": len(record_queue),
            "pct": round(stats.completed / stats.total_records * 100, 2) if stats.total_records else 0,
            "avg_rec_per_sec": round(stats.avg_throughput, 1),
            "current_rec_per_sec": round(stats.current_throughput, 1),
            "peak_rec_per_sec": round(stats.peak_throughput, 1),
            "eta_hours": round(stats.eta_seconds / 3600, 1) if stats.eta_seconds > 0 else 0,
            "grapes": stats.grapes_found,
            "appellations": stats.appellations_found,
            "vintages": stats.vintages_found,
            "abv": stats.abv_found,
            "applicants": stats.applicant_found,
            "images": stats.images_found,
            "fields_extracted": stats.fields_extracted,
            "errors": stats.errors,
            "waf_blocks": stats.waf_blocks,
            "batches": stats.batches_received,
            "db_writes": db_writer.total_written,
            "db_errors": db_writer.write_errors,
            "elapsed_hours": round(stats.elapsed / 3600, 2),
        }
        status_file.write_text(json.dumps(status, indent=2), encoding="utf-8")

    log(f"Scraper started. {len(remaining):,} records to process.")

    # ── Start HTTP server ──
    HandlerClass = make_handler(stats, checkpoint, db_writer, record_queue, queue_lock)
    server = HTTPServer(("127.0.0.1", args.port), HandlerClass)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    console.print(f"[bold green]Server running on http://localhost:{args.port}[/bold green]")
    console.print(f"[bold]Concurrency: {args.concurrency}[/bold]")
    console.print(f"[bold]Log: {log_file}[/bold]")
    console.print(f"[bold]Status: {status_file}[/bold]")
    console.print()
    console.print("[yellow]Now inject the JavaScript into your Chrome tab on TTB.[/yellow]")
    console.print("[yellow]Open DevTools (F12) → Console → paste contents of:[/yellow]")
    console.print(f"[yellow]  pipeline/fetch/ttb_chrome_inject.js[/yellow]\n")

    log(f"Server running on port {args.port}")

    # ── Dashboard + logging loop ──
    last_log_time = time.monotonic()
    last_completed = stats.completed
    try:
        with Live(build_dashboard(stats), console=console, refresh_per_second=2) as live:
            while stats.completed < stats.total_records:
                time.sleep(0.5)
                live.update(build_dashboard(stats))

                # Log every 30 seconds
                now = time.monotonic()
                if now - last_log_time >= 30:
                    new_records = stats.completed - last_completed
                    rate = new_records / (now - last_log_time)
                    log(f"Progress: {stats.completed:,}/{stats.total_records:,} ({stats.completed/stats.total_records*100:.1f}%) | {rate:.1f} rec/s | grapes={stats.grapes_found:,} app={stats.appellations_found:,} vint={stats.vintages_found:,} abv={stats.abv_found:,} img={stats.images_found:,} | errors={stats.errors} waf={stats.waf_blocks}")
                    write_status()
                    last_log_time = now
                    last_completed = stats.completed

                    # Stall detection: warn if no progress in 5 minutes
                    if new_records == 0 and stats.batches_received > 0:
                        log("WARNING: No progress in last 30s — JS may have stopped or WAF blocked")

    except KeyboardInterrupt:
        console.print("\n[yellow]Stopping...[/yellow]")
        log("Scraper stopped by user (Ctrl+C)")

    server.shutdown()
    checkpoint.close()
    write_status()
    log(f"Final: {stats.completed:,}/{stats.total_records:,} processed. Grapes={stats.grapes_found:,} App={stats.appellations_found:,} Vint={stats.vintages_found:,}")
    log_fh.close()

    console.print(f"\n[bold green]Done![/bold green]")
    console.print(f"  Processed: {stats.completed:,} / {stats.total_records:,}")
    console.print(f"  Grapes: {stats.grapes_found:,}  |  Appellations: {stats.appellations_found:,}  |  Vintages: {stats.vintages_found:,}")
    console.print(f"  ABV: {stats.abv_found:,}  |  Applicants: {stats.applicant_found:,}  |  Images: {stats.images_found:,}")
    console.print(f"  DB writes: {db_writer.total_written:,} ({db_writer.write_errors} errors)")
    console.print(f"  Log: {log_file}")
    console.print(f"  Status: {status_file}")


if __name__ == "__main__":
    main()
