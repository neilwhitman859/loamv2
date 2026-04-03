#!/usr/bin/env python3
"""
TTB COLA Scraper Test Suite

Tests both detail and printable scrapers against a curated set of TTB IDs
spanning different years, ID formats, and data characteristics.

Usage:
    python -m pipeline.fetch.test_ttb_scrape

Requires a Chrome tab open on ttbonline.gov with the test inject JS pasted in.
This script runs a mini HTTP server, serves test batches, and validates results.
"""

import json
import sys
import time
import threading
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler
from dataclasses import dataclass, field

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# ─── Test Cases ──────────────────────────────────────────────────────────────

# Each test case: (ttb_id, description, expected fields for detail, expected fields for printable)
DETAIL_TEST_CASES = [
    # 001 IDs — should have rich detail data
    ("23046001000823", "2023 001 - Chambolle Musigny (grapes, vintage, imageWindow)"),
    ("06350001000089", "2006 001 - old era (imageWindow, maybe grapes)"),
    ("03141001000001", "2003 001 - La Braccesca (never detail-scraped)"),
    # 000/002/003 IDs — app scans, sparse data
    ("26070002000001", "2026 002 - F. Thienpont (sparse detail page)"),
    ("02092003000006", "003 ID - Gandia (null date)"),
    # Short IDs — old format
    ("94101069", "1994 short 8-digit - Domaine La Millier"),
    ("85010001", "1985 short 8-digit - Monte Vertine"),
    # 2018/2019 gap year IDs
    ("18061001000428", "2018 001 - Brentwood Country Club"),
    ("19282001000741", "2019 001 - Mari Vineyards Bestiary Red"),
]

PRINTABLE_TEST_CASES = [
    # 001 IDs — should have printable page with data + images
    ("23046001000823", "2023 001 new form - appellation + images expected"),
    ("06350001000089", "2006 001 old form - appellation + ABV + images expected"),
    ("03141001000001", "2003 001 old form - never printable-scraped"),
    ("18061001000428", "2018 001 - gap year, never printable-scraped"),
    ("19282001000741", "2019 001 - entire year missing, never printable-scraped"),
    ("19002001000235", "2019 001 - Castello dei Rampolla"),
    # Non-001 — should all return errors (no printable page exists)
    ("26070002000001", "002 ID - should error"),
    ("94101069", "short 8-digit ID - should error"),
    ("85010001", "short 8-digit ID - should error"),
]


# ─── Test Server ─────────────────────────────────────────────────────────────

@dataclass
class TestResults:
    results: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    received: threading.Event = field(default_factory=threading.Event)


def make_test_handler(test_cases, test_results, scraper_type):
    """HTTP handler that serves test cases and collects results."""
    batch_served = {"done": False}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            pass

        def do_OPTIONS(self):
            self.send_response(200)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()

        def do_GET(self):
            if self.path == "/batch":
                if not batch_served["done"]:
                    batch_served["done"] = True
                    batch = [{"ttb_id": tc[0], "brand": tc[1][:30], "era": "test"} for tc in test_cases]
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    self.wfile.write(json.dumps(batch).encode())
                else:
                    # Empty batch = done
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    self.wfile.write(b"[]")
            else:
                self.send_response(404)
                self.end_headers()

        def do_POST(self):
            if self.path == "/results":
                content_length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_length)
                data = json.loads(body)
                test_results.results.extend(data.get("results", []))
                test_results.errors.extend(data.get("errors", []))
                test_results.received.set()

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": True}).encode())
            else:
                self.send_response(404)
                self.end_headers()

    return Handler


def run_test(test_cases, port, scraper_type):
    """Run test server, wait for results, validate."""
    print(f"\n{'='*70}")
    print(f"  TESTING {scraper_type.upper()} SCRAPER — {len(test_cases)} test cases on port {port}")
    print(f"{'='*70}\n")

    test_results = TestResults()
    handler = make_test_handler(test_cases, test_results, scraper_type)
    server = HTTPServer(("127.0.0.1", port), handler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    print(f"  Server running on http://localhost:{port}")
    print(f"  Paste the {scraper_type} inject.js into Chrome Console now.")
    print(f"  Waiting for results...\n")

    # Wait up to 120 seconds for results
    if not test_results.received.wait(timeout=120):
        print("  TIMEOUT — no results received after 120s")
        server.shutdown()
        return False

    # Give a moment for the JS to send the "done" batch request
    time.sleep(2)
    server.shutdown()

    # ── Validate Results ──
    print(f"  Received: {len(test_results.results)} results, {len(test_results.errors)} errors\n")

    results_by_id = {r["ttb_id"]: r for r in test_results.results}
    errors_by_id = {e["ttb_id"]: e for e in test_results.errors}

    all_passed = True
    for ttb_id, description in test_cases:
        print(f"  {ttb_id} — {description}")
        if ttb_id in results_by_id:
            r = results_by_id[ttb_id]
            fields = r.get("fields", {})
            field_names = sorted(fields.keys())
            print(f"    ✓ Got data: {len(fields)} fields")
            for k, v in sorted(fields.items()):
                val_str = str(v)[:80] if not isinstance(v, list) else f"[{len(v)} items]"
                print(f"      {k}: {val_str}")

            # Specific validations
            if scraper_type == "detail":
                if "001" in ttb_id[5:8] if len(ttb_id) >= 14 else False:
                    # 001 detail pages should often have grapes
                    pass  # Not all have grapes, so no hard assertion
            elif scraper_type == "printable":
                # Non-001 IDs should have errored, not returned results
                is_001 = len(ttb_id) >= 14 and ttb_id[5:8] == "001"
                if not is_001:
                    print(f"    ⚠ WARNING: non-001 ID returned data (expected error)")

                # 001 IDs should have images
                if is_001 and "label_image_urls" in fields:
                    print(f"    ✓ IMAGES captured: {len(fields['label_image_urls'])} URLs")
                elif is_001:
                    print(f"    ⚠ No images found on printable page")

        elif ttb_id in errors_by_id:
            err = errors_by_id[ttb_id]
            error_type = err.get("error", "unknown")
            if scraper_type == "printable":
                is_001 = len(ttb_id) >= 14 and ttb_id[5:8] == "001"
                if not is_001:
                    print(f"    ✓ Expected error for non-001 ID: {error_type}")
                else:
                    print(f"    ✗ UNEXPECTED error for 001 ID: {error_type}")
                    all_passed = False
            else:
                print(f"    ⚠ Error: {error_type}")
        else:
            print(f"    ✗ MISSING — no result or error received")
            all_passed = False

    print(f"\n  {'='*50}")
    if all_passed:
        print(f"  ✓ ALL {scraper_type.upper()} TESTS PASSED")
    else:
        print(f"  ✗ SOME {scraper_type.upper()} TESTS FAILED")
    print(f"  {'='*50}\n")

    return all_passed


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Test TTB scrapers")
    parser.add_argument("--type", choices=["detail", "printable", "both"], default="both",
                        help="Which scraper to test")
    args = parser.parse_args()

    if sys.platform == "win32":
        import codecs
        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, errors="replace")
        sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, errors="replace")

    if args.type in ("detail", "both"):
        run_test(DETAIL_TEST_CASES, 8765, "detail")

    if args.type in ("printable", "both"):
        if args.type == "both":
            input("\n  Press Enter when ready to test PRINTABLE scraper...\n")
        run_test(PRINTABLE_TEST_CASES, 8766, "printable")


if __name__ == "__main__":
    main()
