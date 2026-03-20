#!/usr/bin/env python3
"""
Tiny HTTP server to receive scraped data from the browser.
CORS-enabled, appends to JSONL file.

Usage:
    python -m pipeline.analyze.tw_receiver
"""

import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

OUTPUT_FILE = Path(__file__).resolve().parents[2] / "totalwine_lexington_green.jsonl"


class ReceiverHandler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(204)
        self._cors_headers()
        self.end_headers()

    def do_POST(self):
        if self.path == "/append":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length).decode("utf-8")
            with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                f.write(body + "\n")
            lines = body.count("\n") + 1
            print(f"Received {lines} lines")
            self.send_response(200)
            self._cors_headers()
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(f'{{"ok": true, "lines": {lines}}}'.encode())
            return
        self.send_response(404)
        self.end_headers()
        self.wfile.write(b"not found")

    def do_GET(self):
        if self.path == "/done":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"done")
            print("All data received. Shutting down.")
            import threading
            threading.Timer(0.5, lambda: sys.exit(0)).start()
            return
        self.send_response(404)
        self.end_headers()

    def _cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")


def main():
    server = HTTPServer(("", 9876), ReceiverHandler)
    print("Receiver listening on http://localhost:9876")
    server.serve_forever()


if __name__ == "__main__":
    main()
