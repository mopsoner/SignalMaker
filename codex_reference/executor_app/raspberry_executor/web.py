"""Deprecated Python http.server dashboard entrypoint.

The supported Raspberry web UI is served by FastAPI from ``frontend/dist`` on
port 8080. This module remains importable for historical launchers and redirects
legacy pages to the official static frontend.
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from raspberry_executor.web_local import LEGACY_WARNING, official_ui_url


_REDIRECTS = {
    "/": "/index.html",
    "/positions": "/positions.html",
    "/orders": "/orders.html",
    "/admin": "/ops.html",
    "/logs": "/ops.html",
    "/candidates": "/candidates.html",
    "/momentum": "/momentum-candidates.html",
    "/momentum-candidates": "/momentum-candidates.html",
}


class AdminHandler(BaseHTTPRequestHandler):
    def _redirect_official(self, target: str = "/index.html") -> None:
        self.send_response(308)
        self.send_header("Location", official_ui_url(target))
        self.send_header("X-Deprecated-Dashboard", LEGACY_WARNING)
        self.end_headers()

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        for legacy, target in _REDIRECTS.items():
            if path == legacy or path.startswith(f"{legacy}/"):
                return self._redirect_official(target)
        return self._redirect_official("/index.html")

    def do_POST(self) -> None:
        return self._redirect_official("/ops.html")

    def log_message(self, format, *args):
        return


def run_web(host: str = "0.0.0.0", port: int = 8090) -> None:
    server = ThreadingHTTPServer((host, port), AdminHandler)
    server.serve_forever()


if __name__ == "__main__":
    run_web()
