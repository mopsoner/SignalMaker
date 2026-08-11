"""Deprecated candidates dashboard wrapper.

The official candidates UI is served by FastAPI from ``frontend/dist``.
"""

from http.server import ThreadingHTTPServer

from raspberry_executor.web_dashboard_v2 import Handler as BaseHandler


class Handler(BaseHandler):
    def do_GET(self):
        if self.path.startswith("/candidates"):
            return self.redirect_official("/candidates.html")
        return super().do_GET()


def run_web(host="0.0.0.0", port=8090):
    ThreadingHTTPServer((host, port), Handler).serve_forever()
