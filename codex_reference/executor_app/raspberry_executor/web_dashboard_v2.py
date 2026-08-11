import json
from http.server import ThreadingHTTPServer

from raspberry_executor.reset_positions_db import reset_positions_db
from raspberry_executor.ui_contract import candidates_view, positions_view, status_view
from raspberry_executor.web_dashboard import Handler as DashboardHandler
from raspberry_executor.web_local import official_ui_url


def send_json(handler, payload: dict, code: int = 200) -> None:
    data = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Cache-Control", "no-store, max-age=0")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


class Handler(DashboardHandler):
    """Deprecated dashboard v2 handler.

    JSON compatibility endpoints are backed by ``ui_contract``. Historical HTML
    pages redirect to the single supported FastAPI frontend in ``frontend/dist``.
    """

    def do_GET(self):
        if self.path.startswith("/api/ui/candidates"):
            return send_json(self, candidates_view(limit=100))
        if self.path.startswith("/api/ui/positions"):
            return send_json(self, positions_view(limit=50))
        if self.path.startswith("/api/ui/status"):
            return send_json(self, status_view())
        if self.path.startswith("/positions"):
            return self.redirect_official("/positions.html")
        if self.path.startswith("/admin"):
            return self.redirect_official("/ops.html")
        return super().do_GET()

    def do_POST(self):
        if self.path.startswith("/admin/reset-positions"):
            reset_positions_db()
            self.send_response(303)
            self.send_header("Location", official_ui_url("/positions.html"))
            self.end_headers()
            return
        if self.path.startswith("/admin"):
            self.send_response(308)
            self.send_header("Location", official_ui_url("/ops.html"))
            self.end_headers()
            return
        return super().do_POST()


def run_web(host="0.0.0.0", port=8090):
    ThreadingHTTPServer((host, port), Handler).serve_forever()
