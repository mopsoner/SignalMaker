from http.server import ThreadingHTTPServer

from raspberry_executor.margin_admin_page import margin_admin_box, save_margin_admin
from raspberry_executor.web_dashboard import page
from raspberry_executor.web_dashboard_candidates import Handler as BaseHandler
from raspberry_executor.web_dashboard_v2 import admin_page_v2


class Handler(BaseHandler):
    def do_GET(self):
        if self.path.startswith("/admin"):
            data = page(admin_page_v2() + margin_admin_box())
            try:
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                return
            return
        return super().do_GET()

    def do_POST(self):
        if self.path.startswith("/admin/margin"):
            length = int(self.headers.get("Content-Length", "0"))
            save_margin_admin(self.rfile.read(length))
            self.send_response(303)
            self.send_header("Location", "/admin")
            self.end_headers()
            return
        return super().do_POST()


def run_web(host="0.0.0.0", port=8090):
    ThreadingHTTPServer((host, port), Handler).serve_forever()
