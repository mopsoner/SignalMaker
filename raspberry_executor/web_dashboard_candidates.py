from http.server import ThreadingHTTPServer

from raspberry_executor.candidates_page import candidates_page
from raspberry_executor.web_dashboard import page
from raspberry_executor.web_dashboard_v2 import Handler as BaseHandler


class Handler(BaseHandler):
    def do_GET(self):
        if self.path.startswith("/candidates"):
            data = page(candidates_page())
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


def run_web(host="0.0.0.0", port=8090):
    ThreadingHTTPServer((host, port), Handler).serve_forever()
