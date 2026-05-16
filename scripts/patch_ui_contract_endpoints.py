from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
path = ROOT / "raspberry_executor" / "web_dashboard_v2.py"
text = path.read_text()

if "import json" not in text.splitlines()[:5]:
    text = text.replace("from html import escape\n", "import json\nfrom html import escape\n")

if "from raspberry_executor.ui_contract import candidates_view, positions_view, status_view" not in text:
    text = text.replace(
        "from raspberry_executor.state import StateStore\n",
        "from raspberry_executor.state import StateStore\nfrom raspberry_executor.ui_contract import candidates_view, positions_view, status_view\n",
    )

if "from raspberry_executor.settings_store import write_settings" not in text:
    text = text.replace(
        "from raspberry_executor.reset_positions_db import reset_positions_db\n",
        "from raspberry_executor.reset_positions_db import reset_positions_db\nfrom raspberry_executor.settings_store import write_settings\n",
    )

if "def send_json(handler, payload" not in text:
    insert_after = "def cell(value):\n    return \"\" if value is None else escape(str(value))\n"
    helper = '''

def send_json(handler, payload: dict, code: int = 200) -> None:
    data = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)
'''
    text = text.replace(insert_after, insert_after + helper)

if "if self.path.startswith(\"/api/ui/candidates\")" not in text:
    text = text.replace(
        "    def do_GET(self):\n        if self.path.startswith(\"/positions\"):\n",
        "    def do_GET(self):\n        if self.path.startswith(\"/api/ui/candidates\"):\n            return send_json(self, candidates_view(limit=100))\n        if self.path.startswith(\"/api/ui/positions\"):\n            return send_json(self, positions_view(limit=50))\n        if self.path.startswith(\"/api/ui/status\"):\n            return send_json(self, status_view())\n        if self.path.startswith(\"/positions\"):\n",
    )

if "write_settings(current, allowed_keys=set(current.keys()))" not in text:
    text = text.replace(
        "            write_env(current)\n            self.send_response(303)\n",
        "            write_env(current)\n            write_settings(current, allowed_keys=set(current.keys()))\n            self.send_response(303)\n",
    )

text = text.replace(
    "Restart after changing trading settings.",
    "Settings are saved to .env and SQLite. Restart after changing trading settings.",
)
text = text.replace(
    "Reset local tracking only: positions, executed candidates, events and pending queue. Binance assets and orders are not modified.",
    "Reset local runtime data only: positions, executed candidates, events, pending queue, local candidates and feed history. Settings are preserved.",
)
text = text.replace(
    "Reset local position tracking tables ?",
    "Reset local runtime data but keep settings ?",
)

path.write_text(text)
print("patched", path)
