#!/usr/bin/env python3
"""
Replit Reserved VM deployment entry point.
1. Converts DATABASE_URL dialect (postgres:// -> postgresql+psycopg://)
2. Starts pipeline / executor / scheduler workers as background processes
3. Runs the FastAPI app via uvicorn on port 5000 (-> external port 80)

DB schema init is handled by the FastAPI lifespan (CREATE_TABLES_ON_BOOT=true).
The built React SPA is served from frontend/dist if present (built by build cmd).
"""
import os
import subprocess
import sys

# ── make sure the workspace root is always on sys.path ──────────────────────
ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def _fix_database_url() -> None:
    url = os.environ.get("DATABASE_URL", "")
    if url.startswith("postgres://"):
        url = "postgresql+psycopg://" + url[len("postgres://"):]
    elif url.startswith("postgresql://"):
        url = "postgresql+psycopg://" + url[len("postgresql://"):]
    if url:
        os.environ["DATABASE_URL"] = url
        return

    pghost = os.environ.get("PGHOST", "")
    pguser = os.environ.get("PGUSER", "")
    pgpass = os.environ.get("PGPASSWORD", "")
    pgdb = os.environ.get("PGDATABASE", "")
    pgport = os.environ.get("PGPORT", "5432")
    sslmode = os.environ.get("PGSSLMODE", "require")
    if pghost and pguser and pgpass and pgdb:
        os.environ["DATABASE_URL"] = (
            f"postgresql+psycopg://{pguser}:{pgpass}@{pghost}:{pgport}/{pgdb}"
            f"?sslmode={sslmode}"
        )


def _start_workers() -> None:
    os.makedirs(os.path.join(ROOT, ".runtime"), exist_ok=True)
    os.makedirs(os.path.join(ROOT, "logs"), exist_ok=True)

    env = {**os.environ, "PYTHONPATH": ROOT}
    workers = [
        ("pipeline",  os.path.join(ROOT, "scripts", "run_pipeline_loop.py")),
        ("executor",  os.path.join(ROOT, "scripts", "run_executor_loop.py")),
        ("scheduler", os.path.join(ROOT, "scripts", "run_scheduler_loop.py")),
    ]
    for name, script in workers:
        print(f"Starting {name} worker …", flush=True)
        logfile = os.path.join(ROOT, "logs", f"{name}.log")
        with open(logfile, "ab") as lf:
            p = subprocess.Popen(
                [sys.executable, script],
                stdout=lf, stderr=lf,
                cwd=ROOT, env=env,
            )
        with open(os.path.join(ROOT, ".runtime", f"{name}.pid"), "w") as pf:
            pf.write(str(p.pid))


if __name__ == "__main__":
    _fix_database_url()
    _start_workers()

    port = int(os.environ.get("APP_PORT", "5000"))
    print(f"Launching API on port {port} …", flush=True)

    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=port)
