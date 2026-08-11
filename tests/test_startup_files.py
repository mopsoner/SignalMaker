from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def test_deployment_entrypoint_and_required_files_are_tracked() -> None:
    """Keep an accidental ``git rm`` from producing a broken deployment."""

    required_files = (
        ".replit",
        "requirements.txt",
        "scripts/start_reserved_vm.sh",
        "scripts/init_db.py",
        "scripts/run_pipeline_loop.py",
        "scripts/run_executor_loop.py",
        "scripts/run_scheduler_loop.py",
        "scripts/run_kraken_candle_feed_loop.py",
    )

    missing = [path for path in required_files if not (REPOSITORY_ROOT / path).is_file()]

    assert not missing, f"Startup requires missing file(s): {', '.join(missing)}"


def test_run_script_exposes_kraken_candle_feed_modes() -> None:
    run_script = (REPOSITORY_ROOT / "run.sh").read_text(encoding="utf-8")

    assert "kraken-candle-feed-once)" in run_script
    assert "kraken-candle-feed-loop)" in run_script
    assert "python -m scripts.run_kraken_candle_feed_loop --once" in run_script
    assert "python -m scripts.run_kraken_candle_feed_loop" in run_script


def test_replit_deployment_uses_the_reserved_vm_entrypoint() -> None:
    replit_config = (REPOSITORY_ROOT / ".replit").read_text(encoding="utf-8")

    assert 'run = ["bash", "scripts/start_reserved_vm.sh"]' in replit_config
