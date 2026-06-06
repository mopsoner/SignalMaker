from __future__ import annotations

import logging

from signalmaker_remote import fetch_momentum_candidates, load_api_config, run_startup_api_checks


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def main() -> None:
    configure_logging()
    config = load_api_config()
    run_startup_api_checks(config)
    candidates = fetch_momentum_candidates(config, limit=50)
    logging.getLogger("signalmaker.executor").info("momentum_candidates_received count=%s", len(candidates))


if __name__ == "__main__":
    main()
