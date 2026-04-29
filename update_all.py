import argparse
from datetime import datetime, timedelta, timezone

from update_utils.settings_loader import load_settings
from update_utils.update_markets import update_markets
from update_utils.update_goldsky import update_goldsky
from update_utils.process_live import process_live


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run full Polymarket data pipeline with filters")
    parser.add_argument("--settings", default="settings.yaml", help="Path to settings YAML")
    parser.add_argument(
        "--window",
        default="7d",
        help="Lookback window, e.g. 7d, 14d, 30d (default: 7d)",
    )
    parser.add_argument("--batch-size", type=int, default=500, help="Gamma markets page size")
    parser.add_argument("--goldsky-batch", type=int, default=1000, help="Goldsky page size")
    return parser.parse_args()


def parse_window_days(window: str) -> int:
    w = window.strip().lower()
    if not w.endswith("d"):
        raise ValueError("--window must be in format <N>d, e.g. 7d")
    return int(w[:-1])


if __name__ == "__main__":
    args = parse_args()
    days = parse_window_days(args.window)
    start_dt = datetime.now(timezone.utc) - timedelta(days=days)
    start_ts = int(start_dt.timestamp())

    settings = load_settings(args.settings)

    print("Updating markets")
    update_markets(
        csv_filename="markets.csv",
        batch_size=args.batch_size,
        markets_filter=settings.market_set,
        timeframes_filter=settings.timeframe_set,
        start_dt=start_dt,
    )

    print("Updating goldsky")
    update_goldsky(
        at_once=args.goldsky_batch,
        min_timestamp=start_ts,
        markets_filter=settings.market_set,
        timeframes_filter=settings.timeframe_set,
    )

    print("Processing live")
    process_live(
        min_timestamp=start_ts,
        markets_filter=settings.market_set,
        timeframes_filter=settings.timeframe_set,
    )
