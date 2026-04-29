import warnings
warnings.filterwarnings('ignore')

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
from poly_utils.utils import get_markets, update_missing_tokens
from update_utils.settings_loader import slug_matches

import csv as csv_lib


def get_processed_df(df: pl.DataFrame, markets_df: pl.DataFrame):
    markets_df = markets_df.rename({'id': 'market_id'})

    markets_long = (
        markets_df
        .select(["market_id", "token1", "token2"])
        .melt(id_vars="market_id", value_vars=["token1", "token2"],
              variable_name="side", value_name="asset_id")
    )

    df = df.with_columns(
        pl.when(pl.col("makerAssetId") != "0")
        .then(pl.col("makerAssetId"))
        .otherwise(pl.col("takerAssetId"))
        .alias("nonusdc_asset_id")
    )

    df = df.join(
        markets_long,
        left_on="nonusdc_asset_id",
        right_on="asset_id",
        how="left",
    )

    df = df.with_columns([
        pl.when(pl.col("makerAssetId") == "0").then(pl.lit("USDC")).otherwise(pl.col("side")).alias("makerAsset"),
        pl.when(pl.col("takerAssetId") == "0").then(pl.lit("USDC")).otherwise(pl.col("side")).alias("takerAsset"),
        pl.col("market_id"),
    ])

    df = df[['timestamp', 'market_id', 'maker', 'makerAsset', 'makerAmountFilled', 'taker', 'takerAsset', 'takerAmountFilled', 'transactionHash']]

    df = df.with_columns([
        (pl.col("makerAmountFilled") / 10**6).alias("makerAmountFilled"),
        (pl.col("takerAmountFilled") / 10**6).alias("takerAmountFilled"),
    ])

    df = df.with_columns([
        pl.when(pl.col("takerAsset") == "USDC").then(pl.lit("BUY")).otherwise(pl.lit("SELL")).alias("taker_direction"),
        pl.when(pl.col("takerAsset") == "USDC").then(pl.lit("SELL")).otherwise(pl.lit("BUY")).alias("maker_direction"),
    ])

    df = df.with_columns([
        pl.when(pl.col("makerAsset") != "USDC").then(pl.col("makerAsset")).otherwise(pl.col("takerAsset")).alias("nonusdc_side"),
        pl.when(pl.col("takerAsset") == "USDC").then(pl.col("takerAmountFilled")).otherwise(pl.col("makerAmountFilled")).alias("usd_amount"),
        pl.when(pl.col("takerAsset") != "USDC").then(pl.col("takerAmountFilled")).otherwise(pl.col("makerAmountFilled")).alias("token_amount"),
        pl.when(pl.col("takerAsset") == "USDC")
        .then(pl.col("takerAmountFilled") / pl.col("makerAmountFilled"))
        .otherwise(pl.col("makerAmountFilled") / pl.col("takerAmountFilled"))
        .cast(pl.Float64)
        .alias("price")
    ])

    df = df[['timestamp', 'market_id', 'maker', 'taker', 'nonusdc_side', 'maker_direction', 'taker_direction', 'price', 'usd_amount', 'token_amount', 'transactionHash']]
    return df


def process_live(
    min_timestamp: int | None = None,
    markets_filter=None,
    timeframes_filter=None,
):
    print("=" * 60)
    print("🔄 Processing Live Trades")
    print("=" * 60)

    schema_overrides = {
        "takerAssetId": pl.Utf8,
        "makerAssetId": pl.Utf8,
    }


    if not os.path.exists("goldsky/orderFilled.csv"):
        print("⚠ No goldsky/orderFilled.csv found. Nothing to process.")
        return

    df = pl.scan_csv("goldsky/orderFilled.csv", schema_overrides=schema_overrides).collect(streaming=True)

    if len(df) == 0:
        print("⚠ orderFilled.csv is empty for selected filters/window. Wrote empty processed/trades.csv")
        if not os.path.isdir('processed'):
            os.makedirs('processed')
        pl.DataFrame({
            'timestamp': [], 'market_id': [], 'maker': [], 'taker': [],
            'nonusdc_side': [], 'maker_direction': [], 'taker_direction': [],
            'price': [], 'usd_amount': [], 'token_amount': [], 'transactionHash': []
        }).write_csv('processed/trades.csv')
        return

    if min_timestamp:
        df = df.filter(pl.col("timestamp") >= min_timestamp)

    df = df.with_columns(
        pl.from_epoch(pl.col('timestamp'), time_unit='s').alias('timestamp')
    )

    print(f"✓ Loaded {len(df):,} filtered order rows")

    # Discover and fetch missing markets before processing
    maker_ids = set()
    taker_ids = set()
    with open("goldsky/orderFilled.csv", newline="", encoding="utf-8") as f:
        reader = csv_lib.DictReader(f)
        for row in reader:
            ts = int(row.get("timestamp", "0") or 0)
            if min_timestamp and ts < min_timestamp:
                continue
            if row.get("makerAssetId", "0") != "0":
                maker_ids.add(row["makerAssetId"])
            if row.get("takerAssetId", "0") != "0":
                taker_ids.add(row["takerAssetId"])
    trade_asset_ids = maker_ids | taker_ids

    existing_ids = set()
    for fname in ("markets.csv", "missing_markets.csv"):
        if os.path.exists(fname):
            with open(fname, newline="", encoding="utf-8") as f:
                reader = csv_lib.DictReader(f)
                for row in reader:
                    if row.get("token1"):
                        existing_ids.add(row["token1"])
                    if row.get("token2"):
                        existing_ids.add(row["token2"])
    missing_ids = sorted(trade_asset_ids - existing_ids)

    if missing_ids:
        print(f"🔍 Found {len(missing_ids)} missing markets — fetching from Polymarket API...")
        update_missing_tokens(missing_ids)

    markets_df = get_markets()

    if markets_filter and timeframes_filter and len(markets_df) > 0:
        mf = {m.lower() for m in markets_filter}
        tf = {t.lower() for t in timeframes_filter}
        markets_df = markets_df.filter(
            pl.col("market_slug").map_elements(lambda s: slug_matches(str(s or ""), mf, tf), return_dtype=pl.Boolean)
        )

    if min_timestamp and len(markets_df) > 0:
        markets_df = markets_df.with_columns(
            pl.col("createdAt").str.to_datetime(strict=False, utc=True).alias("createdAt_dt"),
            pl.col("closedTime").str.to_datetime(strict=False, utc=True).alias("closedTime_dt"),
        ).filter(
            (pl.col("createdAt_dt").is_not_null() & (pl.col("createdAt_dt").dt.epoch("s") >= min_timestamp))
            | (pl.col("closedTime_dt").is_not_null() & (pl.col("closedTime_dt").dt.epoch("s") >= min_timestamp))
        ).drop(["createdAt_dt", "closedTime_dt"])

    print(f"✓ Using {len(markets_df):,} filtered markets")

    new_df = get_processed_df(df, markets_df)

    if not os.path.isdir('processed'):
        os.makedirs('processed')

    op_file = 'processed/trades.csv'
    new_df.write_csv(op_file)
    print(f"✓ Wrote {len(new_df):,} rows to {op_file}")

    print("=" * 60)
    print("✅ Processing complete!")
    print("=" * 60)


if __name__ == "__main__":
    process_live()
