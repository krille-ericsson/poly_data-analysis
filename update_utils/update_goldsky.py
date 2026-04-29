import os
import pandas as pd
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from flatten_json import flatten
from datetime import datetime
import time
import csv

COLUMNS_TO_SAVE = [
    'timestamp', 'maker', 'makerAssetId', 'makerAmountFilled',
    'taker', 'takerAssetId', 'takerAmountFilled', 'transactionHash'
]


def _load_market_token_ids(markets_csv: str = "markets.csv") -> set[str]:
    token_ids = set()
    if not os.path.exists(markets_csv):
        return token_ids
    with open(markets_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            t1 = row.get("token1", "")
            t2 = row.get("token2", "")
            if t1:
                token_ids.add(str(t1))
            if t2:
                token_ids.add(str(t2))
    return token_ids


def scrape(
    at_once: int = 1000,
    min_timestamp: int | None = None,
    output_file: str = "goldsky/orderFilled.csv",
):
    QUERY_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"
    os.makedirs("goldsky", exist_ok=True)

    token_ids = _load_market_token_ids("markets.csv")
    if not token_ids:
        raise RuntimeError("No token IDs found in markets.csv. Run update_markets first.")

    start_ts = int(min_timestamp or 0)
    print(f"Query URL: {QUERY_URL}")
    print(f"Filtering by timestamp >= {start_ts}")
    print(f"Filtering by token IDs from markets.csv: {len(token_ids)} tokens")

    # overwrite file for deterministic window runs
    if os.path.exists(output_file):
        os.remove(output_file)

    last_timestamp = start_ts
    sticky_timestamp = None
    last_id = None
    total_records = 0
    batch_no = 0

    while True:
        is_sticky_query = sticky_timestamp is not None
        if is_sticky_query:
            where_clause = f'timestamp: "{sticky_timestamp}", id_gt: "{last_id}"'
        else:
            where_clause = f'timestamp_gt: "{last_timestamp}"'

        q_string = '''query MyQuery {
                        orderFilledEvents(orderBy: timestamp, orderDirection: asc
                                             first: ''' + str(at_once) + '''
                                             where: {''' + where_clause + '''}) {
                            id
                            maker
                            makerAmountFilled
                            makerAssetId
                            taker
                            takerAmountFilled
                            takerAssetId
                            timestamp
                            transactionHash
                        }
                    }
                '''

        query = gql(q_string)
        transport = RequestsHTTPTransport(url=QUERY_URL, verify=True, retries=3, timeout=30)
        client = Client(transport=transport)

        try:
            res = client.execute(query)
        except Exception as e:
            print(f"Query error: {e}. Retrying in 5s...")
            time.sleep(5)
            continue

        rows = res.get('orderFilledEvents', [])
        if not rows:
            if is_sticky_query:
                last_timestamp = sticky_timestamp
                sticky_timestamp = None
                last_id = None
                continue
            break

        df = pd.DataFrame([flatten(x) for x in rows]).reset_index(drop=True)
        df = df.sort_values(['timestamp', 'id'], ascending=True).reset_index(drop=True)

        batch_no += 1
        batch_last_timestamp = int(df.iloc[-1]['timestamp'])
        batch_last_id = df.iloc[-1]['id']
        batch_first_timestamp = int(df.iloc[0]['timestamp'])

        if len(df) >= at_once:
            sticky_timestamp = batch_last_timestamp
            last_id = batch_last_id
        else:
            if is_sticky_query:
                last_timestamp = sticky_timestamp
                sticky_timestamp = None
                last_id = None
            else:
                last_timestamp = batch_last_timestamp

        # window + market-token filter
        df = df[df['timestamp'].astype(int) >= start_ts]
        if not df.empty:
            df = df[
                df['makerAssetId'].astype(str).isin(token_ids)
                | df['takerAssetId'].astype(str).isin(token_ids)
            ]

        if not df.empty:
            df = df.drop_duplicates(subset=['id'])
            df_to_save = df[COLUMNS_TO_SAVE].copy()
            if os.path.isfile(output_file):
                df_to_save.to_csv(output_file, index=None, mode='a', header=None)
            else:
                df_to_save.to_csv(output_file, index=None)
            total_records += len(df_to_save)

        readable_time = datetime.utcfromtimestamp(batch_last_timestamp).strftime('%Y-%m-%d %H:%M:%S UTC')
        print(
            f"Batch {batch_no}: source_rows={len(rows)} filtered_rows={0 if df is None else len(df)} "
            f"last_ts={batch_last_timestamp} ({readable_time})"
        )

        if len(rows) < at_once and not is_sticky_query:
            break

    print(f"Finished scraping orderFilledEvents. Total filtered records: {total_records}")
    print(f"Output file: {output_file}")


def update_goldsky(
    at_once: int = 1000,
    min_timestamp: int | None = None,
    markets_filter=None,
    timeframes_filter=None,
):
    print(f"\n{'='*50}")
    print("Starting to scrape orderFilledEvents")
    print(f"{'='*50}")
    scrape(at_once=at_once, min_timestamp=min_timestamp)
