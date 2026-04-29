import requests
import csv
import json
import os
from typing import Optional, Set
from datetime import datetime, timezone
from update_utils.settings_loader import slug_matches


def _parse_iso_dt(v: str) -> Optional[datetime]:
    if not v:
        return None
    try:
        return datetime.fromisoformat(v.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def update_markets(
    csv_filename: str = "markets.csv",
    batch_size: int = 500,
    markets_filter: Optional[Set[str]] = None,
    timeframes_filter: Optional[Set[str]] = None,
    start_dt: Optional[datetime] = None,
):
    """
    Fetch markets from Gamma and write filtered dataset to CSV.

    Filters:
    - markets_filter: e.g. {'btc','eth','xrp','sol'}
    - timeframes_filter: e.g. {'5m','15m'}
    - start_dt: include markets created/closed after this UTC datetime
    """
    base_url = "https://gamma-api.polymarket.com/markets"

    headers = [
        'createdAt', 'id', 'question', 'answer1', 'answer2', 'neg_risk',
        'market_slug', 'token1', 'token2', 'condition_id', 'volume', 'ticker', 'closedTime'
    ]

    markets_filter = {m.lower() for m in (markets_filter or set())}
    timeframes_filter = {t.lower() for t in (timeframes_filter or set())}

    print(f"Writing filtered markets to {csv_filename}")
    print(f"markets filter={sorted(markets_filter) if markets_filter else 'none'}")
    print(f"timeframes filter={sorted(timeframes_filter) if timeframes_filter else 'none'}")
    print(f"start_dt={start_dt.isoformat() if start_dt else 'none'}")

    current_offset = 0
    total_written = 0

    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)

        while True:
            print(f"Fetching batch at offset {current_offset}...")
            try:
                params = {
                    'order': 'createdAt',
                    'ascending': 'false',
                    'limit': batch_size,
                    'offset': current_offset,
                }

                response = requests.get(base_url, params=params, timeout=30)

                if response.status_code == 500:
                    print("Server error (500) - retrying in 5 seconds...")
                    import time
                    time.sleep(5)
                    continue
                elif response.status_code == 429:
                    print("Rate limited (429) - waiting 10 seconds...")
                    import time
                    time.sleep(10)
                    continue
                elif response.status_code != 200:
                    print(f"API error {response.status_code}: {response.text}")
                    import time
                    time.sleep(3)
                    continue

                markets = response.json()
                if not markets:
                    print("No more markets found. Completed!")
                    break

                stop_due_to_time = False

                for market in markets:
                    try:
                        slug = market.get('slug', '') or ''
                        if markets_filter and timeframes_filter and not slug_matches(slug, markets_filter, timeframes_filter):
                            continue

                        created_dt = _parse_iso_dt(market.get('createdAt', ''))
                        closed_dt = _parse_iso_dt(market.get('closedTime', ''))

                        if start_dt:
                            # Keep if either created or closed is inside lookback window.
                            in_window = (
                                (created_dt and created_dt >= start_dt)
                                or (closed_dt and closed_dt >= start_dt)
                            )
                            if not in_window:
                                # because sorted by createdAt desc, once clearly older we can stop.
                                if created_dt and created_dt < start_dt:
                                    stop_due_to_time = True
                                continue

                        outcomes_str = market.get('outcomes', '[]')
                        outcomes = json.loads(outcomes_str) if isinstance(outcomes_str, str) else outcomes_str
                        answer1 = outcomes[0] if len(outcomes) > 0 else ''
                        answer2 = outcomes[1] if len(outcomes) > 1 else ''

                        clob_tokens_str = market.get('clobTokenIds', '[]')
                        clob_tokens = json.loads(clob_tokens_str) if isinstance(clob_tokens_str, str) else clob_tokens_str
                        token1 = clob_tokens[0] if len(clob_tokens) > 0 else ''
                        token2 = clob_tokens[1] if len(clob_tokens) > 1 else ''

                        neg_risk = market.get('negRiskAugmented', False) or market.get('negRiskOther', False)
                        question_text = market.get('question', '') or market.get('title', '')

                        ticker = ''
                        if market.get('events') and len(market.get('events', [])) > 0:
                            ticker = market['events'][0].get('ticker', '')

                        row = [
                            market.get('createdAt', ''),
                            market.get('id', ''),
                            question_text,
                            answer1,
                            answer2,
                            neg_risk,
                            slug,
                            token1,
                            token2,
                            market.get('conditionId', ''),
                            market.get('volume', ''),
                            ticker,
                            market.get('closedTime', ''),
                        ]
                        writer.writerow(row)
                        total_written += 1
                    except (ValueError, KeyError, json.JSONDecodeError) as e:
                        print(f"Error processing market {market.get('id', 'unknown')}: {e}")
                        continue

                current_offset += len(markets)
                print(f"Total filtered markets written: {total_written}")

                if stop_due_to_time:
                    print("Reached markets older than requested time window; stopping fetch.")
                    break

                if len(markets) < batch_size:
                    print("Received less than batch size. Reached end.")
                    break

            except requests.exceptions.RequestException as e:
                print(f"Network error: {e}")
                import time
                time.sleep(5)
                continue
            except Exception as e:
                print(f"Unexpected error: {e}")
                import time
                time.sleep(3)
                continue

    print(f"\nCompleted! Wrote {total_written} markets to {csv_filename}")
