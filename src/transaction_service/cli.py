import argparse
import json
import logging
from pathlib import Path
from .store import SQLiteStore
from .processor import TransactionProcessor


def main():
    parser = argparse.ArgumentParser(description="Idempotent background transaction processor")
    parser.add_argument("--input", default="data/sample_transactions.json", help="JSON array of transaction requests")
    parser.add_argument("--db", default="transactions.sqlite3", help="SQLite database path")
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--fail-once", action="append", default=[], help="transactionId(s) to fail once for retry demonstration")
    parser.add_argument("--retry-pending", action="store_true", help="retry pending/retryable records after ingest")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    raw = json.loads(Path(args.input).read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise SystemExit("Input file must contain a JSON array")

    store = SQLiteStore(args.db)
    processor = TransactionProcessor(store, args.max_retries, set(args.fail_once))
    results = [processor.submit(item) for item in raw]
    if args.retry_pending:
        results.extend(processor.retry_pending())

    print(json.dumps({
        "results": [r.__dict__ for r in results],
        "summary": store.summary(),
        "transactions": store.list_transactions(),
        "events": store.list_events(),
    }, indent=2, default=str))


if __name__ == "__main__":
    main()
