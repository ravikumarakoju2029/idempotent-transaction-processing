import json
import sqlite3
from decimal import Decimal
from pathlib import Path
from threading import RLock
from typing import Any


class SQLiteStore:
    """Small durable store. A single SQLite transaction protects each state change."""

    def __init__(self, database_path: str = "transactions.sqlite3"):
        self.database_path = str(database_path)
        self._lock = RLock()
        self._uri = self.database_path == ":memory:"
        self._anchor = None
        if self._uri:
            # Keep one connection alive so separate operations share the in-memory database.
            self.database_path = f"file:txn_store_{id(self)}?mode=memory&cache=shared"
            self._anchor = sqlite3.connect(self.database_path, uri=True, timeout=10)
        else:
            Path(self.database_path).parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self):
        conn = sqlite3.connect(self.database_path, timeout=10, uri=self._uri)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _initialize(self):
        with self._connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS accounts (
                    account_id TEXT PRIMARY KEY,
                    currency TEXT NOT NULL,
                    balance TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL UNIQUE,
                    account_id TEXT NOT NULL,
                    transaction_type TEXT NOT NULL,
                    amount TEXT NOT NULL,
                    currency TEXT NOT NULL,
                    sequence_number INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    error TEXT,
                    payload_fingerprint TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ledger (
                    transaction_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    transaction_type TEXT NOT NULL,
                    amount TEXT NOT NULL,
                    currency TEXT NOT NULL,
                    sequence_number INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS account_sequences (
                    account_id TEXT PRIMARY KEY,
                    last_sequence INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    transaction_id TEXT,
                    event_type TEXT NOT NULL,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
            """)

    def record_event(self, transaction_id: str | None, event_type: str, details: dict[str, Any]):
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO events(transaction_id,event_type,details) VALUES(?,?,?)",
                (transaction_id, event_type, json.dumps(details, default=str))
            )

    def get_transaction(self, transaction_id: str):
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM transactions WHERE transaction_id=?", (transaction_id,)).fetchone()
            return dict(row) if row else None

    def get_by_request(self, request_id: str):
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM transactions WHERE request_id=?", (request_id,)).fetchone()
            return dict(row) if row else None

    def register(self, request, status: str = "RECEIVED"):
        with self._lock, self._connect() as conn:
            conn.execute("""
                INSERT INTO transactions(
                    transaction_id,request_id,account_id,transaction_type,amount,currency,
                    sequence_number,status,payload_fingerprint
                ) VALUES(?,?,?,?,?,?,?,?,?)
            """, (
                request.transaction_id, request.request_id, request.account_id,
                request.transaction_type, str(request.amount), request.currency,
                request.sequence_number, status, json.dumps(request.fingerprint())
            ))

    def set_status(self, transaction_id: str, status: str, error: str | None = None, attempts: int | None = None):
        with self._lock, self._connect() as conn:
            if attempts is None:
                conn.execute("UPDATE transactions SET status=?, error=? WHERE transaction_id=?",
                             (status, error, transaction_id))
            else:
                conn.execute("UPDATE transactions SET status=?, error=?, attempts=? WHERE transaction_id=?",
                             (status, error, attempts, transaction_id))

    def process_atomically(self, request, attempts: int):
        """Apply balance, ledger, sequence and final status in one DB transaction."""
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing_ledger = conn.execute(
                "SELECT 1 FROM ledger WHERE transaction_id=?", (request.transaction_id,)
            ).fetchone()
            if existing_ledger:
                conn.execute("UPDATE transactions SET status='DUPLICATE', attempts=? WHERE transaction_id=?",
                             (attempts, request.transaction_id))
                return "DUPLICATE"

            account = conn.execute(
                "SELECT currency,balance FROM accounts WHERE account_id=?", (request.account_id,)
            ).fetchone()
            if account is None:
                conn.execute("INSERT INTO accounts(account_id,currency,balance) VALUES(?,?,?)",
                             (request.account_id, request.currency, "0"))
                balance = 0
                account_currency = request.currency
            else:
                balance = Decimal(account["balance"])
                account_currency = account["currency"]

            if account_currency != request.currency:
                raise ValueError("Account currency does not match transaction currency")

            last = conn.execute(
                "SELECT last_sequence FROM account_sequences WHERE account_id=?", (request.account_id,)
            ).fetchone()
            last_sequence = int(last["last_sequence"]) if last else 0
            if request.sequence_number != last_sequence + 1:
                raise RuntimeError(f"Sequence gap: expected {last_sequence + 1}, received {request.sequence_number}")

            if request.transaction_type == "DEBIT" and balance < request.amount:
                raise ValueError("Insufficient funds")

            delta = request.amount if request.transaction_type == "CREDIT" else -request.amount
            new_balance = balance + delta
            conn.execute("""
                INSERT INTO ledger(transaction_id,account_id,transaction_type,amount,currency,sequence_number)
                VALUES(?,?,?,?,?,?)
            """, (request.transaction_id, request.account_id, request.transaction_type,
                  str(request.amount), request.currency, request.sequence_number))
            conn.execute("""
                INSERT INTO accounts(account_id,currency,balance) VALUES(?,?,?)
                ON CONFLICT(account_id) DO UPDATE SET balance=excluded.balance
            """, (request.account_id, request.currency, str(new_balance)))
            conn.execute("""
                INSERT INTO account_sequences(account_id,last_sequence) VALUES(?,?)
                ON CONFLICT(account_id) DO UPDATE SET last_sequence=excluded.last_sequence
            """, (request.account_id, request.sequence_number))
            conn.execute("UPDATE transactions SET status='PROCESSED', attempts=?, error=NULL WHERE transaction_id=?",
                         (attempts, request.transaction_id))
            return "PROCESSED"

    def balance(self, account_id: str):
        with self._connect() as conn:
            row = conn.execute("SELECT balance,currency FROM accounts WHERE account_id=?", (account_id,)).fetchone()
            return {"accountId": account_id, "balance": row["balance"], "currency": row["currency"]} if row else None

    def summary(self):
        with self._connect() as conn:
            rows = conn.execute("SELECT status, COUNT(*) AS count FROM transactions GROUP BY status").fetchall()
            counts = {row["status"]: row["count"] for row in rows}
            for status in ("RECEIVED", "PROCESSING", "PROCESSED", "DUPLICATE", "FAILED", "RETRY_PENDING", "PENDING"):
                counts.setdefault(status, 0)
            total = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
            return {"total": total, "byStatus": counts}

    def list_events(self):
        with self._connect() as conn:
            return [dict(row) for row in conn.execute("SELECT * FROM events ORDER BY id").fetchall()]

    def list_transactions(self):
        with self._connect() as conn:
            return [dict(row) for row in conn.execute("SELECT * FROM transactions ORDER BY rowid").fetchall()]
