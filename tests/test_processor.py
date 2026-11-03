import json
from decimal import Decimal
from transaction_service.store import SQLiteStore
from transaction_service.processor import TransactionProcessor


def make_store():
    return SQLiteStore(":memory:")


def txn(tid="T1", rid="R1", seq=1, account="A1", kind="CREDIT", amount=100):
    return {"transactionId": tid, "requestId": rid, "sequenceNumber": seq,
            "accountId": account, "transactionType": kind, "amount": amount, "currency": "INR"}


def test_processes_credit_and_updates_balance_once():
    store = make_store()
    p = TransactionProcessor(store)
    assert p.submit(txn()).status == "PROCESSED"
    assert store.balance("A1")["balance"] == "100"
    assert p.submit(txn(rid="R1-duplicate")).status == "DUPLICATE"
    assert store.balance("A1")["balance"] == "100"
    assert len([e for e in store.list_events() if e["event_type"] == "PROCESSED"]) == 1


def test_out_of_order_event_is_pending_then_can_be_replayed():
    store = make_store()
    p = TransactionProcessor(store)
    assert p.submit(txn("T2", "R2", 2)).status == "PENDING"
    assert p.submit(txn("T1", "R1", 1)).status == "PROCESSED"
    assert p.retry_pending()[0].status == "PROCESSED"
    assert store.balance("A1")["balance"] == "200"


def test_invalid_record_does_not_stop_processing():
    store = make_store()
    p = TransactionProcessor(store)
    invalid = txn("BAD", "RB", 1, amount=-1)
    valid = txn("GOOD", "RG", 1, account="A2", amount=25)
    assert p.submit(invalid).status == "FAILED"
    assert p.submit(valid).status == "PROCESSED"
    assert store.balance("A2")["balance"] == "25"


def test_transient_failure_retries_without_duplicate_effect():
    store = make_store()
    p = TransactionProcessor(store, max_retries=2, failure_once_ids={"T1"})
    result = p.submit(txn())
    assert result.status == "PROCESSED"
    assert store.balance("A1")["balance"] == "100"
    row = store.get_transaction("T1")
    assert row["attempts"] == 2


def test_retry_exhaustion_marks_failed():
    store = make_store()
    p = TransactionProcessor(store, max_retries=0, failure_once_ids={"T1"})
    assert p.submit(txn()).status == "FAILED"
    assert store.balance("A1") is None


def test_conflicting_reuse_of_transaction_id_is_rejected():
    store = make_store()
    p = TransactionProcessor(store)
    assert p.submit(txn()).status == "PROCESSED"
    changed = txn(rid="R2", amount=200)
    assert p.submit(changed).status == "FAILED"
    assert store.balance("A1")["balance"] == "100"


def test_debit_cannot_overdraw():
    store = make_store()
    p = TransactionProcessor(store)
    assert p.submit(txn()).status == "PROCESSED"
    assert p.submit(txn("T2", "R2", 2, kind="DEBIT", amount=150)).status == "FAILED"
    assert store.balance("A1")["balance"] == "100"
