import logging
from dataclasses import dataclass
from .models import TransactionRequest, Status

logger = logging.getLogger("transaction_service")


@dataclass
class ProcessingResult:
    transaction_id: str | None
    status: str
    message: str


class TransactionProcessor:
    def __init__(self, store, max_retries: int = 2, failure_once_ids: set[str] | None = None):
        self.store = store
        self.max_retries = max_retries
        self.failure_once_ids = set(failure_once_ids or ())
        self._failed_once: set[str] = set()

    def submit(self, raw: dict) -> ProcessingResult:
        try:
            request = TransactionRequest.from_dict(raw)
        except (ValueError, TypeError) as exc:
            self.store.record_event(None, "VALIDATION_FAILED", {"error": str(exc)})
            logger.warning("Transaction rejected: %s", exc)
            return ProcessingResult(None, "FAILED", str(exc))

        existing = self.store.get_transaction(request.transaction_id)
        if existing:
            if existing["payload_fingerprint"] != __import__("json").dumps(request.fingerprint()):
                self.store.record_event(request.transaction_id, "IDEMPOTENCY_CONFLICT", {})
                return ProcessingResult(request.transaction_id, "FAILED",
                                        "transactionId was reused with a different payload")
            self.store.record_event(request.transaction_id, "DUPLICATE", {"existingStatus": existing["status"]})
            return ProcessingResult(request.transaction_id, "DUPLICATE",
                                    f"Already registered; current state is {existing['status']}")

        request_replay = self.store.get_by_request(request.request_id)
        if request_replay:
            return ProcessingResult(request.transaction_id, "DUPLICATE", "requestId was already used")

        try:
            self.store.register(request)
        except Exception as exc:
            # A concurrent insert may win the unique-key race. Re-read before deciding.
            existing = self.store.get_transaction(request.transaction_id)
            if existing:
                return ProcessingResult(request.transaction_id, "DUPLICATE", "Concurrent duplicate")
            return ProcessingResult(request.transaction_id, "FAILED", str(exc))

        self.store.record_event(request.transaction_id, "RECEIVED", {"sequence": request.sequence_number})
        return self._process(request)

    def _process(self, request: TransactionRequest) -> ProcessingResult:
        attempts = 0
        while attempts <= self.max_retries:
            attempts += 1
            self.store.set_status(request.transaction_id, "PROCESSING", attempts=attempts)
            try:
                if request.transaction_id in self.failure_once_ids and request.transaction_id not in self._failed_once:
                    self._failed_once.add(request.transaction_id)
                    raise OSError("Simulated transient dependency failure")
                status = self.store.process_atomically(request, attempts)
                self.store.record_event(request.transaction_id, status, {"attempt": attempts})
                return ProcessingResult(request.transaction_id, status, "Processed successfully" if status == "PROCESSED" else "Duplicate effect prevented")
            except RuntimeError as exc:
                # Sequence gaps are not transient infrastructure errors; retain for replay.
                self.store.set_status(request.transaction_id, "PENDING", str(exc), attempts)
                self.store.record_event(request.transaction_id, "PENDING", {"reason": str(exc)})
                return ProcessingResult(request.transaction_id, "PENDING", str(exc))
            except OSError as exc:
                self.store.set_status(request.transaction_id, "RETRY_PENDING", str(exc), attempts)
                self.store.record_event(request.transaction_id, "RETRY", {"attempt": attempts})
                logger.warning("Retryable failure for %s (attempt %s)", request.transaction_id, attempts)
                if attempts > self.max_retries:
                    self.store.set_status(request.transaction_id, "FAILED", str(exc), attempts)
                    self.store.record_event(request.transaction_id, "FAILED", {"reason": "retry exhaustion"})
                    return ProcessingResult(request.transaction_id, "FAILED", "Retry limit exhausted")
            except ValueError as exc:
                self.store.set_status(request.transaction_id, "FAILED", str(exc), attempts)
                self.store.record_event(request.transaction_id, "FAILED", {"reason": str(exc)})
                return ProcessingResult(request.transaction_id, "FAILED", str(exc))
            except Exception as exc:
                self.store.set_status(request.transaction_id, "FAILED", "Unexpected processing error", attempts)
                self.store.record_event(request.transaction_id, "FAILED", {"reason": "unexpected error"})
                logger.exception("Unexpected processing failure for %s", request.transaction_id)
                return ProcessingResult(request.transaction_id, "FAILED", "Unexpected processing error")
        return ProcessingResult(request.transaction_id, "FAILED", "Retry limit exhausted")

    def retry_pending(self) -> list[ProcessingResult]:
        results = []
        for row in self.store.list_transactions():
            if row["status"] in {"PENDING", "RETRY_PENDING"}:
                request = TransactionRequest(
                    transaction_id=row["transaction_id"], request_id=row["request_id"],
                    account_id=row["account_id"], transaction_type=row["transaction_type"],
                    amount=__import__("decimal").Decimal(row["amount"]), currency=row["currency"],
                    sequence_number=row["sequence_number"]
                )
                results.append(self._process(request))
        return results
