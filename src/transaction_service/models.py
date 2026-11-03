from dataclasses import dataclass, asdict
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any


class Status(str, Enum):
    RECEIVED = "RECEIVED"
    PROCESSING = "PROCESSING"
    PROCESSED = "PROCESSED"
    DUPLICATE = "DUPLICATE"
    FAILED = "FAILED"
    RETRY_PENDING = "RETRY_PENDING"
    PENDING = "PENDING"


@dataclass(frozen=True)
class TransactionRequest:
    transaction_id: str
    request_id: str
    account_id: str
    transaction_type: str
    amount: Decimal
    currency: str
    sequence_number: int

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "TransactionRequest":
        required = (
            "transactionId", "requestId", "accountId", "transactionType",
            "amount", "currency", "sequenceNumber"
        )
        missing = [key for key in required if raw.get(key) is None or raw.get(key) == ""]
        if missing:
            raise ValueError(f"Missing required field(s): {', '.join(missing)}")

        txn_type = str(raw["transactionType"]).upper()
        if txn_type not in {"CREDIT", "DEBIT"}:
            raise ValueError("transactionType must be CREDIT or DEBIT")

        try:
            amount = Decimal(str(raw["amount"]))
        except (InvalidOperation, TypeError):
            raise ValueError("amount must be a valid number") from None

        if not amount.is_finite() or amount <= 0:
            raise ValueError("amount must be greater than zero")

        currency = str(raw["currency"]).upper()
        if len(currency) != 3 or not currency.isalpha():
            raise ValueError("currency must be a 3-letter code")

        try:
            sequence = int(raw["sequenceNumber"])
        except (ValueError, TypeError):
            raise ValueError("sequenceNumber must be an integer") from None
        if sequence < 1:
            raise ValueError("sequenceNumber must be >= 1")

        return cls(
            transaction_id=str(raw["transactionId"]).strip(),
            request_id=str(raw["requestId"]).strip(),
            account_id=str(raw["accountId"]).strip(),
            transaction_type=txn_type,
            amount=amount,
            currency=currency,
            sequence_number=sequence,
        )

    def fingerprint(self) -> tuple:
        return (
            self.transaction_id, self.account_id, self.transaction_type,
            str(self.amount), self.currency, self.sequence_number
        )

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["amount"] = str(self.amount)
        return data
