# Idempotent Background Transaction Processing Service

A small, runnable implementation of the L2 practical assessment in the supplied problem statement. It demonstrates idempotent transaction processing, duplicate detection, sequence-based ordering, retry-safe transient failure handling, durable local state, and operational reporting.

## Requirements covered

| Assessment requirement | Implementation |
|---|---|
| FR-01 configurable/replaceable input | JSON input file; CLI boundary is replaceable by an API or queue adapter |
| FR-02 validation | Required fields, type, positive amount, currency and sequence validation |
| FR-03 transaction state | SQLite transaction state table |
| FR-04 lifecycle | RECEIVED, PROCESSING, PROCESSED, DUPLICATE, FAILED, RETRY_PENDING, PENDING |
| FR-05 process unique business transaction once | Unique transaction ID and ledger primary key |
| FR-06 later duplicate detection | Fingerprint check and ledger/idempotency lookup |
| FR-07 out-of-order handling | Per-account sequence numbers; future sequence is PENDING |
| FR-08 transient failure simulation | `--fail-once TXN-ID` |
| FR-09 retry safely | Bounded retry loop; ledger and balance update are atomic |
| FR-10 separation of concerns | Models, processor, persistence store, and CLI are separate |
| FR-11 processing summary | Counts by status, transaction listing and event log |
| FR-12 tests | Duplicate, ordering, validation, retry, conflict and overdraft tests |

## Architecture

```text
JSON input / future queue adapter
              |
              v
          CLI adapter
              |
              v
     TransactionProcessor
       | validation
       | idempotency checks
       | sequence policy
       | bounded retry
              |
              v
          SQLiteStore
       | transactions
       | accounts/balances
       | ledger
       | account sequence
       | processing events
```

The processor owns application workflow; the store owns persistence and atomic state changes. A future HTTP, queue, or scheduler adapter can call `TransactionProcessor.submit()` without changing transaction rules.

## Ordering strategy and assumptions

This implementation uses **per-account, contiguous sequence numbers starting at 1**. A request with a future sequence is stored as `PENDING`; it is not applied. Once the missing event arrives, run the pending replay (`--retry-pending`) to process it. Sequence state is scoped to account, so independent accounts do not block each other.

This is intentionally conservative: financial ledger mutations are not reordered because doing so could change the result of balance-dependent rules such as insufficient-funds validation. A production system should define sequence ownership and gap timeout/escalation with the upstream producer.

## Idempotency and consistency

- `transactionId` is the business idempotency key and is unique in the transaction and ledger tables.
- `requestId` is also unique to detect request replays.
- A canonical payload fingerprint rejects reuse of one transaction ID with different business data.
- Ledger insertion, balance mutation, sequence advancement, and final `PROCESSED` status occur in one SQLite transaction.
- The ledger primary key provides a second line of defense against duplicate effects.

## Retry and failure behavior

The CLI can simulate a one-time transient dependency failure with `--fail-once TXN-2005`. The processor retries up to `--max-retries` additional attempts. Retry attempts are recorded. Exhaustion moves the record to `FAILED`. Validation, insufficient funds, currency mismatch, and sequence gaps are not treated as transient infrastructure failures.

`PENDING` records can be replayed after their missing sequence arrives. `RETRY_PENDING` records can also be replayed. For a production queue, use delayed retries with exponential backoff and a dead-letter queue.

## Run locally

Python 3.10+ is required.

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

python -m pip install -e ".[dev]"
pytest -q
```

Run the sample:

```bash
transaction-service --input data/sample_transactions.json --db demo.sqlite3 --fail-once TXN-2005 --retry-pending
```

Or without installing the console script:

```bash
PYTHONPATH=src python -m transaction_service.cli --input data/sample_transactions.json --db demo.sqlite3 --fail-once TXN-2005 --retry-pending
```

The command prints JSON containing per-record results, counts by status, persisted transactions, and processing events. Delete `demo.sqlite3` before a clean rerun.

## Sample scenario

The sample intentionally sends sequence 2 before sequence 1, repeats TXN-2002, includes an invalid negative amount, and includes a transaction configured for a simulated transient failure.

Expected business outcomes:
- TXN-2002 initially becomes `PENDING`, then processes after TXN-2001 is accepted and pending replay is run.
- The second TXN-2002 request is identified as a duplicate and has no second ledger effect.
- TXN-2004 is rejected by validation.
- TXN-2005 demonstrates retry behavior when invoked with `--fail-once TXN-2005`.
- The final balance for ACC-201 is INR 85 (100 credit - 40 debit + 25 credit).

## Use cases

### UC-01 — Submit and process a valid transaction
**Actor:** Upstream source / batch adapter  
**Preconditions:** Request has all mandatory fields and a valid account sequence.  
**Main flow:** Validate request; register as RECEIVED; mark PROCESSING; atomically write ledger, balance and sequence; mark PROCESSED; emit event.  
**Postcondition:** One ledger entry and one balance effect exist for the transaction.

### UC-02 — Detect a duplicate transaction
**Actor:** Upstream source / retrying client  
**Preconditions:** The transaction ID has already been registered or processed.  
**Main flow:** Look up transaction ID; compare payload fingerprint; return DUPLICATE if identical; reject as FAILED if the same ID has conflicting business data.  
**Postcondition:** No additional ledger or balance mutation occurs.

### UC-03 — Handle an out-of-order transaction
**Actor:** Upstream source / batch adapter  
**Preconditions:** A later sequence arrives before the expected sequence.  
**Main flow:** Persist the request; detect sequence gap; mark PENDING; process the missing sequence; replay pending work.  
**Postcondition:** Transactions are applied in contiguous per-account sequence order.

### UC-04 — Recover from a transient failure
**Actor:** Background processor  
**Preconditions:** A registered transaction encounters a transient dependency error.  
**Main flow:** Record RETRY_PENDING; retry within configured limit; apply business effect atomically once; mark PROCESSED, or mark FAILED after exhaustion.  
**Postcondition:** A retry cannot create a second ledger entry or balance update.

### UC-05 — Reject malformed or invalid input
**Actor:** Upstream source / batch adapter  
**Preconditions:** A record is missing a required field or violates validation rules.  
**Main flow:** Validate independently; record a validation failure event; return FAILED; continue processing the remaining batch records.  
**Postcondition:** Invalid data creates no ledger or balance effect.

### UC-06 — Prevent an invalid debit
**Actor:** Transaction processor  
**Preconditions:** Debit request is in sequence and account exists or can be initialized.  
**Main flow:** Check currency and available balance inside the database transaction; reject debit if it would overdraw; otherwise apply ledger and balance update.  
**Postcondition:** Balance never becomes negative due to a debit.

### UC-07 — Review operational processing results
**Actor:** Support engineer / operator  
**Preconditions:** One or more records have been submitted.  
**Main flow:** Read status counts, transaction attempts, error details and event history from the CLI output / SQLite store.  
**Postcondition:** Operator can distinguish processed, duplicate, pending, retrying and failed work.

## Design decisions and production-readiness notes

1. **Persistence:** SQLite is used for a zero-service local demo. Production should use a transactional relational database with migrations, backups and retention policies.
2. **Concurrency:** SQLite `BEGIN IMMEDIATE`, uniqueness constraints and a process-local lock protect this demo. A multi-instance service needs database-level concurrency control, optimistic versioning or row locks, plus an explicit work-claim/lease mechanism.
3. **Queue semantics:** Assume at-least-once delivery. Acknowledge only after durable state is committed. Use an outbox for downstream side effects and a dead-letter queue for exhausted messages.
4. **Ordering:** Confirm whether sequence numbers are global or per account, define gap timeout behavior, and alert on old pending records.
5. **Money:** Amounts are parsed and calculated with Python `Decimal` and persisted as decimal text in SQLite. A production database should use an appropriate fixed-precision `DECIMAL`/`NUMERIC` type or integer minor units, with currency-specific scale rules.
6. **Retries:** Add exponential backoff, jitter, retry classification, circuit breaking and operational replay controls.
7. **Observability:** Emit structured logs, metrics (throughput, latency, retries, duplicates, pending age, failure rate) and trace/correlation IDs. Avoid logging sensitive account data.
8. **Security:** Add authentication/authorization at the ingestion boundary, input size limits, secret management, encryption and audit retention as required.
9. **Scale:** Partition work by account or another ordering key; monitor hot partitions and database contention.
10. **API evolution:** Version the input contract and define behavior for unknown fields and currency precision.

## AI-assisted development disclosure

AI assistance was used to accelerate scaffolding, test-case design, and documentation. The implementation should be reviewed and executed by the candidate before submission. The included tests validate the central behaviors; the candidate should be prepared to explain idempotency keys, transaction boundaries, sequence gaps, retry classification, and production limitations.

## GitHub submission

1. Create a **public** repository on GitHub, for example `idempotent-transaction-service`.
2. Extract this ZIP and open a terminal in the extracted folder.
3. Run the tests and sample command above.
4. Initialize and push:

```bash
git init
git add .
git commit -m "Build idempotent transaction processing service"
git branch -M main
git remote add origin https://github.com/<YOUR-USERNAME>/idempotent-transaction-service.git
git push -u origin main
```

5. Submit the public repository URL to the recruiter. Do not commit database files, credentials, or personal data.

## Limitations to disclose

This is an assessment-sized reference implementation, not a production banking ledger. The sample uses a local SQLite database and simulated failures. Before submission, run the tests in your own environment, inspect the output, and be ready to discuss the listed trade-offs. In particular, add currency-specific scale enforcement and use a production database's fixed-precision decimal or integer-minor-unit representation before any real-money use.
