"""
Distributed Financial Fraud Detection — PaySim Transaction Stream Ingestor
===========================================================================
Reads the PaySim Mobile Money Transactions dataset
(PS_20174392719_1491204439457_log.csv) from storage/, slices it into
micro-batches, and submits each batch as an independent task to the
Distributed Task Queue.

Dataset stats:
  Total rows  : 6,362,620 transactions
  Fraud rows  : 8,213 (0.129%)
  Tx types    : PAYMENT, TRANSFER, CASH_OUT, CASH_IN, DEBIT
  Fraud types : TRANSFER and CASH_OUT only

Task routing:
  fraud_detect  -> Python Worker  (Balance-Drain + Ghost-Destination + Z-score)
  fraud_rules   -> Java Worker    (Transfer/CashOut-to-Zero + Velocity + HighValue)

Priority mapping (by batch max amount):
  CRITICAL  -> any tx amount > 200,000
  HIGH      -> any tx amount >  50,000
  NORMAL    -> everything else

Usage:
    python client/fraud_ingestor.py [--batch-size 500] [--max-batches 20]
                                    [--server http://127.0.0.1:8000]
                                    [--mode both|python|java]
                                    [--type-filter TRANSFER,CASH_OUT]
"""

import csv
import os
import sys
import time
import argparse

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from client.client_sdk import TaskQueueClient
from models import TaskPriority, WorkerLanguage

PAYSIM_CSV = os.path.join(
    os.path.dirname(__file__), "..", "storage",
    "PS_20174392719_1491204439457_log.csv"
)

# PaySim columns
PAYSIM_COLUMNS = [
    "step", "type", "amount", "nameOrig", "oldbalanceOrg",
    "newbalanceOrig", "nameDest", "oldbalanceDest", "newbalanceDest",
    "isFraud", "isFlaggedFraud"
]


def load_paysim_stream(
    csv_path: str,
    max_rows: int,
    type_filter: list[str] | None,
) -> list[dict]:
    """
    Stream rows from PaySim CSV, optionally filtering by transaction type.
    Returns up to max_rows rows.
    """
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if type_filter and row.get("type", "").upper() not in type_filter:
                continue
            rows.append(row)
            if max_rows > 0 and len(rows) >= max_rows:
                break
    return rows


def compute_batch_priority(batch: list[dict]) -> TaskPriority:
    """
    Priority by maximum transaction amount in the batch.
    PaySim fraudulent sweeps often involve amounts in the hundreds of thousands.
    """
    max_amt = 0.0
    for tx in batch:
        try:
            max_amt = max(max_amt, float(tx.get("amount", 0) or 0))
        except (ValueError, TypeError):
            pass
    if max_amt > 200_000:
        return TaskPriority.CRITICAL
    elif max_amt > 50_000:
        return TaskPriority.HIGH
    return TaskPriority.NORMAL


def ingest(
    csv_path: str,
    batch_size: int,
    max_batches: int,
    server_url: str,
    mode: str,
    type_filter: list[str] | None,
) -> list[str]:
    client = TaskQueueClient(server_url)

    # Verify server
    try:
        stats = client.get_stats()
        print(f"[Ingestor] Connected to server at {server_url}")
        print(f"           Active workers : {stats.get('active_workers', 0)}")
        print(f"           Queue size     : {stats.get('queue_size', 0)}\n")
    except Exception as e:
        print(f"[ERROR] Cannot reach server: {e}")
        sys.exit(1)

    # Verify dataset
    if not os.path.isfile(csv_path):
        print(f"[ERROR] PaySim dataset not found at: {csv_path}")
        print(
            "        Expected file: PS_20174392719_1491204439457_log.csv\n"
            "        Place it in the storage/ folder.\n"
        )
        sys.exit(1)

    # Calculate rows needed
    max_rows = batch_size * max_batches if max_batches > 0 else 0
    type_label = ",".join(type_filter) if type_filter else "ALL"

    print(f"[Ingestor] Loading PaySim dataset (filter: {type_label})...")
    rows = load_paysim_stream(csv_path, max_rows, type_filter)
    print(f"           Loaded {len(rows):,} transactions")

    batches = [rows[i: i + batch_size] for i in range(0, len(rows), batch_size)]
    if max_batches > 0:
        batches = batches[:max_batches]

    total_batches = len(batches)
    print(
        f"\n[Ingestor] Submitting {total_batches} batch(es) × {batch_size} tx\n"
        f"           Mode: {mode.upper()}\n"
    )

    submitted_ids = []

    for idx, batch in enumerate(batches):
        batch_id = f"paysim-{idx + 1:04d}-of-{total_batches:04d}"

        if mode == "python":
            task_type, required_lang = "fraud_detect", WorkerLanguage.PYTHON
        elif mode == "java":
            task_type, required_lang = "fraud_rules", WorkerLanguage.JAVA
        else:
            # Alternate: even -> Python statistical, odd -> Java rules
            if idx % 2 == 0:
                task_type, required_lang = "fraud_detect", WorkerLanguage.PYTHON
            else:
                task_type, required_lang = "fraud_rules", WorkerLanguage.JAVA

        priority = compute_batch_priority(batch)
        payload  = {"batch_id": batch_id, "transactions": batch}

        try:
            res = client.submit_task(
                task_type=task_type,
                priority=priority,
                required_language=required_lang,
                payload=payload,
            )
            task_id = res["task_id"]
            submitted_ids.append(task_id)
            print(
                f"  -> [{priority.name:<8}] [{task_type:<14}] "
                f"Batch {idx + 1:>4}/{total_batches} "
                f"({len(batch)} tx) — Task {task_id[:8]}"
            )
        except Exception as e:
            print(f"  x  Failed to submit batch {idx + 1}: {e}")

        time.sleep(0.04)  # brief stagger

    print(f"\n[Ingestor] Submitted {len(submitted_ids)} tasks.")
    print(f"           Monitor: {server_url}\n")
    return submitted_ids


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="PaySim Fraud Detection — Transaction Stream Ingestor"
    )
    parser.add_argument("--csv", default=PAYSIM_CSV,
                        help="Path to PaySim CSV")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="Transactions per batch (default: 500)")
    parser.add_argument("--max-batches", type=int, default=20,
                        help="Max batches to submit, 0=all (default: 20)")
    parser.add_argument("--server", default="http://127.0.0.1:8000",
                        help="Primary server URL")
    parser.add_argument("--mode", choices=["both", "python", "java"],
                        default="both",
                        help="Worker mode: both (alternating), python, java")
    parser.add_argument("--type-filter", default="TRANSFER,CASH_OUT",
                        help="Comma-separated PaySim tx types to include "
                             "(default: TRANSFER,CASH_OUT — fraud-prone only)")
    args = parser.parse_args()

    tf = [t.strip().upper() for t in args.type_filter.split(",") if t.strip()] \
         if args.type_filter else None

    ingest(
        csv_path=args.csv,
        batch_size=args.batch_size,
        max_batches=args.max_batches,
        server_url=args.server,
        mode=args.mode,
        type_filter=tf,
    )
