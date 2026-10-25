"""
Distributed Financial Fraud Detection — PaySim Automated Demo
==============================================================
Demonstrates the full pipeline using PaySim mobile-money transaction
patterns. Submits 6 carefully crafted batches covering all fraud
archetypes present in the PaySim dataset:

  Batch 1 — Normal PAYMENT/CASH_IN mix (baseline, no fraud expected)
  Batch 2 — TRANSFER with BALANCE_DRAIN + GHOST_DESTINATION (Python)
  Batch 3 — CASH_OUT account emptying sweep (Java rules engine)
  Batch 4 — STEP_VELOCITY: 5 txns in same hour (Java velocity rule)
  Batch 5 — Mixed high-value TRANSFER fraud (Python Z-score)
  Batch 6 — Compound fraud: drain + ghost + high-value (CRITICAL priority)

No CSV download required — all transactions are embedded here.

Usage:
    python client/demo_fraud.py
    or double-click: run_fraud_demo.bat
"""

import os
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from client.client_sdk import TaskQueueClient
from models import TaskPriority, WorkerLanguage

SERVER_URL = "http://127.0.0.1:8000"

# ── PaySim-format sample batches ──────────────────────────────────────────────
# Columns: step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig,
#          nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud
DEMO_BATCHES = [
    {
        "name": "Normal PAYMENT & CASH_IN Baseline (No Fraud Expected)",
        "task_type": "fraud_detect",
        "worker_lang": WorkerLanguage.PYTHON,
        "priority": TaskPriority.NORMAL,
        "transactions": [
            {"step":"1","type":"PAYMENT","amount":"9839.64","nameOrig":"C1231006815","oldbalanceOrg":"170136.0","newbalanceOrig":"160296.36","nameDest":"M1979787155","oldbalanceDest":"0.0","newbalanceDest":"0.0","isFraud":"0","isFlaggedFraud":"0"},
            {"step":"1","type":"PAYMENT","amount":"1864.28","nameOrig":"C1666544295","oldbalanceOrg":"21249.0","newbalanceOrig":"19384.72","nameDest":"M2044282225","oldbalanceDest":"0.0","newbalanceDest":"0.0","isFraud":"0","isFlaggedFraud":"0"},
            {"step":"1","type":"CASH_IN","amount":"181.00","nameOrig":"C553264065","oldbalanceOrg":"0.0","newbalanceOrig":"181.0","nameDest":"C38997010","oldbalanceDest":"21182.0","newbalanceDest":"0.0","isFraud":"0","isFlaggedFraud":"0"},
            {"step":"1","type":"PAYMENT","amount":"11668.14","nameOrig":"C2048537720","oldbalanceOrg":"41554.0","newbalanceOrig":"29885.86","nameDest":"M1230701703","oldbalanceDest":"0.0","newbalanceDest":"0.0","isFraud":"0","isFlaggedFraud":"0"},
            {"step":"1","type":"PAYMENT","amount":"7817.71","nameOrig":"C90045638","oldbalanceOrg":"53860.0","newbalanceOrig":"46042.29","nameDest":"M573487274","oldbalanceDest":"0.0","newbalanceDest":"0.0","isFraud":"0","isFlaggedFraud":"0"},
        ]
    },
    {
        "name": "TRANSFER Balance-Drain + Ghost Destination (Python Signal 1+2)",
        "task_type": "fraud_detect",
        "worker_lang": WorkerLanguage.PYTHON,
        "priority": TaskPriority.CRITICAL,
        "transactions": [
            # Fraud: sender balance drained to 0, dest had 0 and still 0 (money-mule)
            {"step":"206","type":"TRANSFER","amount":"339682.13","nameOrig":"C1305486145","oldbalanceOrg":"339682.13","newbalanceOrig":"0.0","nameDest":"C553324332","oldbalanceDest":"0.0","newbalanceDest":"0.0","isFraud":"1","isFlaggedFraud":"0"},
            {"step":"206","type":"TRANSFER","amount":"70001.00","nameOrig":"C840083671","oldbalanceOrg":"70001.0","newbalanceOrig":"0.0","nameDest":"C1456654776","oldbalanceDest":"0.0","newbalanceDest":"0.0","isFraud":"1","isFlaggedFraud":"0"},
            # Normal transfer (control group)
            {"step":"207","type":"TRANSFER","amount":"1200.00","nameOrig":"C293897897","oldbalanceOrg":"8600.0","newbalanceOrig":"7400.0","nameDest":"C1148868624","oldbalanceDest":"3000.0","newbalanceDest":"4200.0","isFraud":"0","isFlaggedFraud":"0"},
            # Another fraud: sender fully drained
            {"step":"208","type":"TRANSFER","amount":"503761.72","nameOrig":"C1839843580","oldbalanceOrg":"503761.72","newbalanceOrig":"0.0","nameDest":"C2085375449","oldbalanceDest":"0.0","newbalanceDest":"0.0","isFraud":"1","isFlaggedFraud":"1"},
        ]
    },
    {
        "name": "CASH_OUT Account Emptying Sweep (Java Rule 2: CASH_OUT_TO_ZERO)",
        "task_type": "fraud_rules",
        "worker_lang": WorkerLanguage.JAVA,
        "priority": TaskPriority.CRITICAL,
        "transactions": [
            # Fraud: full account cashout to zero
            {"step":"341","type":"CASH_OUT","amount":"175866.50","nameOrig":"C1392462509","oldbalanceOrg":"175866.50","newbalanceOrig":"0.0","nameDest":"C791907198","oldbalanceDest":"0.0","newbalanceDest":"175866.50","isFraud":"1","isFlaggedFraud":"0"},
            {"step":"341","type":"CASH_OUT","amount":"92406.01","nameOrig":"C1905524798","oldbalanceOrg":"92406.01","newbalanceOrig":"0.0","nameDest":"C532572579","oldbalanceDest":"0.0","newbalanceDest":"92406.01","isFraud":"1","isFlaggedFraud":"0"},
            # Normal cashout (partial)
            {"step":"342","type":"CASH_OUT","amount":"5000.00","nameOrig":"C1201556775","oldbalanceOrg":"30000.0","newbalanceOrig":"25000.0","nameDest":"C1610602916","oldbalanceDest":"0.0","newbalanceDest":"5000.0","isFraud":"0","isFlaggedFraud":"0"},
            {"step":"343","type":"CASH_OUT","amount":"2500.00","nameOrig":"C1048009085","oldbalanceOrg":"12500.0","newbalanceOrig":"10000.0","nameDest":"C1562207031","oldbalanceDest":"0.0","newbalanceDest":"2500.0","isFraud":"0","isFlaggedFraud":"0"},
            # Fraud: full drain again
            {"step":"344","type":"CASH_OUT","amount":"44312.00","nameOrig":"C473215938","oldbalanceOrg":"44312.0","newbalanceOrig":"0.0","nameDest":"C1896456708","oldbalanceDest":"0.0","newbalanceDest":"44312.0","isFraud":"1","isFlaggedFraud":"0"},
        ]
    },
    {
        "name": "STEP_VELOCITY Attack: 6 TRANSFERs in Same Hour (Java Rule 3)",
        "task_type": "fraud_rules",
        "worker_lang": WorkerLanguage.JAVA,
        "priority": TaskPriority.HIGH,
        "transactions": [
            # Same step=74 — rapid-fire transfers from different accounts
            {"step":"74","type":"TRANSFER","amount":"215310.30","nameOrig":"C1434659796","oldbalanceOrg":"215310.30","newbalanceOrig":"0.0","nameDest":"C523956750","oldbalanceDest":"0.0","newbalanceDest":"215310.30","isFraud":"1","isFlaggedFraud":"0"},
            {"step":"74","type":"TRANSFER","amount":"311685.89","nameOrig":"C1598963052","oldbalanceOrg":"311685.89","newbalanceOrig":"0.0","nameDest":"C1396198422","oldbalanceDest":"0.0","newbalanceDest":"311685.89","isFraud":"1","isFlaggedFraud":"0"},
            {"step":"74","type":"TRANSFER","amount":"4005.00","nameOrig":"C1756742023","oldbalanceOrg":"22400.0","newbalanceOrig":"18395.0","nameDest":"C428024567","oldbalanceDest":"0.0","newbalanceDest":"4005.0","isFraud":"0","isFlaggedFraud":"0"},
            {"step":"74","type":"TRANSFER","amount":"1200.00","nameOrig":"C1838093786","oldbalanceOrg":"9600.0","newbalanceOrig":"8400.0","nameDest":"C1596569203","oldbalanceDest":"0.0","newbalanceDest":"1200.0","isFraud":"0","isFlaggedFraud":"0"},
            {"step":"74","type":"TRANSFER","amount":"180123.45","nameOrig":"C2019990850","oldbalanceOrg":"180123.45","newbalanceOrig":"0.0","nameDest":"C1940907085","oldbalanceDest":"0.0","newbalanceDest":"180123.45","isFraud":"1","isFlaggedFraud":"0"},
            {"step":"74","type":"TRANSFER","amount":"9500.00","nameOrig":"C784890458","oldbalanceOrg":"60000.0","newbalanceOrig":"50500.0","nameDest":"C1139776353","oldbalanceDest":"0.0","newbalanceDest":"9500.0","isFraud":"0","isFlaggedFraud":"0"},
        ]
    },
    {
        "name": "High-Value TRANSFER Outliers: Z-Score Anomaly (Python Signal 3)",
        "task_type": "fraud_detect",
        "worker_lang": WorkerLanguage.PYTHON,
        "priority": TaskPriority.HIGH,
        "transactions": [
            # Mix of small normal and huge fraudulent transfers
            {"step":"100","type":"TRANSFER","amount":"850.00","nameOrig":"C1021591744","oldbalanceOrg":"5000.0","newbalanceOrig":"4150.0","nameDest":"C2006098497","oldbalanceDest":"2000.0","newbalanceDest":"2850.0","isFraud":"0","isFlaggedFraud":"0"},
            {"step":"101","type":"TRANSFER","amount":"1250.00","nameOrig":"C1547819025","oldbalanceOrg":"8000.0","newbalanceOrig":"6750.0","nameDest":"C1879604539","oldbalanceDest":"0.0","newbalanceDest":"1250.0","isFraud":"0","isFlaggedFraud":"0"},
            {"step":"102","type":"TRANSFER","amount":"960.50","nameOrig":"C1030547706","oldbalanceOrg":"4000.0","newbalanceOrig":"3039.5","nameDest":"C1234567890","oldbalanceDest":"500.0","newbalanceDest":"1460.5","isFraud":"0","isFlaggedFraud":"0"},
            # Outlier: massive fraud transfer (z-score >> 3)
            {"step":"103","type":"TRANSFER","amount":"9876543.21","nameOrig":"C1925602748","oldbalanceOrg":"9876543.21","newbalanceOrig":"0.0","nameDest":"C685295960","oldbalanceDest":"0.0","newbalanceDest":"0.0","isFraud":"1","isFlaggedFraud":"1"},
            {"step":"104","type":"TRANSFER","amount":"1100.00","nameOrig":"C1547819025","oldbalanceOrg":"9000.0","newbalanceOrig":"7900.0","nameDest":"C1530372611","oldbalanceDest":"200.0","newbalanceDest":"1300.0","isFraud":"0","isFlaggedFraud":"0"},
        ]
    },
    {
        "name": "Compound Fraud: Drain + Ghost + High-Value (CRITICAL Priority)",
        "task_type": "fraud_detect",
        "worker_lang": WorkerLanguage.PYTHON,
        "priority": TaskPriority.CRITICAL,
        "transactions": [
            # Classic PaySim fraud pattern: transfer all, dest stays at 0
            {"step":"470","type":"TRANSFER","amount":"10000000.00","nameOrig":"C1655156016","oldbalanceOrg":"10000000.0","newbalanceOrig":"0.0","nameDest":"C1996339462","oldbalanceDest":"0.0","newbalanceDest":"0.0","isFraud":"1","isFlaggedFraud":"1"},
            {"step":"471","type":"TRANSFER","amount":"5400000.00","nameOrig":"C1474818027","oldbalanceOrg":"5400000.0","newbalanceOrig":"0.0","nameDest":"C1900366749","oldbalanceDest":"0.0","newbalanceDest":"0.0","isFraud":"1","isFlaggedFraud":"1"},
            {"step":"472","type":"CASH_OUT","amount":"300000.00","nameOrig":"C1448010498","oldbalanceOrg":"300000.0","newbalanceOrig":"0.0","nameDest":"C1598963052","oldbalanceDest":"0.0","newbalanceDest":"300000.0","isFraud":"1","isFlaggedFraud":"0"},
            # Legitimate small transaction mixed in
            {"step":"472","type":"PAYMENT","amount":"500.00","nameOrig":"C1285285164","oldbalanceOrg":"15000.0","newbalanceOrig":"14500.0","nameDest":"M1242974403","oldbalanceDest":"0.0","newbalanceDest":"0.0","isFraud":"0","isFlaggedFraud":"0"},
        ]
    },
]


def run_fraud_demo():
    client = TaskQueueClient(SERVER_URL)

    print("=" * 70)
    print("  DISTRIBUTED FINANCIAL FRAUD DETECTION — PAYSIM DEMO")
    print("  Sathyabama Institute of Science and Technology — CSE PT1")
    print("  Dataset: PaySim Mobile Money Transactions (6.3M rows)")
    print("=" * 70)
    print()

    # 1. Server check
    try:
        stats = client.get_stats()
        print(f"[OK] Server online at {SERVER_URL}")
        print(f"     Queue size    : {stats.get('queue_size', 0)}")
        print(f"     Active workers: {stats.get('active_workers', 0)}")
        print(f"     Algorithm     : {stats.get('scheduling_algorithm', 'N/A')}\n")
    except Exception as e:
        print(f"[FAIL] Cannot connect to server: {e}")
        print("       Start with: run_server.bat")
        return

    # 2. Workers
    workers = client.get_workers()
    active  = [w for w in workers if w.get("status") != "DEAD"]
    print(f"[*] Active worker nodes ({len(active)} online):")
    for w in active:
        print(
            f"    [{w['language']:<6}] {w['worker_id']:<30} "
            f"CPU: {w.get('cpu_percent',0):.1f}%  "
            f"RAM: {w.get('memory_percent',0):.1f}%"
        )
    print()
    if not active:
        print("[WARN] No active workers. Start with:")
        print("       run_python_worker.bat  +  run_java_worker.bat\n")

    # 3. Submit
    print(f"[*] Submitting {len(DEMO_BATCHES)} PaySim fraud detection tasks...\n")
    submitted = []

    for idx, b in enumerate(DEMO_BATCHES):
        batch_id = f"paysim-demo-{idx + 1:02d}"
        payload  = {"batch_id": batch_id, "transactions": b["transactions"]}
        try:
            res = client.submit_task(
                task_type=b["task_type"],
                priority=b["priority"],
                required_language=b["worker_lang"],
                payload=payload,
            )
            task_id = res["task_id"]
            submitted.append({
                "task_id":   task_id,
                "name":      b["name"],
                "task_type": b["task_type"],
                "priority":  b["priority"],
                "tx_count":  len(b["transactions"]),
            })
            print(
                f"  -> [{b['priority'].name:<8}] [{b['task_type']:<14}] "
                f"{b['name'][:48]}"
            )
            print(f"              Task ID: {task_id[:8]}...  ({len(b['transactions'])} tx)")
        except Exception as e:
            print(f"  x  Failed to submit batch {idx + 1}: {e}")
        time.sleep(0.1)

    # 4. Monitor
    print(f"\n[*] Monitoring {len(submitted)} tasks...\n")
    start_wait = time.time()
    results    = {}
    timeout    = 90.0

    while len(results) < len(submitted) and (time.time() - start_wait) < timeout:
        for item in submitted:
            tid = item["task_id"]
            if tid not in results:
                try:
                    info = client.get_task(tid)
                    if info.get("status") in ["COMPLETED", "FAILED", "DEAD_LETTER"]:
                        results[tid] = info
                        status = info.get("status")
                        exec_t = info.get("execution_time_seconds", 0) or 0
                        lang   = info.get("assigned_worker_language", "N/A")
                        icon   = "✓" if status == "COMPLETED" else "✗"
                        print(
                            f"  [{icon}] {item['name'][:50]}\n"
                            f"       {lang:<6} | {exec_t:.3f}s | {status}"
                        )
                except Exception:
                    pass
        time.sleep(0.5)

    # 5. Final Report
    print("\n" + "=" * 70)
    print("  PAYSIM FRAUD DETECTION — FINAL REPORT")
    print("=" * 70)

    grand_flagged = 0
    grand_true_fraud = 0
    grand_amount_at_risk = 0.0

    header = f"  {'Batch':<48} {'Engine':<14} {'Status':<10} {'t(s)':<7}"
    print(header)
    print("  " + "-" * 65)

    for item in submitted:
        tid  = item["task_id"]
        info = results.get(tid, {})
        status = info.get("status", "TIMEOUT")
        exec_t = f"{info.get('execution_time_seconds', 0) or 0:.3f}"
        engine = item["task_type"].upper()

        r = info.get("result") or {}
        if isinstance(r, dict):
            flagged  = r.get("total_flagged", r.get("total_rule_violations", 0)) or 0
            tf_batch = r.get("true_fraud_in_batch", 0) or 0
            at_risk  = float(r.get("total_amount_at_risk", 0) or 0)
            grand_flagged        += flagged
            grand_true_fraud     += tf_batch
            grand_amount_at_risk += at_risk

        print(f"  {item['name'][:48]:<48} {engine:<14} {status:<10} {exec_t:<7}")

    print("  " + "=" * 65)
    print(f"  Total Fraud Flags Raised    : {grand_flagged}")
    print(f"  True Fraud Txns in Batches  : {grand_true_fraud}")
    print(f"  Total Amount at Risk        : ${grand_amount_at_risk:>16,.2f}")
    print(f"  Tasks Completed             : {len(results)}/{len(submitted)}")
    print("=" * 70)
    print(f"\n[OK] Full results in SQLite DB — dashboard at {SERVER_URL}")
    print(
        f"\n[TIP] To run against the real 6.3M-row PaySim dataset:\n"
        f"      .venv\\Scripts\\python.exe client\\fraud_ingestor.py "
        f"--batch-size 500 --max-batches 50\n"
    )


if __name__ == "__main__":
    run_fraud_demo()
