import os
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from client.client_sdk import TaskQueueClient
from models import TaskPriority, WorkerLanguage

def run_automated_demo():
    client = TaskQueueClient("http://127.0.0.1:8000")

    print("==================================================================")
    print(" DISTRIBUTED TASK QUEUE - AUTOMATED EVALUATION & DEMONSTRATION")
    print(" Sathyabama Institute of Science and Technology - CSE PT1")
    print("==================================================================\n")

    # 1. Check Server Connection
    try:
        stats = client.get_stats()
        print(f"[OK] Connected to Primary Server at {client.server_url}")
        print(f"    Current Queue Size:        {stats['queue_size']}")
        print(f"    Active Workers:            {stats['active_workers']}")
        print(f"    Active Scheduling Engine:  {stats['scheduling_algorithm']}\n")
    except Exception as e:
        print(f"[FAIL] Error connecting to server: {e}")
        print("Please start the server first using 'run_server.bat'!")
        return

    # 2. Check Workers
    workers = client.get_workers()
    active = [w for w in workers if w["status"] != "DEAD"]
    print(f"[*] Detected {len(active)} active worker nodes:")
    for w in active:
        print(f"    - [{w['language']:<6}] ID: {w['worker_id']:<24} (Concurrency: {w['concurrency_limit']})")
    print()

    # 3. Define Diverse Batch of Computational Tasks
    demo_batch = [
        {
            "name": "Image Edge Detection (Sobel/Laplacian)",
            "type": "image_process",
            "priority": TaskPriority.CRITICAL,
            "lang": WorkerLanguage.PYTHON,
            "payload": {"filter": "EDGE_DETECT"}
        },
        {
            "name": "Java Parallel Matrix Multiplication (300x300)",
            "type": "matrix_multiply",
            "priority": TaskPriority.HIGH,
            "lang": WorkerLanguage.JAVA,
            "payload": {"dimension": 300}
        },
        {
            "name": "Prime Number Sieve (up to 300,000)",
            "type": "prime_sieve",
            "priority": TaskPriority.NORMAL,
            "lang": None,  # Any available worker
            "payload": {"limit": 300000}
        },
        {
            "name": "SHA-256 Cryptographic Mining Challenge",
            "type": "hash_solve",
            "priority": TaskPriority.HIGH,
            "lang": None,
            "payload": {"difficulty": 4}
        },
        {
            "name": "Image Gaussian Blur Filtering",
            "type": "image_process",
            "priority": TaskPriority.NORMAL,
            "lang": WorkerLanguage.PYTHON,
            "payload": {"filter": "GAUSSIAN_BLUR"}
        },
        {
            "name": "Distributed Text NLP Frequency Counter",
            "type": "word_count",
            "priority": TaskPriority.LOW,
            "lang": WorkerLanguage.PYTHON,
            "payload": {"text": "Distributed systems utilize asynchronous worker nodes to balance loads across LAN and cloud networks."}
        }
    ]

    print(f"[*] Dispatching {len(demo_batch)} heterogeneous computational tasks to queue...\n")
    submitted_tasks = []

    for item in demo_batch:
        res = client.submit_task(
            task_type=item["type"],
            priority=item["priority"],
            required_language=item["lang"],
            payload=item["payload"]
        )
        task_id = res["task_id"]
        submitted_tasks.append((task_id, item["name"], item["priority"]))
        lang_str = item["lang"].value if item["lang"] else "ANY"
        print(f"  -> Enqueued: [{item['priority'].name:<8}] [{lang_str:<6}] {item['name']} (ID: {task_id[:8]})")

    print("\n[*] Asynchronously monitoring execution across worker pool...")
    start_wait = time.time()
    results = {}

    while len(results) < len(submitted_tasks) and time.time() - start_wait < 45.0:
        for t_id, name, prio in submitted_tasks:
            if t_id not in results:
                info = client.get_task(t_id)
                if info.get("status") in ["COMPLETED", "FAILED", "DEAD_LETTER"]:
                    results[t_id] = info
                    print(f"  [DONE] [{info.get('assigned_worker_language', 'N/A'):<6}] {name} in {info.get('execution_time_seconds')}s by {info.get('assigned_worker_id')}")
        time.sleep(0.5)

    print("\n" + "=" * 80)
    print(" DEMONSTRATION SUMMARY REPORT")
    print("=" * 80)
    print(f"{'Task Name':<38} {'Priority':<10} {'Runtime':<8} {'Time (s)':<10} {'Status':<10}")
    print("-" * 80)

    for t_id, name, prio in submitted_tasks:
        info = results.get(t_id, {})
        status = info.get("status", "TIMEOUT")
        exec_time = f"{info.get('execution_time_seconds', 0.0):.2f}" if info.get("execution_time_seconds") else "--"
        lang = info.get("assigned_worker_language", "--")
        print(f"{name:<38} {prio.name:<10} {lang:<8} {exec_time:<10} {status:<10}")

    print("=" * 80)
    print("\n[OK] All results stored in SQLite database and visible in real-time web dashboard.")
    print(f"    Open dashboard in browser: http://localhost:8000\n")

if __name__ == "__main__":
    run_automated_demo()
