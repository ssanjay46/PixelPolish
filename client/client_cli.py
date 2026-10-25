import argparse
import json
import os
import sys
import time

# Add root to pythonpath
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from client.client_sdk import TaskQueueClient
from models import TaskPriority, WorkerLanguage

def print_banner():
    print("""
===========================================================
  DISTRIBUTED TASK QUEUE - COMMAND LINE INTERFACE (CLI)
  Modules: Client (1) | Server (2) | Queue (3) | Workers (4)
===========================================================
""")

def handle_submit(client: TaskQueueClient, args):
    priority_map = {1: TaskPriority.LOW, 2: TaskPriority.NORMAL, 3: TaskPriority.HIGH, 4: TaskPriority.CRITICAL}
    priority = priority_map.get(args.priority, TaskPriority.NORMAL)
    req_lang = WorkerLanguage(args.runtime) if args.runtime else None
    
    payload = {}
    if args.payload:
        try:
            payload = json.loads(args.payload)
        except Exception as e:
            print(f"[Error] Invalid JSON payload: {e}")
            return

    # Default payload helpers
    if args.type == "image_process" and "filter" not in payload:
        payload["filter"] = "EDGE_DETECT"
    elif args.type == "matrix_multiply" and "dimension" not in payload:
        payload["dimension"] = 200
    elif args.type == "prime_sieve" and "limit" not in payload:
        payload["limit"] = 150000
    elif args.type == "hash_solve" and "difficulty" not in payload:
        payload["difficulty"] = 4

    print(f"[*] Submitting Task '{args.type}' (Priority: {priority.name}, Runtime: {req_lang.value if req_lang else 'ANY'})...")
    res = client.submit_task(
        task_type=args.type,
        payload=payload,
        priority=priority,
        required_language=req_lang
    )
    task_id = res["task_id"]
    print(f"[OK] Task successfully enqueued! Task ID: {task_id}")

    if args.wait:
        print("[*] Waiting for worker node to finish processing...")
        try:
            finished = client.wait_for_task(task_id, timeout=30.0)
            print(f"\n================ TASK COMPLETED ================")
            print(f"Status:         {finished['status']}")
            print(f"Worker Node:    {finished.get('assigned_worker_id')} ({finished.get('assigned_worker_language')})")
            print(f"Execution Time: {finished.get('execution_time_seconds')}s")
            print(f"Result:         {json.dumps(finished.get('result'), indent=2)}")
            print(f"================================================")
        except Exception as e:
            print(f"[!] Error or timeout waiting for task: {e}")

def handle_status(client: TaskQueueClient, task_id: str):
    try:
        t = client.get_task(task_id)
        print(f"\n--- Task {task_id} Details ---")
        print(f"Type:       {t.get('task_type')}")
        print(f"Status:     {t.get('status')}")
        print(f"Priority:   {t.get('priority')}")
        print(f"Worker:     {t.get('assigned_worker_id')} [{t.get('assigned_worker_language')}]")
        print(f"Duration:   {t.get('execution_time_seconds')}s")
        print(f"Retries:    {t.get('retries_count')}/{t.get('max_retries')}")
        if t.get('result'):
            print(f"Result:     {json.dumps(t['result'], indent=2)}")
        if t.get('error_message'):
            print(f"Error:      {t['error_message']}")
    except Exception as e:
        print(f"[!] Failed to fetch task status: {e}")

def handle_workers(client: TaskQueueClient):
    try:
        workers = client.get_workers()
        print(f"\nActive Worker Topology ({len(workers)} registered):")
        print(f"{'Worker ID':<25} {'Runtime':<10} {'Status':<12} {'CPU%':<8} {'RAM%':<8} {'Completed':<10}")
        print("-" * 75)
        for w in workers:
            print(f"{w['worker_id']:<25} {w['language']:<10} {w['status']:<12} {w['cpu_percent']:<8.1f} {w['memory_percent']:<8.1f} {w['completed_tasks_count']:<10}")
    except Exception as e:
        print(f"[!] Failed to fetch workers: {e}")

def handle_stats(client: TaskQueueClient):
    try:
        stats = client.get_stats()
        print("\n--- Distributed Task Queue Metrics ---")
        for k, v in stats.items():
            print(f"  {k:<30}: {v}")
    except Exception as e:
        print(f"[!] Failed to fetch stats: {e}")

def main():
    print_banner()
    parser = argparse.ArgumentParser(description="Distributed Task Queue CLI")
    parser.add_argument("--server", default="http://127.0.0.1:8000", help="Primary Server URL")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Submit subcommand
    submit_p = subparsers.add_parser("submit", help="Submit a new computational task")
    submit_p.add_argument("--type", required=True, choices=["image_process", "matrix_multiply", "prime_sieve", "hash_solve", "word_count"])
    submit_p.add_argument("--priority", type=int, default=2, choices=[1, 2, 3, 4], help="1=LOW, 2=NORMAL, 3=HIGH, 4=CRITICAL")
    submit_p.add_argument("--runtime", choices=["PYTHON", "JAVA"], default=None, help="Force specific worker language")
    submit_p.add_argument("--payload", default=None, help="JSON string of task payload")
    submit_p.add_argument("--wait", action="store_true", help="Block until task finishes and print result")

    # Status subcommand
    status_p = subparsers.add_parser("status", help="Get status of a task")
    status_p.add_argument("task_id", help="Task UUID")

    # Workers subcommand
    subparsers.add_parser("workers", help="List all registered worker nodes")

    # Stats subcommand
    subparsers.add_parser("stats", help="Get queue metrics and system stats")

    args = parser.parse_args()
    client = TaskQueueClient(server_url=args.server)

    if args.command == "submit":
        handle_submit(client, args)
    elif args.command == "status":
        handle_status(client, args.task_id)
    elif args.command == "workers":
        handle_workers(client)
    elif args.command == "stats":
        handle_stats(client)

if __name__ == "__main__":
    main()
