import time
from models import Task, TaskPriority, TaskStatus, WorkerInfo, WorkerLanguage, WorkerRegisterRequest
from server.queue_manager import QueueManager
from server.worker_manager import WorkerManager

def test_fault_tolerance_reclaim():
    reclaimed_tasks = []
    dlq_tasks = []

    def on_reclaim(task, reason):
        reclaimed_tasks.append((task, reason))

    def on_dlq(task, reason):
        dlq_tasks.append((task, reason))

    wm = WorkerManager(on_task_reclaim=on_reclaim, on_task_dlq=on_dlq)
    wm.start_watchdog()

    try:
        # Register worker
        w_req = WorkerRegisterRequest(
            worker_id="crash-worker-test",
            language=WorkerLanguage.PYTHON,
            hostname="test-node"
        )
        wm.register_worker(w_req)

        # Assign task
        task = Task(task_type="prime_sieve", priority=TaskPriority.NORMAL, max_retries=2)
        task.assigned_worker_id = "crash-worker-test"
        task.claimed_at = time.time()
        wm.track_in_flight(task)

        assert len(wm.in_flight_tasks) == 1

        # Simulate worker dying (simulate last heartbeat far in past)
        wm.workers["crash-worker-test"].last_heartbeat = time.time() - 20.0

        # Wait for watchdog to detect dead worker and reclaim task
        print("[*] Waiting for watchdog loop to detect heartbeat timeout...")
        timeout_wait = 6.0
        start = time.time()
        while time.time() - start < timeout_wait:
            if len(reclaimed_tasks) > 0:
                break
            time.sleep(0.5)

        assert len(reclaimed_tasks) == 1, "Task was not auto-reclaimed after worker timeout!"
        reclaimed_task, reason = reclaimed_tasks[0]
        assert reclaimed_task.retries_count == 1
        print(f"[OK] Task successfully auto-reclaimed: {reason}")

    finally:
        wm.stop_watchdog()

if __name__ == "__main__":
    test_fault_tolerance_reclaim()
    print("\n[OK] All Fault Tolerance tests passed!")
