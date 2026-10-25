import threading
import time
from typing import Callable, Dict, List, Optional
from config import HEARTBEAT_TIMEOUT, MAX_TASK_RETRIES, TASK_LEASE_TIMEOUT
from models import (
    Task,
    TaskStatus,
    WorkerHeartbeat,
    WorkerInfo,
    WorkerLanguage,
    WorkerRegisterRequest,
    WorkerStatus,
)

class WorkerManager:
    def __init__(
        self,
        on_task_reclaim: Optional[Callable[[Task, str], None]] = None,
        on_task_dlq: Optional[Callable[[Task, str], None]] = None
    ):
        self.workers: Dict[str, WorkerInfo] = {}
        self.in_flight_tasks: Dict[str, Task] = {}  # task_id -> Task
        self._lock = threading.Lock()
        self.on_task_reclaim = on_task_reclaim
        self.on_task_dlq = on_task_dlq
        self._running = False
        self._watchdog_thread: Optional[threading.Thread] = None

    def start_watchdog(self):
        self._running = True
        self._watchdog_thread = threading.Thread(target=self._watchdog_loop, daemon=True)
        self._watchdog_thread.start()

    def stop_watchdog(self):
        self._running = False

    def register_worker(self, req: WorkerRegisterRequest) -> WorkerInfo:
        with self._lock:
            info = WorkerInfo(
                worker_id=req.worker_id,
                language=req.language,
                hostname=req.hostname,
                status=WorkerStatus.ONLINE,
                last_heartbeat=time.time(),
                concurrency_limit=req.concurrency_limit,
                active_tasks_count=0,
                completed_tasks_count=0,
                failed_tasks_count=0,
                cpu_percent=0.0,
                memory_percent=0.0,
                supported_tasks=req.supported_tasks,
                tags=req.tags
            )
            self.workers[req.worker_id] = info
            return info

    def process_heartbeat(self, hb: WorkerHeartbeat):
        with self._lock:
            if hb.worker_id in self.workers:
                worker = self.workers[hb.worker_id]
                worker.last_heartbeat = time.time()
                worker.cpu_percent = hb.cpu_percent
                worker.memory_percent = hb.memory_percent
                worker.active_tasks_count = hb.active_tasks_count
                worker.status = hb.status if hb.status != WorkerStatus.DEAD else WorkerStatus.ONLINE
            else:
                # Auto-register if worker reconnected
                self.workers[hb.worker_id] = WorkerInfo(
                    worker_id=hb.worker_id,
                    language=hb.language,
                    hostname="remote-host",
                    status=WorkerStatus.ONLINE,
                    last_heartbeat=time.time(),
                    cpu_percent=hb.cpu_percent,
                    memory_percent=hb.memory_percent,
                    active_tasks_count=hb.active_tasks_count
                )

    def track_in_flight(self, task: Task):
        with self._lock:
            self.in_flight_tasks[task.task_id] = task
            if task.assigned_worker_id and task.assigned_worker_id in self.workers:
                self.workers[task.assigned_worker_id].active_tasks_count += 1
                self.workers[task.assigned_worker_id].status = WorkerStatus.BUSY

    def complete_in_flight(self, task_id: str, success: bool = True) -> Optional[Task]:
        with self._lock:
            task = self.in_flight_tasks.pop(task_id, None)
            if task and task.assigned_worker_id and task.assigned_worker_id in self.workers:
                worker = self.workers[task.assigned_worker_id]
                worker.active_tasks_count = max(0, worker.active_tasks_count - 1)
                if worker.active_tasks_count == 0:
                    worker.status = WorkerStatus.ONLINE
                if success:
                    worker.completed_tasks_count += 1
                else:
                    worker.failed_tasks_count += 1
            return task

    def get_worker(self, worker_id: str) -> Optional[WorkerInfo]:
        with self._lock:
            return self.workers.get(worker_id)

    def get_all_workers(self) -> List[WorkerInfo]:
        with self._lock:
            now = time.time()
            res = []
            for w in self.workers.values():
                # Mark status as DEAD if timed out
                if now - w.last_heartbeat > HEARTBEAT_TIMEOUT and w.status != WorkerStatus.DEAD:
                    w.status = WorkerStatus.DEAD
                res.append(w.model_copy())
            return res

    def _watchdog_loop(self):
        """Monitors worker heartbeats and detects dead workers to trigger fault tolerance."""
        while self._running:
            try:
                time.sleep(2)
                now = time.time()
                dead_worker_ids = set()

                with self._lock:
                    for w_id, w in self.workers.items():
                        if now - w.last_heartbeat > HEARTBEAT_TIMEOUT:
                            if w.status != WorkerStatus.DEAD:
                                w.status = WorkerStatus.DEAD
                                dead_worker_ids.add(w_id)

                    # Check for in-flight tasks leased to dead workers
                    reclaim_list: List[Task] = []
                    dlq_list: List[Task] = []

                    for task_id, task in list(self.in_flight_tasks.items()):
                        worker_dead = task.assigned_worker_id in dead_worker_ids
                        lease_expired = (task.claimed_at is not None and (now - task.claimed_at) > TASK_LEASE_TIMEOUT)

                        if worker_dead or lease_expired:
                            self.in_flight_tasks.pop(task_id, None)
                            task.retries_count += 1
                            if task.retries_count <= task.max_retries:
                                reclaim_list.append(task)
                            else:
                                dlq_list.append(task)

                # Execute callbacks outside of lock to prevent deadlocks
                for task in reclaim_list:
                    reason = f"Worker {task.assigned_worker_id} heartbeat timeout. Auto-reclaiming task for retry {task.retries_count}/{task.max_retries}."
                    if self.on_task_reclaim:
                        self.on_task_reclaim(task, reason)

                for task in dlq_list:
                    reason = f"Task failed after {task.max_retries} retries due to worker failure. Routed to Dead-Letter Queue (DLQ)."
                    if self.on_task_dlq:
                        self.on_task_dlq(task, reason)

            except Exception as e:
                print(f"[Watchdog Error]: {e}")
