import threading
import time
from typing import Dict, List, Optional
from config import AGING_INTERVAL_SECONDS, DEFAULT_SCHEDULING_ALGORITHM
from models import Task, TaskPriority, TaskStatus, WorkerInfo, WorkerLanguage

class QueueManager:
    def __init__(self, scheduling_algorithm: str = DEFAULT_SCHEDULING_ALGORITHM):
        self.scheduling_algorithm = scheduling_algorithm
        self._queue: List[Task] = []
        self._lock = threading.Lock()

    def set_scheduling_algorithm(self, algorithm: str):
        with self._lock:
            self.scheduling_algorithm = algorithm

    def enqueue(self, task: Task):
        with self._lock:
            task.status = TaskStatus.PENDING
            task.effective_priority = float(task.priority)
            self._queue.append(task)

    def requeue(self, task: Task, front: bool = True):
        """Re-queue an orphaned or retried task."""
        with self._lock:
            task.status = TaskStatus.PENDING
            task.assigned_worker_id = None
            task.assigned_worker_language = None
            task.claimed_at = None
            if front:
                # Give high priority to retried tasks
                task.effective_priority = float(task.priority) + 1.0
                self._queue.insert(0, task)
            else:
                self._queue.append(task)

    def dequeue_for_worker(self, worker: WorkerInfo) -> Optional[Task]:
        """
        Picks the best task for the requesting worker based on:
        1. Language runtime requirement (PYTHON / JAVA / ANY)
        2. Worker supported tasks
        3. Selected scheduling algorithm (FIFO, Priority with Aging, etc.)
        """
        with self._lock:
            if not self._queue:
                return None

            # Apply aging dynamically before picking
            now = time.time()
            if self.scheduling_algorithm == "PRIORITY_WITH_AGING":
                for t in self._queue:
                    wait_time = now - t.created_at
                    age_bonus = (wait_time / AGING_INTERVAL_SECONDS) * 0.5
                    t.effective_priority = float(t.priority) + age_bonus

            # Filter candidates that this worker is capable of running
            matching_indices = []
            for idx, task in enumerate(self._queue):
                # Check runtime language constraint
                if task.required_language and task.required_language != worker.language:
                    continue
                # Check supported task types if specified by worker
                if worker.supported_tasks and task.task_type not in worker.supported_tasks:
                    continue
                matching_indices.append(idx)

            if not matching_indices:
                return None

            # Sort candidate tasks according to the chosen algorithm
            if self.scheduling_algorithm == "FIFO":
                best_idx = matching_indices[0]  # First in, first out
            elif self.scheduling_algorithm == "PRIORITY_WITH_AGING":
                # Max effective priority, then oldest created_at
                best_idx = max(
                    matching_indices,
                    key=lambda i: (self._queue[i].effective_priority, -self._queue[i].created_at)
                )
            elif self.scheduling_algorithm == "LOAD_AWARE":
                # Choose task with lowest estimated_workload; tie‑break with higher priority
                def score(i):
                    task = self._queue[i]
                    # lower workload is better, so we use negative for max()
                    return (-task.estimated_workload, task.priority.value)
                best_idx = max(matching_indices, key=score)
            else:
                # Default fallback
                best_idx = matching_indices[0]

            chosen_task = self._queue.pop(best_idx)
            chosen_task.status = TaskStatus.CLAIMED
            chosen_task.claimed_at = now
            chosen_task.assigned_worker_id = worker.worker_id
            chosen_task.assigned_worker_language = worker.language
            return chosen_task

    def dequeue(self) -> Optional[Task]:
        """Dequeues the next pending task from the front of the queue."""
        with self._lock:
            if not self._queue:
                return None
            return self._queue.pop(0)

    def remove(self, task_id: str) -> Optional[Task]:
        with self._lock:
            for idx, task in enumerate(self._queue):
                if task.task_id == task_id:
                    return self._queue.pop(idx)
            return None

    def size(self) -> int:
        with self._lock:
            return len(self._queue)

    def get_snapshot(self) -> List[Dict]:
        with self._lock:
            return [
                {
                    "task_id": t.task_id,
                    "task_type": t.task_type,
                    "priority": t.priority.name,
                    "effective_priority": round(t.effective_priority, 2),
                    "required_language": t.required_language.value if t.required_language else "ANY",
                    "created_at": t.created_at,
                    "age_seconds": round(time.time() - t.created_at, 1),
                    "retries_count": t.retries_count
                }
                for t in self._queue
            ]
