import time
from typing import Any, Dict, List, Optional
import requests

from models import TaskPriority, WorkerLanguage

class TaskQueueClient:
    """Client SDK for interacting with the Distributed Task Queue Server."""
    
    def __init__(self, server_url: str = "http://127.0.0.1:8000"):
        self.server_url = server_url.rstrip("/")

    def submit_task(
        self,
        task_type: str,
        payload: Dict[str, Any] = None,
        priority: TaskPriority = TaskPriority.NORMAL,
        required_language: Optional[WorkerLanguage] = None,
        max_retries: int = 3,
        estimated_workload: float = None
    ) -> Dict[str, Any]:
        url = f"{self.server_url}/api/tasks"
        body = {
            "task_type": task_type,
            "priority": int(priority),
            "required_language": required_language.value if required_language else None,
            "payload": payload or {},
            "max_retries": max_retries
        }
        res = requests.post(url, json=body, timeout=5)
        res.raise_for_status()
        return res.json()

    def get_task(self, task_id: str) -> Dict[str, Any]:
        url = f"{self.server_url}/api/tasks/{task_id}"
        res = requests.get(url, timeout=5)
        res.raise_for_status()
        return res.json()

    def wait_for_task(self, task_id: str, poll_interval: float = 0.5, timeout: float = 60.0) -> Dict[str, Any]:
        start = time.time()
        while time.time() - start < timeout:
            task = self.get_task(task_id)
            status = task.get("status")
            if status in ["COMPLETED", "FAILED", "DEAD_LETTER"]:
                return task
            time.sleep(poll_interval)
        raise TimeoutError(f"Task {task_id} did not finish within {timeout}s")

    def get_workers(self) -> List[Dict[str, Any]]:
        res = requests.get(f"{self.server_url}/api/workers", timeout=5)
        res.raise_for_status()
        return res.json()

    def get_stats(self) -> Dict[str, Any]:
        res = requests.get(f"{self.server_url}/api/stats", timeout=5)
        res.raise_for_status()
        return res.json()
