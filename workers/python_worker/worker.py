import argparse
import os
import platform
import socket
import sys
import threading
import time
import uuid
import psutil
import requests

# Adjust path to import config & models
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from config import HEARTBEAT_INTERVAL, SERVER_URL
from models import TaskStatus, WorkerHeartbeat, WorkerLanguage, WorkerRegisterRequest, WorkerStatus
from workers.python_worker.tasks import TASK_REGISTRY

class PythonWorker:
    def __init__(self, server_url: str = SERVER_URL, worker_id: str = None, concurrency: int = 1):
        self.server_url = server_url.rstrip("/")
        self.worker_id = worker_id or f"python-worker-{socket.gethostname()[:8]}-{uuid.uuid4().hex[:4]}"
        self.concurrency = concurrency
        self.hostname = socket.gethostname()
        self.running = False
        self.active_tasks_count = 0
        self.registered = False
        self._lock = threading.Lock()

    def register(self):
        url = f"{self.server_url}/api/workers/register"
        req = WorkerRegisterRequest(
            worker_id=self.worker_id,
            language=WorkerLanguage.PYTHON,
            hostname=self.hostname,
            concurrency_limit=self.concurrency,
            supported_tasks=list(TASK_REGISTRY.keys()),
            tags=["python", "ffmpeg", "transcoding", "vision"]
        )
        try:
            res = requests.post(url, json=req.model_dump(), timeout=5)
            res.raise_for_status()
            self.registered = True
            print(f"[Worker {self.worker_id}] Successfully registered with Primary Server at {self.server_url}")
        except Exception:
            self.registered = False

    def heartbeat_loop(self):
        url = f"{self.server_url}/api/workers/heartbeat"
        while self.running:
            try:
                cpu = psutil.cpu_percent(interval=None)
                ram = psutil.virtual_memory().percent
                with self._lock:
                    count = self.active_tasks_count

                hb = WorkerHeartbeat(
                    worker_id=self.worker_id,
                    language=WorkerLanguage.PYTHON,
                    cpu_percent=cpu,
                    memory_percent=ram,
                    active_tasks_count=count,
                    status=WorkerStatus.BUSY if count > 0 else WorkerStatus.ONLINE
                )
                requests.post(url, json=hb.model_dump(), timeout=3)
            except Exception as e:
                # Network glitch or server busy
                pass
            time.sleep(HEARTBEAT_INTERVAL)

    def execute_task(self, task_data: dict):
        task_id = task_data["task_id"]
        task_type = task_data["task_type"]
        payload = task_data.get("payload", {})
        
        print(f"\n[Worker {self.worker_id}] Processing Task [{task_type}] ID: {task_id[:8]}...")
        start_time = time.time()
        status = TaskStatus.COMPLETED
        result = None
        error_msg = None

        try:
            handler = TASK_REGISTRY.get(task_type)
            if not handler:
                raise ValueError(f"No task handler registered for type '{task_type}'")
            result = handler(payload)
            duration = time.time() - start_time
            print(f"[Worker {self.worker_id}] Task {task_id[:8]} completed in {duration:.3f}s")
        except Exception as e:
            duration = time.time() - start_time
            status = TaskStatus.FAILED
            error_msg = str(e)
            print(f"[Worker {self.worker_id}] Task {task_id[:8]} failed: {e}")

        # Submit result back to primary server
        self.submit_result(task_id, status, result, error_msg, duration)

    def submit_result(self, task_id: str, status: TaskStatus, result: any, error_msg: str, duration: float):
        url = f"{self.server_url}/api/tasks/result"
        payload = {
            "task_id": task_id,
            "worker_id": self.worker_id,
            "status": status.value,
            "result": result,
            "error_message": error_msg,
            "execution_time_seconds": round(duration, 3)
        }
        try:
            res = requests.post(url, json=payload, timeout=5)
            res.raise_for_status()
        except Exception as e:
            print(f"[Worker {self.worker_id}] Failed to submit result for {task_id[:8]}: {e}")

    def run(self):
        self.running = True
        self.register()

        # Start heartbeat thread
        hb_thread = threading.Thread(target=self.heartbeat_loop, daemon=True)
        hb_thread.start()

        poll_url = f"{self.server_url}/api/tasks/poll?worker_id={self.worker_id}"
        print(f"[Worker {self.worker_id}] Polling for tasks. Press Ctrl+C to terminate...")

        while self.running:
            if not self.registered:
                self.register()
            try:
                res = requests.post(poll_url, timeout=5)
                if res.status_code == 200:
                    if not self.registered:
                        self.register()
                    data = res.json()
                    task = data.get("task")
                    if task:
                        with self._lock:
                            self.active_tasks_count += 1
                        try:
                            self.execute_task(task)
                        finally:
                            with self._lock:
                                self.active_tasks_count = max(0, self.active_tasks_count - 1)
                        continue  # Immediately check for next task if any
            except requests.exceptions.ConnectionError:
                print(f"[Worker {self.worker_id}] Primary Server not detected at {self.server_url}. (Did you start 'run_server.bat'?) Retrying in 3s...")
                time.sleep(3)
                continue
            except Exception as e:
                # Poll failure, pause
                pass

            time.sleep(1.0)

    def stop(self):
        print(f"\n[Worker {self.worker_id}] Stopping gracefully...")
        self.running = False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed Task Queue - Python Worker Node")
    parser.add_argument("--server", default=SERVER_URL, help="Primary Server URL (e.g. http://192.168.1.10:8000)")
    parser.add_argument("--id", default=None, help="Custom worker ID")
    parser.add_argument("--concurrency", type=int, default=1, help="Worker concurrency limit")
    args = parser.parse_args()

    worker = PythonWorker(server_url=args.server, worker_id=args.id, concurrency=args.concurrency)
    try:
        worker.run()
    except KeyboardInterrupt:
        worker.stop()
