import json
import sqlite3
import time
from typing import Any, Dict, List, Optional
from config import DB_PATH
from models import Task, TaskPriority, TaskStatus, WorkerLanguage

class ResultManager:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self):
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        # Enable WAL mode for high concurrency
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def _init_db(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    task_type TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    effective_priority REAL NOT NULL,
                    required_language TEXT,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    claimed_at REAL,
                    completed_at REAL,
                    assigned_worker_id TEXT,
                    assigned_worker_language TEXT,
                    retries_count INTEGER NOT NULL DEFAULT 0,
                    max_retries INTEGER NOT NULL DEFAULT 3,
                    result TEXT,
                    error_message TEXT,
                    execution_time_seconds REAL
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS audit_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    message TEXT NOT NULL,
                    timestamp REAL NOT NULL
                )
            """)
            conn.commit()

    def log_event(self, task_id: str, event_type: str, message: str):
        with self._get_connection() as conn:
            conn.execute(
                "INSERT INTO audit_logs (task_id, event_type, message, timestamp) VALUES (?, ?, ?, ?)",
                (task_id, event_type, message, time.time())
            )
            conn.commit()

    def save_task(self, task: Task):
        with self._get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO tasks (
                    task_id, task_type, priority, effective_priority, required_language,
                    payload, status, created_at, claimed_at, completed_at,
                    assigned_worker_id, assigned_worker_language, retries_count,
                    max_retries, result, error_message, execution_time_seconds
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                task.task_id,
                task.task_type,
                int(task.priority),
                float(task.effective_priority),
                task.required_language.value if task.required_language else None,
                json.dumps(task.payload),
                task.status.value,
                task.created_at,
                task.claimed_at,
                task.completed_at,
                task.assigned_worker_id,
                task.assigned_worker_language.value if task.assigned_worker_language else None,
                task.retries_count,
                task.max_retries,
                json.dumps(task.result) if task.result is not None else None,
                task.error_message,
                task.execution_time_seconds
            ))
            conn.commit()

    def get_task(self, task_id: str) -> Optional[Task]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,))
            row = cursor.fetchone()
            if not row:
                return None
            return self._row_to_task(row)

    def get_all_tasks(self, limit: int = 150) -> List[Task]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM tasks ORDER BY created_at DESC LIMIT ?", (limit,))
            rows = cursor.fetchall()
            return [self._row_to_task(row) for row in rows]

    def get_tasks_by_status(self, status: TaskStatus) -> List[Task]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM tasks WHERE status = ? ORDER BY created_at ASC", (status.value,))
            rows = cursor.fetchall()
            return [self._row_to_task(row) for row in rows]

    def get_system_stats(self) -> Dict[str, Any]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT status, COUNT(*) as count 
                FROM tasks 
                GROUP BY status
            """)
            status_counts = {row["status"]: row["count"] for row in cursor.fetchall()}
            
            cursor.execute("""
                SELECT AVG(execution_time_seconds) as avg_exec_time 
                FROM tasks 
                WHERE status = 'COMPLETED' AND execution_time_seconds IS NOT NULL
            """)
            avg_exec = cursor.fetchone()["avg_exec_time"] or 0.0

            cursor.execute("SELECT COUNT(*) as total FROM tasks")
            total = cursor.fetchone()["total"]

            return {
                "total_tasks": total,
                "pending": status_counts.get("PENDING", 0),
                "claimed": status_counts.get("CLAIMED", 0),
                "running": status_counts.get("RUNNING", 0),
                "completed": status_counts.get("COMPLETED", 0),
                "failed": status_counts.get("FAILED", 0),
                "dead_letter": status_counts.get("DEAD_LETTER", 0),
                "avg_execution_time_seconds": round(avg_exec, 3)
            }

    def _row_to_task(self, row: sqlite3.Row) -> Task:
        return Task(
            task_id=row["task_id"],
            task_type=row["task_type"],
            priority=TaskPriority(row["priority"]),
            effective_priority=row["effective_priority"],
            required_language=WorkerLanguage(row["required_language"]) if row["required_language"] else None,
            payload=json.loads(row["payload"]) if row["payload"] else {},
            status=TaskStatus(row["status"]),
            created_at=row["created_at"],
            claimed_at=row["claimed_at"],
            completed_at=row["completed_at"],
            assigned_worker_id=row["assigned_worker_id"],
            assigned_worker_language=WorkerLanguage(row["assigned_worker_language"]) if row["assigned_worker_language"] else None,
            retries_count=row["retries_count"],
            max_retries=row["max_retries"],
            result=json.loads(row["result"]) if row["result"] is not None else None,
            error_message=row["error_message"],
            execution_time_seconds=row["execution_time_seconds"]
        )
