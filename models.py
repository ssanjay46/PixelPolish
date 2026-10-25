import enum
import time
import uuid
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

class TaskStatus(str, enum.Enum):
    PENDING = "PENDING"
    CLAIMED = "CLAIMED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    DEAD_LETTER = "DEAD_LETTER"

class TaskPriority(int, enum.Enum):
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4

class WorkerLanguage(str, enum.Enum):
    PYTHON = "PYTHON"
    JAVA = "JAVA"
    OTHER = "OTHER"

class WorkerStatus(str, enum.Enum):
    ONLINE = "ONLINE"
    BUSY = "BUSY"
    DEAD = "DEAD"
    DRAINED = "DRAINED"

class TaskCreateRequest(BaseModel):
    task_type: str = Field(..., description="Type of task: image_blur, prime_sieve, matrix_multiply, word_count, hash_solve")
    priority: TaskPriority = Field(default=TaskPriority.NORMAL, description="Task Priority (1=LOW to 4=CRITICAL)")
    required_language: Optional[WorkerLanguage] = Field(default=None, description="Require a specific worker runtime (PYTHON or JAVA)")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Task arguments/inputs")
    max_retries: int = Field(default=3, description="Maximum number of retry attempts before moving to DLQ")
    estimated_workload: float = Field(default=0.0, description="Estimated computational effort (e.g., file size in MB)")

class Task(BaseModel):
    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str
    priority: TaskPriority = TaskPriority.NORMAL
    effective_priority: float = 2.0  # Used by Aging algorithm
    # Estimated computational effort (e.g., file size in MB, or heuristic)
    estimated_workload: float = 0.0
    required_language: Optional[WorkerLanguage] = None
    payload: Dict[str, Any] = Field(default_factory=dict)
    status: TaskStatus = TaskStatus.PENDING
    created_at: float = Field(default_factory=time.time)
    claimed_at: Optional[float] = None
    completed_at: Optional[float] = None
    assigned_worker_id: Optional[str] = None
    assigned_worker_language: Optional[WorkerLanguage] = None
    retries_count: int = 0
    max_retries: int = 3
    result: Optional[Any] = None
    error_message: Optional[str] = None
    execution_time_seconds: Optional[float] = None

class WorkerRegisterRequest(BaseModel):
    worker_id: str
    language: WorkerLanguage
    hostname: str
    concurrency_limit: int = 1
    supported_tasks: List[str] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)

class WorkerHeartbeat(BaseModel):
    worker_id: str
    language: WorkerLanguage
    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    active_tasks_count: int = 0
    status: WorkerStatus = WorkerStatus.ONLINE
    timestamp: float = Field(default_factory=time.time)

class WorkerInfo(BaseModel):
    worker_id: str
    language: WorkerLanguage
    hostname: str
    status: WorkerStatus = WorkerStatus.ONLINE
    last_heartbeat: float = Field(default_factory=time.time)
    concurrency_limit: int = 1
    active_tasks_count: int = 0
    completed_tasks_count: int = 0
    failed_tasks_count: int = 0
    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    supported_tasks: List[str] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)

class TaskResultSubmission(BaseModel):
    task_id: str
    worker_id: str
    status: TaskStatus
    result: Optional[Any] = None
    error_message: Optional[str] = None
    execution_time_seconds: Optional[float] = None
