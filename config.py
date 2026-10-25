import os
from pathlib import Path

# Base Paths
BASE_DIR = Path(__file__).resolve().parent
STORAGE_DIR = BASE_DIR / "storage"
MEDIA_DIR = STORAGE_DIR / "media"
DB_PATH = STORAGE_DIR / "tasks.db"

# Ensure directories exist
STORAGE_DIR.mkdir(parents=True, exist_ok=True)
MEDIA_DIR.mkdir(parents=True, exist_ok=True)

# Server Configuration
SERVER_HOST = os.getenv("SERVER_HOST", "0.0.0.0")
SERVER_PORT = int(os.getenv("SERVER_PORT", 8000))
SERVER_URL = f"http://127.0.0.1:{SERVER_PORT}"

# Fault Tolerance & Heartbeat Settings (in seconds)
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", 3))  # Workers send heartbeat every 3s
HEARTBEAT_TIMEOUT = int(os.getenv("HEARTBEAT_TIMEOUT", 8))    # Worker considered dead if no heartbeat for 8s
TASK_LEASE_TIMEOUT = int(os.getenv("TASK_LEASE_TIMEOUT", 15)) # Task reclaimed if in-progress worker dies
MAX_TASK_RETRIES = int(os.getenv("MAX_TASK_RETRIES", 3))       # Move to DLQ after 3 failures

# Scheduling Algorithms
DEFAULT_SCHEDULING_ALGORITHM = "PRIORITY_WITH_AGING"  # Options: FIFO, PRIORITY_WITH_AGING, LOAD_AWARE
AGING_INTERVAL_SECONDS = 10  # Priority boosts every 10s pending to prevent starvation
