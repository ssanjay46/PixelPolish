# media_ingestor.py – discovers media files, estimates workload, and submits jobs
"""
Universal Media Ingestor
----------------------
* Scans the configured MEDIA_DIR for supported image/video files.
* Estimates workload based on file size (MB) and, for videos, duration.
* Submits a task of type ``media_process`` with a ``task_subtype`` payload field
  (``enhance``, ``transcode`` or ``extract``).
* Leverages the extended ``TaskCreateRequest`` schema which now contains an
  ``estimated_workload`` field – the scheduler can use this for LOAD_AWARE
  distribution.
"""

import os
import pathlib
import argparse
from typing import List

from client.client_sdk import TaskQueueClient
from models import TaskPriority, WorkerLanguage
from config import MEDIA_DIR

SUPPORTED_EXTS = {".png", ".jpg", ".jpeg", ".mp4", ".mov", ".avi"}

def discover_media(root: pathlib.Path) -> List[pathlib.Path]:
    """Return a list of all media files under ``root`` matching SUPPORTED_EXTS."""
    return [p for p in root.rglob("*") if p.suffix.lower() in SUPPORTED_EXTS]

def estimate_workload(file_path: pathlib.Path) -> float:
    """Simple heuristic: file size in MB. For videos we add a small bonus.

    Returns a float (MB) that the scheduler treats as the workload.
    """
    size_mb = file_path.stat().st_size / (1024 * 1024)
    if file_path.suffix.lower() in {".mp4", ".mov", ".avi"}:
        # Very rough duration bonus – 0.1 MB per second of video length.
        # We use ffprobe if available; if it fails we fall back to size only.
        try:
            import subprocess, json, shlex
            cmd = f"ffprobe -v error -show_entries format=duration -of json \"{file_path}\""
            out = subprocess.check_output(shlex.split(cmd), stderr=subprocess.DEVNULL)
            info = json.loads(out)
            duration = float(info["format"]["duration"])
            size_mb += duration * 0.1
        except Exception:
            pass
    return round(size_mb, 2)

def infer_subtype(file_path: pathlib.Path, args) -> str:
    if file_path.suffix.lower() in {".png", ".jpg", ".jpeg"}:
        return args.subtype or "enhance"
    return args.subtype or "transcode"

def main():
    parser = argparse.ArgumentParser(description="Upload media files and queue processing tasks.")
    parser.add_argument("--server", default="http://127.0.0.1:8000", help="Task queue server URL")
    parser.add_argument("--priority", default="NORMAL", choices=["LOW", "NORMAL", "HIGH", "CRITICAL"], help="Task priority")
    parser.add_argument("--subtype", default=None, help="Force a specific sub‑task (enhance|transcode|extract)")
    parser.add_argument("--filter", default="EDGE_DETECT", help="Image filter for 'enhance' subtype")
    parser.add_argument("--resolution", default="1280x720", help="Target resolution for video transcode")
    parser.add_argument("--crf", default="23", help="FFmpeg CRF quality factor for transcode")
    parser.add_argument("--count", type=int, default=10, help="Number of frames to extract (extract subtype)")
    args = parser.parse_args()

    client = TaskQueueClient(args.server)
    media_files = discover_media(MEDIA_DIR)
    print(f"[Ingestor] Found {len(media_files)} media files in {MEDIA_DIR}")

    for f in media_files:
        subtype = infer_subtype(f, args)
        workload = estimate_workload(f)
        payload = {
            "filename": f.name,
            "task_subtype": subtype,
            "filter": args.filter,
            "resolution": args.resolution,
            "crf": args.crf,
            "count": args.count,
        }
        res = client.submit_task(
            task_type="media_process",
            payload=payload,
            priority=TaskPriority[args.priority],
            required_language=WorkerLanguage.PYTHON,
            estimated_workload=workload,
        )
        print(f"Submitted {f.name} ({subtype}, {workload} MB) → task {res['task_id'][:8]}")

if __name__ == "__main__":
    main()
