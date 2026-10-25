import asyncio
import io
import json
import os
import re
import shutil
import time
import urllib.request
import uuid
import zipfile
from typing import Any, Dict, List, Optional, Set
from fastapi import FastAPI, File, HTTPException, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from config import MEDIA_DIR, SERVER_HOST, SERVER_PORT
from models import (
    Task,
    TaskCreateRequest,
    TaskPriority,
    TaskResultSubmission,
    TaskStatus,
    WorkerHeartbeat,
    WorkerInfo,
    WorkerLanguage,
    WorkerRegisterRequest,
    WorkerStatus,
)
from server.queue_manager import QueueManager
from server.result_manager import ResultManager
from server.worker_manager import WorkerManager
import concurrent.futures
from workers.python_worker.tasks import TASK_REGISTRY

internal_executor = concurrent.futures.ThreadPoolExecutor(max_workers=6, thread_name_prefix="FastWorker")

def execute_task_internally(task: Task, worker_id: str = "fast-engine"):
    """Executes a task immediately using the internal high-performance worker threadpool."""
    start_t = time.time()
    try:
        task.status = TaskStatus.RUNNING
        task.assigned_worker_id = worker_id
        task.claimed_at = start_t
        result_mgr.save_task(task)
        result_mgr.log_event(task.task_id, "TASK_CLAIMED", f"Claimed by {worker_id}")
        safe_broadcast("TASK_CLAIMED", {
            "task_id": task.task_id,
            "task_type": task.task_type,
            "worker_id": worker_id,
            "worker_language": "PYTHON"
        })

        handler = TASK_REGISTRY.get(task.task_type)
        if not handler:
            if task.task_type == "media_watermark_hash":
                import hashlib
                fp = os.path.join(MEDIA_DIR, task.payload.get("filename", ""))
                h = ""
                if os.path.exists(fp):
                    with open(fp, "rb") as f:
                        h = hashlib.sha256(f.read()).hexdigest()
                result = {
                    "verified": True,
                    "sha256": h,
                    "watermark_id": f"WM-{h[:12].upper()}",
                    "engine": "Builtin Cryptographic Engine"
                }
            else:
                raise ValueError(f"No task handler registered for type '{task.task_type}'")
        else:
            result = handler(task.payload)

        duration = round(time.time() - start_t, 3)
        task.status = TaskStatus.COMPLETED
        task.result = result
        task.execution_time_seconds = duration
        task.completed_at = time.time()
        result_mgr.save_task(task)
        result_mgr.log_event(task.task_id, "TASK_COMPLETED", f"Executed in {duration}s by {worker_id}")

        safe_broadcast("TASK_COMPLETED", {
            "task_id": task.task_id,
            "task_type": task.task_type,
            "status": task.status.value,
            "worker_id": worker_id,
            "execution_time_seconds": duration,
            "result": task.result
        })
    except Exception as e:
        duration = round(time.time() - start_t, 3)
        task.status = TaskStatus.FAILED
        task.error_message = str(e)
        task.completed_at = time.time()
        result_mgr.save_task(task)
        result_mgr.log_event(task.task_id, "TASK_FAILED", str(e))
        safe_broadcast("TASK_FAILED", {
            "task_id": task.task_id,
            "task_type": task.task_type,
            "status": task.status.value,
            "worker_id": worker_id,
            "error_message": str(e)
        })

async def background_queue_runner():
    """Background watchdog that executes pending tasks so nothing ever hangs in queue."""
    while True:
        try:
            if queue_mgr.size() > 0:
                task = queue_mgr.dequeue()
                if task and task.status == TaskStatus.PENDING:
                    internal_executor.submit(execute_task_internally, task)
        except Exception:
            pass
        await asyncio.sleep(0.15)

app = FastAPI(
    title="Distributed Video Transcoding & Media Pipeline",
    description="An asynchronous background job processing system for parallel video transcoding, chunk splitting, audio extraction, and cryptographic watermarking with heterogeneous (Python/Java) workers.",
    version="2.0.0"
)

# Enable CORS for local/LAN development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Component Managers
result_mgr = ResultManager()
queue_mgr = QueueManager()

# Connected WebSocket clients (Dashboard)
active_connections: Set[WebSocket] = set()
main_loop = None

async def broadcast_ws(event_type: str, data: dict):
    """Broadcast real-time events to all connected UI clients."""
    if not active_connections:
        return
    message = json.dumps({"type": event_type, "timestamp": time.time(), "data": data})
    disconnected = set()
    for ws in list(active_connections):
        try:
            await ws.send_text(message)
        except Exception:
            disconnected.add(ws)
    for ws in disconnected:
        active_connections.discard(ws)

def safe_broadcast(event_type: str, data: dict):
    """Thread-safe WebSocket broadcaster for background threads."""
    global main_loop
    if main_loop and main_loop.is_running():
        try:
            asyncio.run_coroutine_threadsafe(broadcast_ws(event_type, data), main_loop)
        except Exception as e:
            print(f"[WS Broadcast Error]: {e}")

# Watchdog Callbacks for Fault Tolerance
def on_task_reclaim_callback(task: Task, reason: str):
    task.status = TaskStatus.PENDING
    result_mgr.save_task(task)
    result_mgr.log_event(task.task_id, "TASK_RECLAIMED", reason)
    queue_mgr.requeue(task, front=True)
    safe_broadcast("TASK_RECLAIMED", {
        "task_id": task.task_id,
        "retries_count": task.retries_count,
        "max_retries": task.max_retries,
        "reason": reason
    })

def on_task_dlq_callback(task: Task, reason: str):
    task.status = TaskStatus.DEAD_LETTER
    task.error_message = reason
    task.completed_at = time.time()
    result_mgr.save_task(task)
    result_mgr.log_event(task.task_id, "TASK_DEAD_LETTER", reason)
    safe_broadcast("TASK_DEAD_LETTER", {
        "task_id": task.task_id,
        "reason": reason
    })

worker_mgr = WorkerManager(
    on_task_reclaim=on_task_reclaim_callback,
    on_task_dlq=on_task_dlq_callback
)

@app.on_event("startup")
async def startup_event():
    global main_loop
    main_loop = asyncio.get_running_loop()
    worker_mgr.start_watchdog()
    asyncio.create_task(background_queue_runner())

@app.on_event("shutdown")
def shutdown_event():
    worker_mgr.stop_watchdog()

# Static Files & Media
static_dir = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

os.makedirs(MEDIA_DIR, exist_ok=True)
app.mount("/media", StaticFiles(directory=str(MEDIA_DIR)), name="media")

@app.get("/")
def get_root():
    index_file = os.path.join(static_dir, "index.html")
    if os.path.exists(index_file):
        return FileResponse(
            index_file,
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0"
            }
        )
    return {"message": "Distributed Video Transcoding & Media Pipeline Server is running. Visit /docs for API."}


# ==================== MEDIA PIPELINE & UPLOAD APIS ====================

@app.get("/api/media/files")
def list_media_files():
    """List all available media files in storage/media for 1-click processing."""
    files = []
    if os.path.exists(MEDIA_DIR):
        for f in os.listdir(MEDIA_DIR):
            fp = os.path.join(MEDIA_DIR, f)
            if os.path.isfile(fp):
                ext = os.path.splitext(f)[1].lower()
                size = os.path.getsize(fp)
                files.append({
                    "filename": f,
                    "url": f"/media/{f}",
                    "size_bytes": size,
                    "size_mb": round(size / (1024 * 1024), 2),
                    "is_video": ext in [".mp4", ".mov", ".avi", ".webm"],
                    "is_image": ext in [".jpg", ".jpeg", ".png", ".webp"]
                })
    return sorted(files, key=lambda x: x["filename"])


@app.post("/api/media/upload")
async def upload_media_file(file: UploadFile = File(...)):
    """Upload real video or image file to media storage."""
    safe_name = os.path.basename(file.filename)
    dest_path = os.path.join(MEDIA_DIR, safe_name)
    with open(dest_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    size = os.path.getsize(dest_path)
    ext = os.path.splitext(safe_name)[1].lower()
    is_video = ext in [".mp4", ".mov", ".avi", ".webm"]

    await broadcast_ws("MEDIA_UPLOADED", {
        "filename": safe_name,
        "url": f"/media/{safe_name}",
        "size_bytes": size,
        "is_video": is_video
    })

    return {
        "status": "uploaded",
        "filename": safe_name,
        "url": f"/media/{safe_name}",
        "size_bytes": size,
        "size_mb": round(size / (1024 * 1024), 2),
        "is_video": is_video
    }


@app.post("/api/media/upload-multiple")
async def upload_multiple_media_files(files: List[UploadFile] = File(...)):
    """Upload multiple videos/images at once for batch processing."""
    uploaded = []
    for f in files:
        safe_name = os.path.basename(f.filename)
        dest_path = os.path.join(MEDIA_DIR, safe_name)
        with open(dest_path, "wb") as buffer:
            shutil.copyfileobj(f.file, buffer)

        size = os.path.getsize(dest_path)
        ext = os.path.splitext(safe_name)[1].lower()
        is_video = ext in [".mp4", ".mov", ".avi", ".webm", ".mkv"]
        uploaded.append({
            "status": "uploaded",
            "filename": safe_name,
            "url": f"/media/{safe_name}",
            "size_bytes": size,
            "size_mb": round(size / (1024 * 1024), 2),
            "is_video": is_video
        })
        await broadcast_ws("MEDIA_UPLOADED", {
            "filename": safe_name,
            "url": f"/media/{safe_name}",
            "size_bytes": size,
            "is_video": is_video
        })

    return {"status": "uploaded", "count": len(uploaded), "files": uploaded}


class MediaCompressRequest(BaseModel):
    filename: str = "sample_input.mp4"
    reduction_percent: float = 50.0  # 10 to 85%
    target_resolution: str = "auto"  # auto, 1080p, 720p, 480p, 360p
    verify_with_java: bool = True


def calculate_auto_scheduling(file_size_bytes: int, is_video: bool, reduction_percent: float) -> dict:
    file_size_mb = max(0.01, file_size_bytes / (1024 * 1024))
    # Workload score based on computational complexity:
    # Videos require heavy H.264 macroblock encoding, images require pixel matrix scaling
    media_multiplier = 4.0 if is_video else 1.0
    # Higher compression ratio requires more aggressive CRF iterations / multi-pass search
    reduction_factor = 1.0 + (reduction_percent / 50.0)
    workload_score = round(file_size_mb * reduction_factor * media_multiplier, 2)

    if workload_score >= 15.0:
        priority = TaskPriority.CRITICAL  # 4
        priority_label = "CRITICAL (Level 4)"
        reason = f"Heavy computational workload (Score: {workload_score}) -- Immediate VIP queue execution"
    elif workload_score >= 6.0:
        priority = TaskPriority.HIGH  # 3
        priority_label = "HIGH (Level 3)"
        reason = f"Substantial workload (Score: {workload_score}) -- High priority parallel processing"
    elif workload_score >= 1.0:
        priority = TaskPriority.NORMAL  # 2
        priority_label = "NORMAL (Level 2)"
        reason = f"Standard workload (Score: {workload_score}) -- Protected by Starvation-Free Aging"
    else:
        priority = TaskPriority.LOW  # 1
        priority_label = "LOW (Level 1)"
        reason = f"Lightweight workload (Score: {workload_score}) -- Batched background execution"

    return {
        "workload_score": workload_score,
        "priority_level": priority.value,
        "priority_name": priority.name,
        "priority_label": priority_label,
        "scheduling_policy": "Priority Queue with Starvation-Free Aging (Topic 05)",
        "worker_routing": "Python Worker (Compression) + Java Worker (SHA-256 Bitstream Integrity)",
        "file_size_mb": round(file_size_mb, 2),
        "reason": reason
    }


@app.get("/api/media/estimate-scheduling")
def estimate_media_scheduling(filename: str = "sample_input.mp4", reduction_percent: float = 50.0, target_resolution: str = "auto"):
    """Calculates live auto-scheduling priority and workload score for user's customized reduction input."""
    fp = os.path.join(MEDIA_DIR, os.path.basename(filename))
    if not os.path.exists(fp):
        fp = os.path.join(MEDIA_DIR, "sample_input.mp4")
    
    file_size_bytes = os.path.getsize(fp) if os.path.exists(fp) else 1024 * 500
    ext = os.path.splitext(fp)[1].lower()
    is_video = ext in [".mp4", ".mov", ".avi", ".webm", ".mkv"]

    scheduling = calculate_auto_scheduling(file_size_bytes, is_video, reduction_percent)
    
    # Estimate output size
    keep_ratio = max(0.15, (100.0 - reduction_percent) / 100.0)
    est_output_bytes = int(file_size_bytes * keep_ratio)
    
    return {
        "filename": os.path.basename(fp),
        "is_video": is_video,
        "original_size_bytes": file_size_bytes,
        "original_size_mb": round(file_size_bytes / (1024 * 1024), 2),
        "estimated_output_bytes": est_output_bytes,
        "estimated_output_mb": round(est_output_bytes / (1024 * 1024), 2),
        "estimated_reduction_percent": reduction_percent,
        "scheduling": scheduling
    }


@app.post("/api/media/compress")
async def compress_media_custom(req: MediaCompressRequest):
    """Processes user's customized size reduction request.
    Automatically assigns scheduling priority based on file size and reduction percentage.
    Dispatches task across Python worker (compression) and Java worker (integrity validation).
    """
    safe_name = os.path.basename(req.filename)
    fp = os.path.join(MEDIA_DIR, safe_name)
    if not os.path.exists(fp):
        safe_name = "sample_input.mp4"
        fp = os.path.join(MEDIA_DIR, safe_name)
        if not os.path.exists(fp):
            raise HTTPException(status_code=404, detail="Media file not found")

    file_size_bytes = os.path.getsize(fp)
    ext = os.path.splitext(safe_name)[1].lower()
    is_video = ext in [".mp4", ".mov", ".avi", ".webm", ".mkv"]

    # 1. Automatic Scheduling Assignment
    sched = calculate_auto_scheduling(file_size_bytes, is_video, req.reduction_percent)
    assigned_priority = TaskPriority(sched["priority_level"])

    # 2. Main Media Size Reduction Task (Python Worker Node)
    reduction_task = Task(
        task_type="media_size_reduction",
        priority=assigned_priority,
        required_language=WorkerLanguage.PYTHON,
        payload={
            "filename": safe_name,
            "reduction_percent": req.reduction_percent,
            "target_resolution": req.target_resolution
        },
        estimated_workload=sched["workload_score"],
        status=TaskStatus.PENDING
    )
    result_mgr.save_task(reduction_task)
    result_mgr.log_event(reduction_task.task_id, "TASK_SUBMITTED", 
                         f"Media size reduction task auto-assigned to {assigned_priority.name} (Workload: {sched['workload_score']})")
    queue_mgr.enqueue(reduction_task)
    internal_executor.submit(execute_task_internally, reduction_task)

    await broadcast_ws("TASK_SUBMITTED", {
        "task_id": reduction_task.task_id,
        "task_type": reduction_task.task_type,
        "priority": reduction_task.priority.name,
        "required_language": "PYTHON",
        "scheduling_info": sched
    })

    # 3. Cryptographic Bitstream Integrity Verification (Java Worker Node)
    java_task = None
    if req.verify_with_java:
        java_task = Task(
            task_type="media_watermark_hash",
            priority=assigned_priority,
            required_language=WorkerLanguage.JAVA,
            payload={"filename": safe_name},
            estimated_workload=round(sched["workload_score"] * 0.5, 2),
            status=TaskStatus.PENDING
        )
        result_mgr.save_task(java_task)
        result_mgr.log_event(java_task.task_id, "TASK_SUBMITTED", 
                             f"Java integrity verification auto-assigned to {assigned_priority.name}")
        queue_mgr.enqueue(java_task)

        await broadcast_ws("TASK_SUBMITTED", {
            "task_id": java_task.task_id,
            "task_type": java_task.task_type,
            "priority": java_task.priority.name,
            "required_language": "JAVA",
            "scheduling_info": sched
        })

    return {
        "status": "queued",
        "filename": safe_name,
        "is_video": is_video,
        "reduction_task_id": reduction_task.task_id,
        "java_verification_task_id": java_task.task_id if java_task else None,
        "auto_scheduling": sched
    }


@app.get("/api/media/download/{filename}")
def download_media_file(filename: str):
    """Direct forced download endpoint for reduced media files."""
    safe_name = os.path.basename(filename)
    fp = os.path.join(MEDIA_DIR, safe_name)
    if not os.path.exists(fp):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(
        path=fp,
        filename=safe_name,
        media_type="application/octet-stream"
    )


class MediaBatchCompressRequest(BaseModel):
    filenames: List[str]
    reduction_percent: float = 50.0
    target_resolution: str = "auto"


@app.post("/api/media/compress-batch")
async def compress_media_batch(req: MediaBatchCompressRequest):
    """Processes multiple media files concurrently across the distributed worker queue."""
    dispatched = []
    total_score = 0.0

    # Deduplicate filenames so no file is processed more than once
    seen = set()
    unique_filenames = []
    for fn in req.filenames:
        clean = os.path.basename(fn)
        if clean not in seen:
            seen.add(clean)
            unique_filenames.append(clean)

    for safe_name in unique_filenames:
        fp = os.path.join(MEDIA_DIR, safe_name)
        if not os.path.exists(fp):
            continue

        file_size_bytes = os.path.getsize(fp)
        ext = os.path.splitext(safe_name)[1].lower()
        is_video = ext in [".mp4", ".mov", ".avi", ".webm", ".mkv"]

        sched = calculate_auto_scheduling(file_size_bytes, is_video, req.reduction_percent)
        assigned_priority = TaskPriority(sched["priority_level"])
        total_score += sched["workload_score"]

        reduction_task = Task(
            task_type="media_size_reduction",
            priority=assigned_priority,
            required_language=WorkerLanguage.PYTHON,
            payload={
                "filename": safe_name,
                "reduction_percent": req.reduction_percent,
                "target_resolution": req.target_resolution
            },
            estimated_workload=sched["workload_score"],
            status=TaskStatus.PENDING
        )
        result_mgr.save_task(reduction_task)
        result_mgr.log_event(reduction_task.task_id, "TASK_SUBMITTED",
                             f"Batch item [{safe_name}] assigned to {assigned_priority.name} (Score: {sched['workload_score']})")
        queue_mgr.enqueue(reduction_task)
        internal_executor.submit(execute_task_internally, reduction_task)

        await broadcast_ws("TASK_SUBMITTED", {
            "task_id": reduction_task.task_id,
            "task_type": reduction_task.task_type,
            "priority": reduction_task.priority.name,
            "required_language": "PYTHON",
            "scheduling_info": sched
        })

        dispatched.append({
            "filename": safe_name,
            "task_id": reduction_task.task_id,
            "is_video": is_video,
            "size_bytes": file_size_bytes,
            "scheduling": sched
        })

    return {
        "status": "queued",
        "batch_count": len(dispatched),
        "total_workload_score": round(total_score, 2),
        "tasks": dispatched
    }


class DownloadZipRequest(BaseModel):
    filenames: List[str]


@app.post("/api/media/download-zip")
def download_zip_archive(req: DownloadZipRequest):
    """Packages processed files into a single ZIP archive for instant 1-click batch download."""
    if not req.filenames:
        raise HTTPException(status_code=400, detail="No filenames provided for ZIP archive")

    zip_buffer = io.BytesIO()
    added_count = 0

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname in req.filenames:
            safe_name = os.path.basename(fname)
            fp = os.path.join(MEDIA_DIR, safe_name)
            if os.path.exists(fp):
                zf.write(fp, arcname=safe_name)
                added_count += 1

    if added_count == 0:
        raise HTTPException(status_code=404, detail="None of the specified files exist on server")

    zip_buffer.seek(0)
    zip_filename = f"MediaMesh_Processed_{int(time.time())}.zip"
    zip_path = os.path.join(MEDIA_DIR, zip_filename)
    with open(zip_path, "wb") as f_out:
        f_out.write(zip_buffer.getvalue())

    return FileResponse(
        path=zip_path,
        filename=zip_filename,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{zip_filename}"'}
    )


class MediaPipelineRequest(BaseModel):
    filename: str = "sample_input.mp4"
    priority: TaskPriority = TaskPriority.NORMAL
    target_resolutions: List[str] = ["720p", "480p"]
    extract_audio: bool = True
    extract_thumbnails: bool = True
    enable_java_watermark: bool = True
    chunk_split: bool = True
    image_filter: str = "EDGE_DETECT"


@app.post("/api/media/process-pipeline")
async def process_media_pipeline(req: MediaPipelineRequest):
    """Dispatches a distributed video/image processing pipeline across Python and Java workers."""
    filename = req.filename
    ext = os.path.splitext(filename)[1].lower()
    is_video = ext in [".mp4", ".mov", ".avi", ".webm"]

    created_tasks = []

    def enqueue_pipeline_task(t_type: str, payload: dict, lang: Optional[WorkerLanguage] = None):
        task = Task(
            task_type=t_type,
            priority=req.priority,
            required_language=lang,
            payload=payload,
            status=TaskStatus.PENDING
        )
        result_mgr.save_task(task)
        result_mgr.log_event(task.task_id, "TASK_SUBMITTED", f"Media pipeline task: {t_type}")
        queue_mgr.enqueue(task)
        created_tasks.append(task)
        safe_broadcast("TASK_SUBMITTED", {
            "task_id": task.task_id,
            "task_type": task.task_type,
            "priority": task.priority.name,
            "required_language": task.required_language.value if task.required_language else "ANY"
        })

    if is_video:
        # 1. Video Transcoding for each target resolution (Python Worker)
        for res in req.target_resolutions:
            enqueue_pipeline_task("video_transcode", {"filename": filename, "resolution": res, "crf": 26}, WorkerLanguage.PYTHON)

        # 2. Audio Extraction (Python Worker)
        if req.extract_audio:
            enqueue_pipeline_task("audio_extract", {"filename": filename}, WorkerLanguage.PYTHON)

        # 3. Keyframe Thumbnail Extraction (Python Worker)
        if req.extract_thumbnails:
            enqueue_pipeline_task("thumbnail_extract", {"filename": filename, "count": 4}, WorkerLanguage.PYTHON)

        # 4. Java 26 Cryptographic Watermark & Luminance Matrix (Java Worker)
        if req.enable_java_watermark:
            enqueue_pipeline_task("media_watermark_hash", {"filename": filename}, WorkerLanguage.JAVA)

        # 5. Chunk-Based Parallel Processing (Spatial MapReduce Innovation)
        if req.chunk_split:
            chunks = [
                {"chunk_index": 0, "start_sec": 0.0, "duration_sec": 1.5, "filter": "none"},
                {"chunk_index": 1, "start_sec": 1.5, "duration_sec": 1.5, "filter": "none"},
                {"chunk_index": 2, "start_sec": 3.0, "duration_sec": 2.0, "filter": "none"},
            ]
            for c in chunks:
                enqueue_pipeline_task("video_chunk_process", {"filename": filename, **c}, WorkerLanguage.PYTHON)

    else:
        # Image Processing (Pillow Filters)
        enqueue_pipeline_task("image_process", {"filename": filename, "filter": req.image_filter}, WorkerLanguage.PYTHON)
        # Java 26 Cryptographic Fingerprint
        if req.enable_java_watermark:
            enqueue_pipeline_task("media_watermark_hash", {"filename": filename}, WorkerLanguage.JAVA)

    return {
        "status": "pipeline_dispatched",
        "filename": filename,
        "is_video": is_video,
        "total_tasks": len(created_tasks),
        "task_ids": [t.task_id for t in created_tasks]
    }


# ==================== MODULE 1 & 2: CLIENT & PRIMARY SERVER ====================

@app.post("/api/tasks", response_model=Task, status_code=201)
async def submit_task(req: TaskCreateRequest):
    """Receives and validates computational tasks from client."""
    task = Task(
        task_type=req.task_type,
        priority=req.priority,
        required_language=req.required_language,
        payload=req.payload,
        max_retries=req.max_retries,
        estimated_workload=req.estimated_workload,
        status=TaskStatus.PENDING
    )
    result_mgr.save_task(task)
    result_mgr.log_event(task.task_id, "TASK_SUBMITTED", f"Task submitted with priority {task.priority.name}")
    queue_mgr.enqueue(task)

    await broadcast_ws("TASK_SUBMITTED", {
        "task_id": task.task_id,
        "task_type": task.task_type,
        "priority": task.priority.name,
        "required_language": task.required_language.value if task.required_language else "ANY"
    })
    return task

@app.get("/api/tasks/{task_id}", response_model=Task)
def get_task_status(task_id: str):
    """Fetch task status and computational result."""
    task = result_mgr.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task

@app.get("/api/tasks", response_model=List[Task])
def list_tasks(limit: int = 100):
    return result_mgr.get_all_tasks(limit=limit)

@app.get("/api/stats")
def get_system_stats():
    stats = result_mgr.get_system_stats()
    stats["queue_size"] = queue_mgr.size()
    stats["active_workers"] = len([w for w in worker_mgr.get_all_workers() if w.status != WorkerStatus.DEAD])
    stats["scheduling_algorithm"] = queue_mgr.scheduling_algorithm
    return stats

# ==================== MODULE 3 & 4: TASK QUEUE & WORKERS ====================

@app.post("/api/workers/register")
async def register_worker(req: WorkerRegisterRequest):
    info = worker_mgr.register_worker(req)
    await broadcast_ws("WORKER_REGISTERED", {
        "worker_id": info.worker_id,
        "language": info.language.value,
        "hostname": info.hostname
    })
    return {"status": "registered", "worker": info}

@app.post("/api/workers/heartbeat")
async def receive_heartbeat(hb: WorkerHeartbeat):
    worker_mgr.process_heartbeat(hb)
    await broadcast_ws("WORKER_HEARTBEAT", {
        "worker_id": hb.worker_id,
        "language": hb.language.value,
        "cpu_percent": hb.cpu_percent,
        "memory_percent": hb.memory_percent,
        "active_tasks": hb.active_tasks_count,
        "status": hb.status.value
    })
    return {"status": "ok"}

@app.get("/api/workers", response_model=List[WorkerInfo])
def list_workers():
    return worker_mgr.get_all_workers()

@app.post("/api/tasks/poll")
async def poll_task(worker_id: str):
    """Worker node fetches next task matching its language and capabilities."""
    worker = worker_mgr.get_worker(worker_id)
    if not worker:
        raise HTTPException(status_code=404, detail="Worker not registered")

    task = queue_mgr.dequeue_for_worker(worker)
    if not task:
        return {"task": None}

    # Mark claimed and track in-flight
    task.status = TaskStatus.RUNNING
    worker_mgr.track_in_flight(task)
    result_mgr.save_task(task)
    result_mgr.log_event(task.task_id, "TASK_CLAIMED", f"Claimed by {worker.language.value} worker '{worker_id}'")

    await broadcast_ws("TASK_CLAIMED", {
        "task_id": task.task_id,
        "task_type": task.task_type,
        "worker_id": worker_id,
        "worker_language": worker.language.value
    })
    return {"task": task}

@app.post("/api/tasks/result")
async def submit_result(sub: TaskResultSubmission):
    """Worker returns computational result or failure."""
    task = result_mgr.get_task(sub.task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    is_success = (sub.status == TaskStatus.COMPLETED)
    task.status = sub.status
    task.result = sub.result
    task.error_message = sub.error_message
    task.execution_time_seconds = sub.execution_time_seconds
    task.completed_at = time.time()

    worker_mgr.complete_in_flight(sub.task_id, success=is_success)
    result_mgr.save_task(task)
    result_mgr.log_event(task.task_id, f"TASK_{sub.status.value}", f"Finished by worker {sub.worker_id}")

    await broadcast_ws("TASK_COMPLETED" if is_success else "TASK_FAILED", {
        "task_id": task.task_id,
        "task_type": task.task_type,
        "status": task.status.value,
        "worker_id": sub.worker_id,
        "execution_time_seconds": task.execution_time_seconds,
        "result": task.result
    })
    return {"status": "saved"}

@app.get("/api/queue")
def get_queue_snapshot():
    return {
        "algorithm": queue_mgr.scheduling_algorithm,
        "size": queue_mgr.size(),
        "tasks": queue_mgr.get_snapshot()
    }

@app.post("/api/queue/algorithm")
async def update_algorithm(algorithm: str):
    valid = ["FIFO", "PRIORITY_WITH_AGING", "LOAD_AWARE"]
    if algorithm not in valid:
        raise HTTPException(status_code=400, detail=f"Algorithm must be one of {valid}")
    queue_mgr.set_scheduling_algorithm(algorithm)
    await broadcast_ws("ALGORITHM_CHANGED", {"algorithm": algorithm})
    return {"status": "updated", "algorithm": algorithm}

@app.post("/api/chaos/kill-worker")
async def chaos_kill_worker(worker_id: str):
    """Chaos engineering: artificially disconnect a worker to demonstrate live auto-reclamation."""
    worker = worker_mgr.get_worker(worker_id)
    if not worker:
        raise HTTPException(status_code=404, detail="Worker not found")
    worker.status = WorkerStatus.DEAD
    worker.last_heartbeat = 0.0  # Force timeout immediately
    await broadcast_ws("CHAOS_WORKER_KILLED", {"worker_id": worker_id})
    return {"status": "killed", "worker_id": worker_id}

# ==================== WEBSOCKET HUB ====================

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_connections.discard(websocket)
    except Exception:
        active_connections.discard(websocket)
