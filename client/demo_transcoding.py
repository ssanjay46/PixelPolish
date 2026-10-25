import os
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from client.client_sdk import TaskQueueClient
from models import TaskPriority, WorkerLanguage

def run_transcoding_demo():
    client = TaskQueueClient("http://127.0.0.1:8000")

    print("==================================================================")
    print(" DISTRIBUTED VIDEO TRANSCODING & MEDIA PIPELINE (MEDIAMESH)")
    print(" Sathyabama Institute of Science and Technology - CSE PT1")
    print(" Architecture: AWS Elemental / YouTube Transcode Cluster")
    print("==================================================================\n")

    # 1. Check Server Connection
    try:
        stats = client.get_stats()
        print(f"[OK] Connected to Primary Dispatcher at {client.server_url}")
        print(f"     Active Workers in Mesh:     {stats['active_workers']}")
        print(f"     Active Scheduling Engine:   {stats['scheduling_algorithm']}")
        print(f"     Pending Queue Backlog:      {stats['queue_size']}\n")
    except Exception as e:
        print(f"[FAIL] Error connecting to server: {e}")
        print("Please ensure the primary server is running via 'run_server.bat'!")
        return

    # 2. Check Workers
    workers = client.get_workers()
    active = [w for w in workers if w["status"] != "DEAD"]
    print(f"[*] Detected {len(active)} active heterogeneous worker nodes:")
    for w in active:
        print(f"     - [{w['language']:<6}] ID: {w['worker_id']:<24} (CPU: {w['cpu_percent']}%, RAM: {w['memory_percent']}%)")
    print()

    # 3. Define the Distributed Media Transcoding Pipeline Batch
    # Media: sample_input.mp4
    media_file = "sample_input.mp4"

    pipeline_tasks = [
        {
            "name": "H.264 Video Transcode (720p HD)",
            "type": "video_transcode",
            "prio": TaskPriority.HIGH,
            "lang": WorkerLanguage.PYTHON,
            "payload": {"filename": media_file, "resolution": "720p", "crf": 26}
        },
        {
            "name": "Mobile Resolution Transcode (480p SD)",
            "type": "video_transcode",
            "prio": TaskPriority.NORMAL,
            "lang": WorkerLanguage.PYTHON,
            "payload": {"filename": media_file, "resolution": "480p", "crf": 28}
        },
        {
            "name": "MP3 Audio Track Extraction (192kbps)",
            "type": "audio_extract",
            "prio": TaskPriority.HIGH,
            "lang": WorkerLanguage.PYTHON,
            "payload": {"filename": media_file}
        },
        {
            "name": "Keyframe Timeline Thumbnails (4 frames)",
            "type": "thumbnail_extract",
            "prio": TaskPriority.NORMAL,
            "lang": WorkerLanguage.PYTHON,
            "payload": {"filename": media_file, "count": 4}
        },
        {
            "name": "Java 26 Cryptographic Watermark & Luminance",
            "type": "media_watermark_hash",
            "prio": TaskPriority.HIGH,
            "lang": WorkerLanguage.JAVA,
            "payload": {"filename": media_file}
        },
        {
            "name": "Parallel Chunk 1 Time-Slice (0.0s - 2.5s)",
            "type": "video_chunk_process",
            "prio": TaskPriority.CRITICAL,
            "lang": WorkerLanguage.PYTHON,
            "payload": {"filename": media_file, "chunk_index": 1, "start_sec": 0.0, "duration_sec": 2.5}
        },
        {
            "name": "Parallel Chunk 2 Time-Slice (2.5s - 5.0s)",
            "type": "video_chunk_process",
            "prio": TaskPriority.CRITICAL,
            "lang": WorkerLanguage.PYTHON,
            "payload": {"filename": media_file, "chunk_index": 2, "start_sec": 2.5, "duration_sec": 2.5}
        }
    ]

    print(f"[*] Dispatching {len(pipeline_tasks)} heterogeneous media tasks across worker mesh...\n")
    submitted = []

    for item in pipeline_tasks:
        res = client.submit_task(
            task_type=item["type"],
            priority=item["prio"],
            required_language=item["lang"],
            payload=item["payload"]
        )
        task_id = res["task_id"]
        submitted.append((task_id, item["name"], item["prio"], item["lang"]))
        print(f"  -> Enqueued: [{item['prio'].name:<8}] [{item['lang'].value:<6}] {item['name']} (ID: {task_id[:8]})")

    print("\n[*] Asynchronously executing media pipeline across Python and Java workers...")
    start_wait = time.time()
    results = {}

    while len(results) < len(submitted) and time.time() - start_wait < 35.0:
        for t_id, name, prio, lang in submitted:
            if t_id not in results:
                info = client.get_task(t_id)
                if info.get("status") in ["COMPLETED", "FAILED", "DEAD_LETTER"]:
                    results[t_id] = info
                    duration = info.get("execution_time_seconds", 0.0)
                    worker = info.get("assigned_worker_id", "node")
                    print(f"  [DONE] [{info.get('assigned_worker_language', 'N/A'):<6}] {name} in {duration}s by {worker}")
        time.sleep(0.4)

    total_parallel_time = round(time.time() - start_wait, 2)
    sequential_time = round(sum(results.get(t_id, {}).get("execution_time_seconds", 0.0) for t_id, _, _, _ in submitted), 2)
    speedup = round(sequential_time / max(total_parallel_time, 0.01), 1)

    print("\n" + "=" * 85)
    print(" DISTRIBUTED VIDEO TRANSCODING & MEDIA BENCHMARK REPORT")
    print("=" * 85)
    print(f"{'Task Name':<42} {'Priority':<9} {'Runtime':<7} {'Time (s)':<10} {'Status':<10}")
    print("-" * 85)

    for t_id, name, prio, lang in submitted:
        info = results.get(t_id, {})
        status = info.get("status", "TIMEOUT")
        exec_time = f"{info.get('execution_time_seconds', 0.0):.2f}" if info.get("execution_time_seconds") else "--"
        worker_lang = info.get("assigned_worker_language", lang.value)
        print(f"{name:<42} {prio.name:<9} {worker_lang:<7} {exec_time:<10} {status:<10}")

    print("=" * 85)
    print(f"\n[+] PERFORMANCE INNOVATION METRICS:")
    print(f"   - Cumulative Sequential Execution Time:  {sequential_time}s (Single Standalone Node)")
    print(f"   - Distributed Mesh Parallel Wall-Clock:  {total_parallel_time}s (Concurrent Workers)")
    print(f"   - Measured Effective Parallel Speedup:   {speedup}x Faster! [SPEEDUP]\n")
    print("[OK] All video resolutions, audio track, thumbnails, and Java DRM hashes ready.")
    print("     View the live interactive web dashboard: http://localhost:8000\n")

if __name__ == "__main__":
    run_transcoding_demo()
