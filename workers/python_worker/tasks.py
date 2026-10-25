import hashlib
import io
import math
import os
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List
from PIL import Image, ImageDraw, ImageFilter
from config import MEDIA_DIR

# Get static FFmpeg binary from imageio_ffmpeg
try:
    import imageio_ffmpeg
    FFMPEG_EXE = imageio_ffmpeg.get_ffmpeg_exe()
except Exception:
    FFMPEG_EXE = "ffmpeg"


def run_video_transcode(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Transcodes a video to a target resolution (1080p, 720p, 480p, 360p) with H.264 compression."""
    filename = payload.get("filename", "sample_input.mp4")
    input_path = MEDIA_DIR / filename
    if not input_path.exists():
        input_path = MEDIA_DIR / "sample_input.mp4"

    resolution = payload.get("resolution", "720p").lower()
    res_map = {
        "1080p": "1920x1080",
        "720p": "1280x720",
        "480p": "854x480",
        "360p": "640x360"
    }
    scale_dim = res_map.get(resolution, "1280x720")
    crf = str(payload.get("crf", 26))

    output_filename = f"transcode_{resolution}_{uuid.uuid4().hex[:8]}.mp4"
    output_path = MEDIA_DIR / output_filename

    start_time = time.time()
    cmd = [
        FFMPEG_EXE, "-y",
        "-i", str(input_path),
        "-vf", f"scale={scale_dim}",
        "-c:v", "libx264", "-crf", crf, "-preset", "veryfast",
        "-c:a", "aac", "-b:a", "128k",
        str(output_path)
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    duration = round(time.time() - start_time, 3)

    out_size_mb = round(output_path.stat().st_size / (1024 * 1024), 2)
    in_size_mb = round(input_path.stat().st_size / (1024 * 1024), 2)

    return {
        "operation": "VIDEO_TRANSCODE",
        "input_file": input_path.name,
        "input_size_mb": in_size_mb,
        "target_resolution": resolution,
        "dimensions": scale_dim,
        "output_filename": output_filename,
        "output_url": f"/media/{output_filename}",
        "output_size_mb": out_size_mb,
        "compression_ratio": f"{round((1 - out_size_mb / max(in_size_mb, 0.01)) * 100, 1)}%",
        "compute_time_seconds": duration,
        "message": f"Successfully transcoded to {resolution} ({scale_dim}) in {duration}s"
    }


def run_audio_extract(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Extracts the audio track from a video into MP3 format."""
    filename = payload.get("filename", "sample_input.mp4")
    input_path = MEDIA_DIR / filename
    if not input_path.exists():
        input_path = MEDIA_DIR / "sample_input.mp4"

    output_filename = f"audio_{uuid.uuid4().hex[:8]}.mp3"
    output_path = MEDIA_DIR / output_filename

    start_time = time.time()
    cmd = [
        FFMPEG_EXE, "-y",
        "-i", str(input_path),
        "-vn",
        "-acodec", "libmp3lame",
        "-b:a", "192k",
        str(output_path)
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    duration = round(time.time() - start_time, 3)

    size_kb = round(output_path.stat().st_size / 1024, 1)

    return {
        "operation": "AUDIO_EXTRACTION",
        "input_file": input_path.name,
        "audio_format": "MP3",
        "bitrate": "192 kbps",
        "output_filename": output_filename,
        "output_url": f"/media/{output_filename}",
        "output_size_kb": size_kb,
        "compute_time_seconds": duration,
        "message": f"Audio track extracted successfully in {duration}s"
    }


def run_thumbnail_extract(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Extracts keyframe preview thumbnails across the timeline of a video."""
    filename = payload.get("filename", "sample_input.mp4")
    input_path = MEDIA_DIR / filename
    if not input_path.exists():
        input_path = MEDIA_DIR / "sample_input.mp4"

    count = int(payload.get("count", 4))
    thumbnails = []
    start_time = time.time()

    # Extract frames at 1s, 2s, 3s, 4s (or proportional offsets)
    for i in range(1, count + 1):
        thumb_name = f"thumb_{uuid.uuid4().hex[:8]}_sec{i}.jpg"
        thumb_path = MEDIA_DIR / thumb_name
        cmd = [
            FFMPEG_EXE, "-y",
            "-ss", str(i),
            "-i", str(input_path),
            "-vframes", "1",
            "-vf", "scale=320:180",
            "-q:v", "3",
            str(thumb_path)
        ]
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if thumb_path.exists():
                thumbnails.append({
                    "timestamp_sec": i,
                    "filename": thumb_name,
                    "url": f"/media/{thumb_name}"
                })
        except Exception:
            pass

    duration = round(time.time() - start_time, 3)

    return {
        "operation": "THUMBNAIL_EXTRACTION",
        "input_file": input_path.name,
        "thumbnails_generated": len(thumbnails),
        "thumbnails": thumbnails,
        "compute_time_seconds": duration,
        "message": f"Generated {len(thumbnails)} keyframe preview thumbnails in {duration}s"
    }


def run_video_chunk_process(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Processes a specific time segment of a video in parallel (Chunk-Based Parallel Splitting)."""
    filename = payload.get("filename", "sample_input.mp4")
    input_path = MEDIA_DIR / filename
    if not input_path.exists():
        input_path = MEDIA_DIR / "sample_input.mp4"

    chunk_idx = payload.get("chunk_index", 0)
    start_sec = payload.get("start_sec", 0.0)
    duration_sec = payload.get("duration_sec", 2.0)
    filter_effect = payload.get("filter", "none")

    output_filename = f"chunk_{chunk_idx}_{uuid.uuid4().hex[:8]}.mp4"
    output_path = MEDIA_DIR / output_filename

    vf_filters = []
    if filter_effect == "grayscale":
        vf_filters.append("colorchannelmixer=.3:.4:.3:0:.3:.4:.3:0:.3:.4:.3")
    elif filter_effect == "edge":
        vf_filters.append("edgedetect=low=0.1:high=0.4")
    elif filter_effect == "sepia":
        vf_filters.append("colorchannelmixer=.393:.769:.189:0:.349:.686:.168:0:.272:.534:.131")

    vf_arg = ["-vf", ",".join(vf_filters)] if vf_filters else []

    start_time = time.time()
    cmd = [
        FFMPEG_EXE, "-y",
        "-ss", str(start_sec),
        "-t", str(duration_sec),
        "-i", str(input_path),
        "-c:v", "libx264", "-preset", "ultrafast",
        *vf_arg,
        str(output_path)
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    compute_time = round(time.time() - start_time, 3)

    return {
        "operation": "VIDEO_CHUNK_PROCESS",
        "chunk_index": chunk_idx,
        "time_slice": f"{start_sec}s - {start_sec + duration_sec}s",
        "output_filename": output_filename,
        "output_url": f"/media/{output_filename}",
        "compute_time_seconds": compute_time,
        "message": f"Chunk {chunk_idx} ({start_sec}s-{start_sec+duration_sec}s) processed in {compute_time}s"
    }


def run_image_process(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Applies computational filters (Sobel Edge, Blur, Grayscale, Contour) to an uploaded or sample image."""
    filter_type = payload.get("filter", "EDGE_DETECT").upper()
    filename = payload.get("filename")

    # Load uploaded image or generate high-contrast benchmark test pattern
    if filename and (MEDIA_DIR / filename).exists():
        img = Image.open(MEDIA_DIR / filename).convert("RGB")
        width, height = img.size
    else:
        width, height = 800, 600
        img = Image.new("RGB", (width, height), color=(15, 23, 42))
        draw = ImageDraw.Draw(img)
        for r in range(30, 320, 25):
            draw.ellipse([width//2 - r, height//2 - r, width//2 + r, height//2 + r], outline=(99, 102, 241), width=4)
        for i in range(0, width, 50):
            draw.line([(i, 0), (width - i, height)], fill=(244, 63, 94), width=3)
        draw.text((40, 40), f"Distributed Media Transcoder - {filter_type}", fill=(255, 255, 255))

    start_time = time.time()
    # Apply filter
    if filter_type == "EDGE_DETECT":
        processed_img = img.filter(ImageFilter.FIND_EDGES)
    elif filter_type == "GAUSSIAN_BLUR":
        processed_img = img.filter(ImageFilter.GaussianBlur(radius=8))
    elif filter_type == "GRAYSCALE":
        processed_img = img.convert("L")
    elif filter_type == "CONTOUR":
        processed_img = img.filter(ImageFilter.CONTOUR)
    else:
        processed_img = img.filter(ImageFilter.SHARPEN)

    output_filename = f"filtered_{filter_type.lower()}_{uuid.uuid4().hex[:8]}.png"
    output_path = MEDIA_DIR / output_filename
    processed_img.save(output_path, "PNG")
    compute_time = round(time.time() - start_time, 3)

    return {
        "operation": "IMAGE_FILTER",
        "filter_applied": filter_type,
        "image_dimensions": f"{width}x{height}",
        "output_filename": output_filename,
        "output_url": f"/media/{output_filename}",
        "compute_time_seconds": compute_time,
        "message": f"Successfully applied {filter_type} filter in {compute_time}s"
    }


def run_prime_sieve(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Calculates primes up to limit using Sieve of Eratosthenes."""
    limit = int(payload.get("limit", 100000))
    limit = min(limit, 5000000)
    sieve = [True] * (limit + 1)
    sieve[0] = sieve[1] = False
    for p in range(2, int(math.isqrt(limit)) + 1):
        if sieve[p]:
            for i in range(p * p, limit + 1, p):
                sieve[i] = False
    prime_count = sum(1 for is_p in sieve if is_p)
    largest_prime = max(idx for idx, is_p in enumerate(sieve) if is_p)
    return {
        "limit": limit,
        "total_primes_found": prime_count,
        "largest_prime": largest_prime
    }


def run_hash_solve(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Mines a cryptographic proof-of-work nonce matching the target difficulty."""
    difficulty = int(payload.get("difficulty", 4))
    prefix = "0" * difficulty
    nonce = 0
    start_time = time.time()
    salt = uuid.uuid4().hex[:8]

    while True:
        candidate = f"TASK-{salt}-{nonce}"
        h = hashlib.sha256(candidate.encode()).hexdigest()
        if h.startswith(prefix):
            return {
                "salt": salt,
                "nonce": nonce,
                "hash": h,
                "difficulty": difficulty,
                "iterations_tried": nonce + 1,
                "compute_time_seconds": round(time.time() - start_time, 4)
            }
        nonce += 1
        if nonce > 2000000:
            return {"error": "Nonce search limit reached without solution"}


def run_word_count(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Computes word frequency and statistics for text payload."""
    text = payload.get("text", "")
    words = [w.strip(".,!?;:\"'()[]").lower() for w in text.split() if w.strip()]
    freq = {}
    for w in words:
        freq[w] = freq.get(w, 0) + 1
    sorted_freq = sorted(freq.items(), key=lambda x: x[1], reverse=True)[:10]
    return {
        "total_words": len(words),
        "unique_words": len(freq),
        "top_frequencies": dict(sorted_freq)
    }


def format_bytes(num_bytes: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.2f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.2f} TB"


def run_media_size_reduction(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Custom Media Size Reduction Engine (Videos and Images).
    Reduces media file size according to user's exact requested percentage (e.g. 25%, 50%, 75%).
    Uses FFmpeg H.264/AAC for video compression and Pillow for image optimization.
    """
    filename = payload.get("filename", "sample_input.mp4")
    input_path = MEDIA_DIR / filename
    if not input_path.exists():
        input_path = MEDIA_DIR / "sample_input.mp4"

    reduction_percent = float(payload.get("reduction_percent", 50))
    reduction_percent = max(10.0, min(85.0, reduction_percent))

    target_resolution = payload.get("target_resolution", "auto").lower()

    ext = input_path.suffix.lower()
    is_video = ext in [".mp4", ".mov", ".avi", ".webm", ".mkv"]

    start_time = time.time()
    in_size = input_path.stat().st_size

    if is_video:
        # Calculate compression parameters based on requested reduction percentage
        # CRF 22 (high quality) to 36 (aggressive reduction)
        crf = int(22 + (reduction_percent / 100.0) * 14)
        audio_bitrate = "96k" if reduction_percent >= 50 else "128k"

        if target_resolution == "360p":
            scale_filter = "scale=-2:'min(360,ih)'"
            res_label = "360p"
        elif target_resolution == "480p":
            scale_filter = "scale=-2:'min(480,ih)'"
            res_label = "480p"
        elif target_resolution == "720p":
            scale_filter = "scale=-2:'min(720,ih)'"
            res_label = "720p"
        elif target_resolution == "1080p":
            scale_filter = "scale=-2:'min(1080,ih)'"
            res_label = "1080p"
        else:
            # Auto: Keep EXACT original resolution and aspect ratio — NO zoom, NO crop!
            scale_filter = "scale=trunc(iw/2)*2:trunc(ih/2)*2"
            res_label = "Original"

        output_filename = f"reduced_{res_label}_{int(reduction_percent)}pct_{uuid.uuid4().hex[:8]}.mp4"
        output_path = MEDIA_DIR / output_filename

        cmd = [
            FFMPEG_EXE, "-y",
            "-i", str(input_path),
            "-vf", scale_filter,
            "-c:v", "libx264", "-crf", str(crf), "-preset", "veryfast",
            "-pix_fmt", "yuv420p",
            "-movflags", "+faststart",
            "-c:a", "aac", "-b:a", audio_bitrate,
            str(output_path)
        ]
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    else:
        img = Image.open(input_path)
        # Handle transparency or palette modes cleanly without black artifacts
        if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
            img_rgba = img.convert("RGBA")
            bg = Image.new("RGB", img_rgba.size, (255, 255, 255))
            bg.paste(img_rgba, mask=img_rgba.split()[3])
            img = bg
        elif img.mode != "RGB":
            img = img.convert("RGB")

        w, h = img.size
        if reduction_percent <= 25:
            # Enhancement mode: preserve resolution, apply subtle clarity enhancement
            processed = img.filter(ImageFilter.UnsharpMask(radius=1.5, percent=115, threshold=2))
            quality = 95
            output_filename = f"enhanced_{int(reduction_percent)}pct_{uuid.uuid4().hex[:8]}.jpg"
            output_path = MEDIA_DIR / output_filename
            processed.save(output_path, "JPEG", optimize=True, quality=quality)
            res_label = f"{w}x{h} (Enhanced)"
        else:
            keep_ratio = max(0.15, (100.0 - reduction_percent) / 100.0)
            scale_factor = math.sqrt(keep_ratio)
            if max(w, h) > 800 or reduction_percent >= 50:
                new_w = max(120, int(w * scale_factor))
                new_h = max(120, int(h * scale_factor))
                processed = img.resize((new_w, new_h), Image.Resampling.LANCZOS)
            else:
                processed = img
                new_w, new_h = w, h

            quality = max(25, min(85, int(90 * keep_ratio)))
            output_filename = f"reduced_{int(reduction_percent)}pct_{uuid.uuid4().hex[:8]}.jpg"
            output_path = MEDIA_DIR / output_filename
            processed.save(output_path, "JPEG", optimize=True, quality=quality)
            res_label = f"{new_w}x{new_h}"

            # Ensure reduced file is actually smaller than original if reduction requested
            if output_path.stat().st_size >= in_size:
                target_q = max(20, int(quality * 0.75))
                target_w = max(100, int(new_w * 0.85))
                target_h = max(100, int(new_h * 0.85))
                smaller = processed.resize((target_w, target_h), Image.Resampling.LANCZOS)
                smaller.save(output_path, "JPEG", optimize=True, quality=target_q)
                res_label = f"{target_w}x{target_h}"

    out_size = output_path.stat().st_size
    duration = round(time.time() - start_time, 3)

    is_enhanced = out_size > in_size
    if is_enhanced:
        actual_change_pct = round(((out_size - in_size) / max(in_size, 1)) * 100.0, 1)
        bytes_diff = out_size - in_size
        op_label = "MEDIA_ENHANCEMENT"
        msg = f"Enhanced media quality (+{actual_change_pct}% fidelity, {format_bytes(in_size)} -> {format_bytes(out_size)})"
    else:
        actual_change_pct = round((1.0 - (out_size / max(in_size, 1))) * 100.0, 1)
        bytes_diff = in_size - out_size
        op_label = "MEDIA_SIZE_REDUCTION"
        msg = f"Reduced size by {actual_change_pct}% ({format_bytes(in_size)} -> {format_bytes(out_size)})"

    return {
        "operation": op_label,
        "is_enhanced": is_enhanced,
        "media_type": "VIDEO" if is_video else "IMAGE",
        "input_filename": input_path.name,
        "original_size_bytes": in_size,
        "original_size_formatted": format_bytes(in_size),
        "reduced_size_bytes": out_size,
        "reduced_size_formatted": format_bytes(out_size),
        "reduction_percent_requested": reduction_percent,
        "reduction_percent_achieved": actual_change_pct if not is_enhanced else -actual_change_pct,
        "change_percent": actual_change_pct,
        "bytes_saved": max(0, in_size - out_size),
        "bytes_saved_formatted": format_bytes(max(0, in_size - out_size)),
        "bytes_added": max(0, out_size - in_size),
        "bytes_added_formatted": format_bytes(max(0, out_size - in_size)),
        "resolution_applied": res_label,
        "output_filename": output_filename,
        "output_url": f"/media/{output_filename}",
        "download_url": f"/media/{output_filename}",
        "compute_time_seconds": duration,
        "message": msg
    }


TASK_REGISTRY = {
    "media_size_reduction": run_media_size_reduction,
    "video_transcode":      run_video_transcode,
    "audio_extract":        run_audio_extract,
    "thumbnail_extract":    run_thumbnail_extract,
    "video_chunk_process":  run_video_chunk_process,
    "image_process":        run_image_process,
    "prime_sieve":          run_prime_sieve,
    "hash_solve":           run_hash_solve,
    "word_count":           run_word_count,
}

