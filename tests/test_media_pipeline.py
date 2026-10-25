import os
import unittest
from pathlib import Path
from config import MEDIA_DIR
from workers.python_worker.tasks import (
    run_video_transcode,
    run_audio_extract,
    run_thumbnail_extract,
    run_video_chunk_process,
    run_image_process
)

class TestMediaPipeline(unittest.TestCase):
    def setUp(self):
        self.sample_video = "sample_input.mp4"
        self.assertTrue((MEDIA_DIR / self.sample_video).exists(), "sample_input.mp4 must exist in storage/media")

    def test_video_transcode_480p(self):
        res = run_video_transcode({"filename": self.sample_video, "resolution": "480p", "crf": 28})
        self.assertEqual(res["operation"], "VIDEO_TRANSCODE")
        self.assertEqual(res["target_resolution"], "480p")
        self.assertTrue(Path(MEDIA_DIR / res["output_filename"]).exists())

    def test_audio_extract_mp3(self):
        res = run_audio_extract({"filename": self.sample_video})
        self.assertEqual(res["operation"], "AUDIO_EXTRACTION")
        self.assertEqual(res["audio_format"], "MP3")
        self.assertTrue(Path(MEDIA_DIR / res["output_filename"]).exists())

    def test_thumbnail_extract(self):
        res = run_thumbnail_extract({"filename": self.sample_video, "count": 2})
        self.assertEqual(res["operation"], "THUMBNAIL_EXTRACTION")
        self.assertGreaterEqual(res["thumbnails_generated"], 1)

    def test_image_process_filter(self):
        res = run_image_process({"filter": "EDGE_DETECT"})
        self.assertEqual(res["operation"], "IMAGE_FILTER")
        self.assertTrue(Path(MEDIA_DIR / res["output_filename"]).exists())

if __name__ == "__main__":
    unittest.main()
