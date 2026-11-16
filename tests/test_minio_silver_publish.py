import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "04_orchestration/airflow/scripts"))
import minio_task


class FakeS3Error(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}


class FakePaginator:
    def __init__(self, client):
        self.client = client

    def paginate(self, Bucket, Prefix):
        contents = [
            {"Key": key, "Size": len(value)}
            for key, value in self.client.objects.items()
            if key.startswith(Prefix)
        ]
        yield {"Contents": contents} if contents else {}


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.metadata = {}
        self.fail_copy_target = None

    def upload_file(self, filename, bucket, key, ExtraArgs=None):
        self.objects[key] = Path(filename).read_bytes()
        self.metadata[key] = (ExtraArgs or {}).get("Metadata", {})

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.objects[Key] = Body

    def get_paginator(self, operation):
        return FakePaginator(self)

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise FakeS3Error("404")
        return {"ContentLength": len(self.objects[Key]), "Metadata": self.metadata.get(Key, {})}

    def copy_object(self, Bucket, Key, CopySource):
        if Key == self.fail_copy_target:
            self.fail_copy_target = None
            raise RuntimeError("simulated copy failure")
        source = CopySource["Key"]
        self.objects[Key] = self.objects[source]
        self.metadata[Key] = self.metadata.get(source, {}).copy()

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)
        self.metadata.pop(Key, None)

    def download_file(self, bucket, key, filename):
        Path(filename).write_bytes(self.objects[key])


class MinioSilverPublishTests(unittest.TestCase):
    def test_failed_preprocessing_restores_local_silver_and_does_not_stage(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            data_root = Path(tmp)
            silver = data_root / "02_process_raw_data"
            silver.mkdir()
            original = silver / "review.csv"
            original.write_text("stable", encoding="utf-8")
            s3 = FakeS3()

            def fail_after_partial_write(command, cwd, check):
                original.write_text("partial", encoding="utf-8")
                (silver / "new_partial.csv").write_text("partial", encoding="utf-8")
                raise __import__("subprocess").CalledProcessError(1, command)

            with (
                patch.object(minio_task, "DATA_ROOT", data_root),
                patch.object(minio_task, "PROJECT_DIR", data_root),
                patch.object(minio_task, "client", return_value=s3),
                patch.object(minio_task, "ensure_bucket"),
                patch.object(minio_task, "download_current"),
                patch.object(minio_task.subprocess, "run", side_effect=fail_after_partial_write),
                patch.object(minio_task, "upload_staged_silver") as stage,
            ):
                with self.assertRaises(__import__("subprocess").CalledProcessError):
                    minio_task.main("preprocess", "scheduled__2026-10-23", "2026-10-23")

            self.assertEqual(original.read_text(encoding="utf-8"), "stable")
            self.assertFalse((silver / "new_partial.csv").exists())
            stage.assert_not_called()
            self.assertEqual(s3.objects, {})

    def test_only_validated_generation_replaces_current_and_archives_prior(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            data_root = Path(tmp)
            silver = data_root / "02_process_raw_data"
            silver.mkdir()
            (silver / "prices.csv").write_text("new", encoding="utf-8")
            s3 = FakeS3()
            s3.objects["current/silver/prices.csv"] = b"old"

            with patch.object(minio_task, "DATA_ROOT", data_root):
                count = minio_task.upload_staged_silver(s3, "scheduled__2026-10-23")
            self.assertEqual(count, 1)
            staged_key = "staging/scheduled_2026-10-23/silver/prices.csv"
            self.assertEqual(s3.objects["current/silver/prices.csv"], b"old")

            promoted = minio_task.promote_staged_silver(s3, "scheduled__2026-10-23")
            self.assertEqual(promoted, 1)
            self.assertEqual(s3.objects["current/silver/prices.csv"], b"new")
            archive_key = f"history/{minio_task.safe_run_id('scheduled__2026-10-23')}/silver/prices.csv"
            self.assertEqual(s3.objects[archive_key], b"old")
            self.assertNotIn(staged_key, s3.objects)
            self.assertNotIn(f"{minio_task.staging_silver_prefix('scheduled__2026-10-23')}_complete.json", s3.objects)

    def test_partial_promotion_failure_rolls_back_current_objects(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            data_root = Path(tmp)
            silver = data_root / "02_process_raw_data"
            silver.mkdir()
            (silver / "a.csv").write_text("new-a", encoding="utf-8")
            (silver / "b.csv").write_text("new-b", encoding="utf-8")
            s3 = FakeS3()
            s3.objects["current/silver/a.csv"] = b"old-a"
            s3.objects["current/silver/b.csv"] = b"old-b"
            with patch.object(minio_task, "DATA_ROOT", data_root):
                minio_task.upload_staged_silver(s3, "run-1")
            s3.fail_copy_target = "current/silver/b.csv"

            with self.assertRaisesRegex(RuntimeError, "simulated copy failure"):
                minio_task.promote_staged_silver(s3, "run-1")

            self.assertEqual(s3.objects["current/silver/a.csv"], b"old-a")
            self.assertEqual(s3.objects["current/silver/b.csv"], b"old-b")


if __name__ == "__main__":
    unittest.main()
