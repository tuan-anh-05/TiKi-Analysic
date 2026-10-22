"""Restore current data from MinIO, run one existing ETL script, archive changes."""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

PROJECT_DIR = Path(os.environ.get("TIKI_PROJECT_DIR", Path(__file__).resolve().parents[3]))
DATA_ROOT = Path(os.environ.get("TIKI_DATA_DIR", PROJECT_DIR)).resolve()
BUCKET = os.environ.get("MINIO_BUCKET", "tiki-lake")
SYNC_FILES = ("03_push_to_sql/sync_checkpoint_supabase.json",)
TASK_COMMANDS = {
    "daily_price": ["01_craw_data/daily_price/crawl_daily_price.py"],
    "product": ["01_craw_data/product/product.py"],
    "review": ["01_craw_data/review/review.py"],
    "preprocess": ["02_process_raw_data/run_all_preprocessing.py"],
    "validate_silver": ["02_process_raw_data/validate_silver.py"],
    "push": ["03_push_to_sql/push_to_sql/push_to_database.py"],
}
TASK_SYNC_ROOTS = {
    "daily_price": ("01_craw_data/daily_price",),
    "product": ("01_craw_data/category", "01_craw_data/product"),
    "review": ("01_craw_data/product", "01_craw_data/review"),
    "preprocess": ("01_craw_data", "02_process_raw_data"),
    "validate_silver": ("02_process_raw_data",),
    "push": ("02_process_raw_data",),
    "bootstrap": ("01_craw_data", "02_process_raw_data"),
}


def client():
    endpoint = os.environ.get("MINIO_ENDPOINT_URL", "http://minio:9000")
    access = os.environ.get("MINIO_ACCESS_KEY")
    secret = os.environ.get("MINIO_SECRET_KEY")
    if not access or not secret:
        raise RuntimeError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be configured.")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def ensure_bucket(s3) -> None:
    from botocore.exceptions import ClientError

    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code not in {"404", "NoSuchBucket", "NotFound"}:
            raise
        s3.create_bucket(Bucket=BUCKET)


def tracked_paths(task_name: str):
    roots = TASK_SYNC_ROOTS[task_name]
    for folder in roots:
        root = DATA_ROOT / folder
        if root.exists():
            for path in root.rglob("*"):
                if path.is_file() and path.suffix.lower() in {".csv", ".json"}:
                    yield path
        if task_name == "bootstrap":
            # Seed a new persistent data volume from the repository's initial
            # CSV/LFS snapshot, without replacing files already present there.
            source_root = PROJECT_DIR / folder
            if source_root.exists():
                for source in source_root.rglob("*"):
                    if not source.is_file() or source.suffix.lower() not in {".csv", ".json"}:
                        continue
                    destination = DATA_ROOT / source.relative_to(PROJECT_DIR)
                    if not destination.exists():
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        destination.write_bytes(source.read_bytes())
                        yield destination
    if task_name in {"push", "bootstrap"}:
        for relative in SYNC_FILES:
            path = DATA_ROOT / relative
            if path.is_file():
                yield path


def object_key(path: Path) -> str:
    relative = path.relative_to(DATA_ROOT).as_posix()
    if relative.startswith("01_craw_data/"):
        return f"current/bronze/{relative.removeprefix('01_craw_data/')}"
    if relative.startswith("02_process_raw_data/"):
        return f"current/silver/{relative.removeprefix('02_process_raw_data/')}"
    return f"current/state/{Path(relative).name}"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def download_current(s3, task_name: str) -> None:
    """MinIO current objects take precedence over the Git bootstrap copy."""
    relevant = TASK_SYNC_ROOTS[task_name]
    should_download_state = task_name in {"push", "bootstrap"}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix="current/"):
        for item in page.get("Contents", []):
            key = item["Key"]
            suffix = key.removeprefix("current/")
            if suffix.startswith("bronze/"):
                relative = f"01_craw_data/{suffix.removeprefix('bronze/')}"
                if not any(relative.startswith(root + "/") for root in relevant):
                    continue
            elif suffix.startswith("silver/"):
                relative = f"02_process_raw_data/{suffix.removeprefix('silver/')}"
                if not any(relative.startswith(root + "/") for root in relevant):
                    continue
            elif suffix.startswith("state/"):
                if not should_download_state:
                    continue
                relative = f"03_push_to_sql/{suffix.removeprefix('state/')}"
            else:
                continue
            destination = DATA_ROOT / Path(relative)
            destination.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(BUCKET, key, str(destination))


def upload_changes(s3, task_name: str, run_id: str) -> int:
    from botocore.exceptions import ClientError

    uploaded = 0
    for path in tracked_paths(task_name):
        key = object_key(path)
        local_hash = sha256(path)
        exists = False
        try:
            old = s3.head_object(Bucket=BUCKET, Key=key)
            exists = True
            old_hash = old.get("Metadata", {}).get("sha256")
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code not in {"404", "NoSuchKey", "NotFound"}:
                raise
            old_hash = None
        if task_name == "bootstrap" and exists:
            # A repeated bootstrap must never replace MinIO's current source of truth.
            continue
        if old_hash == local_hash:
            continue
        if exists:
            archive_key = f"history/{run_id}/{key.removeprefix('current/')}"
            s3.copy_object(
                Bucket=BUCKET,
                Key=archive_key,
                CopySource={"Bucket": BUCKET, "Key": key},
            )
        s3.upload_file(
            str(path),
            BUCKET,
            key,
            ExtraArgs={"Metadata": {"sha256": local_hash, "run-id": run_id}},
        )
        uploaded += 1
    return uploaded


def main(task_name: str) -> None:
    if task_name == "bootstrap":
        command = None
    elif task_name in TASK_COMMANDS:
        command = [sys.executable, *(str(PROJECT_DIR / item) for item in TASK_COMMANDS[task_name])]
    else:
        raise SystemExit(f"Unknown task: {task_name}. Choose one of: bootstrap, {', '.join(TASK_COMMANDS)}")

    s3 = client()
    ensure_bucket(s3)
    download_current(s3, task_name)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{task_name}"
    result = None
    try:
        if command:
            subprocess.run(command, cwd=PROJECT_DIR, check=True)
    except subprocess.CalledProcessError as exc:
        result = exc
    finally:
        count = upload_changes(s3, task_name, run_id)
        print(f"MinIO sync completed: {count} changed object(s); run_id={run_id}")
    if result:
        raise result


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python minio_task.py {bootstrap|daily_price|product|review|preprocess|validate_silver|push}")
    main(sys.argv[1])
