"""Restore current data from MinIO, run one existing ETL script, archive changes."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

PROJECT_DIR = Path(os.environ.get("TIKI_PROJECT_DIR", Path(__file__).resolve().parents[3]))
DATA_ROOT = Path(os.environ.get("TIKI_DATA_DIR", PROJECT_DIR)).resolve()
BUCKET = os.environ.get("MINIO_BUCKET", "tiki-lake")
SYNC_FILES = ("03_push_to_sql/sync_checkpoint_supabase.json",)
TASK_COMMANDS = {
    "daily_price": ["01_craw_data/daily_price/crawl_daily_price.py"],
    "product": ["01_craw_data/product/crawl_product.py"],
    "review": ["01_craw_data/review/crawl_review.py"],
    "preprocess": ["02_process_raw_data/run_all_preprocessing.py"],
    "validate_silver": ["02_process_raw_data/validate_silver.py"],
    "push": ["03_push_to_sql/push_to_database.py"],
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
    import boto3
    from botocore.config import Config

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
            if destination.is_file():
                try:
                    remote = s3.head_object(Bucket=BUCKET, Key=key)
                    expected_hash = remote.get("Metadata", {}).get("sha256")
                    if expected_hash and sha256(destination) == expected_hash:
                        continue
                except Exception:
                    # Missing metadata or an unavailable HEAD falls back to download.
                    pass
            s3.download_file(BUCKET, key, str(destination))


def download_staged_silver(s3, run_id: str) -> int:
    """Restore the exact staged generation that this DAG run will validate."""
    # Start from known-good Silver and overlay this run's staged delta.
    download_current(s3, "validate_silver")
    prefix = staging_silver_prefix(run_id)
    paginator = s3.get_paginator("list_objects_v2")
    downloaded = 0
    found_manifest = False
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for item in page.get("Contents", []):
            relative = item["Key"][len(prefix):]
            if not relative:
                continue
            if relative == "_complete.json":
                found_manifest = True
                continue
            destination = DATA_ROOT / "02_process_raw_data" / Path(relative)
            destination.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(BUCKET, item["Key"], str(destination))
            downloaded += 1
    if not found_manifest:
        raise RuntimeError(f"No staged Silver generation found for run {run_id!r}")
    return downloaded


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


def safe_run_id(run_id: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", run_id).strip("._-")
    if not cleaned:
        raise ValueError("run_id must contain at least one safe character")
    return cleaned[:160]


def snapshot_silver() -> Path:
    """Back up local Silver files so failed transforms can be rolled back."""
    silver_root = DATA_ROOT / "02_process_raw_data"
    backup_root = Path(tempfile.mkdtemp(prefix="tiki-silver-backup-", dir=DATA_ROOT))
    if silver_root.exists():
        for source in silver_root.rglob("*"):
            if source.is_file() and source.suffix.lower() in {".csv", ".json"}:
                target = backup_root / source.relative_to(silver_root)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target)
    return backup_root


def restore_silver(backup_root: Path) -> None:
    """Restore previous files and remove new partial Silver outputs."""
    silver_root = DATA_ROOT / "02_process_raw_data"
    backup_files = {
        path.relative_to(backup_root)
        for path in backup_root.rglob("*")
        if path.is_file()
    }
    if silver_root.exists():
        for current in silver_root.rglob("*"):
            if not current.is_file() or current.suffix.lower() not in {".csv", ".json"}:
                continue
            relative = current.relative_to(silver_root)
            if relative not in backup_files:
                current.unlink()
    for relative in backup_files:
        source = backup_root / relative
        destination = silver_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)


def staging_silver_prefix(run_id: str) -> str:
    return f"staging/{safe_run_id(run_id)}/silver/"


def upload_staged_silver(s3, run_id: str) -> int:
    """Stage changed Silver objects without replacing the known-good current set."""
    silver_root = DATA_ROOT / "02_process_raw_data"
    uploaded = 0
    for path in silver_root.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in {".csv", ".json"}:
            continue
        relative = path.relative_to(silver_root).as_posix()
        digest = sha256(path)
        current_key = f"current/silver/{relative}"
        try:
            current = s3.head_object(Bucket=BUCKET, Key=current_key)
            if current.get("Metadata", {}).get("sha256") == digest:
                continue
        except Exception as exc:
            code = getattr(exc, "response", {}).get("Error", {}).get("Code")
            if code not in {"404", "NoSuchKey", "NotFound"}:
                raise
        s3.upload_file(
            str(path),
            BUCKET,
            staging_silver_prefix(run_id) + relative,
            ExtraArgs={
                "Metadata": {
                    "sha256": digest,
                    "run-id": safe_run_id(run_id),
                }
            },
        )
        uploaded += 1
    s3.put_object(
        Bucket=BUCKET,
        Key=staging_silver_prefix(run_id) + "_complete.json",
        Body=json.dumps({"run_id": safe_run_id(run_id), "staged_objects": uploaded}).encode("utf-8"),
        ContentType="application/json",
    )
    return uploaded


def promote_staged_silver(s3, run_id: str) -> int:
    """Atomically-at-task-level promote validated staged objects into current."""
    prefix = staging_silver_prefix(run_id)
    paginator = s3.get_paginator("list_objects_v2")
    staged_keys = [
        item["Key"]
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix)
        for item in page.get("Contents", [])
    ]
    manifest_key = prefix + "_complete.json"
    if manifest_key not in staged_keys:
        raise RuntimeError(f"No staged Silver generation found for run {run_id!r}")
    staged_keys = [key for key in staged_keys if key != manifest_key]
    if not staged_keys:
        s3.delete_object(Bucket=BUCKET, Key=manifest_key)
        return 0

    run_token = safe_run_id(run_id)
    completed: list[tuple[str, str | None]] = []
    try:
        for staged_key in staged_keys:
            relative = staged_key[len(prefix):]
            current_key = f"current/silver/{relative}"
            archive_key = None
            try:
                s3.head_object(Bucket=BUCKET, Key=current_key)
                archive_key = f"history/{run_token}/silver/{relative}"
                s3.copy_object(
                    Bucket=BUCKET,
                    Key=archive_key,
                    CopySource={"Bucket": BUCKET, "Key": current_key},
                )
            except Exception as exc:
                code = getattr(exc, "response", {}).get("Error", {}).get("Code")
                if code not in {"404", "NoSuchKey", "NotFound"}:
                    raise
            completed.append((current_key, archive_key))
            s3.copy_object(
                Bucket=BUCKET,
                Key=current_key,
                CopySource={"Bucket": BUCKET, "Key": staged_key},
            )
    except Exception:
        # If a multi-object promotion fails halfway through, undo completed copies.
        for current_key, archive_key in reversed(completed):
            if archive_key:
                s3.copy_object(
                    Bucket=BUCKET,
                    Key=current_key,
                    CopySource={"Bucket": BUCKET, "Key": archive_key},
                )
            else:
                s3.delete_object(Bucket=BUCKET, Key=current_key)
        raise
    for staged_key in [*staged_keys, manifest_key]:
        try:
            s3.delete_object(Bucket=BUCKET, Key=staged_key)
        except Exception as exc:
            print(f"Warning: staged object cleanup failed for {staged_key}: {exc}")
    return len(completed)


def main(task_name: str, airflow_run_id: str | None = None, run_date: str | None = None) -> None:
    if task_name == "bootstrap":
        command = None
    elif task_name in TASK_COMMANDS:
        command = [sys.executable, *(str(PROJECT_DIR / item) for item in TASK_COMMANDS[task_name])]
    else:
        raise SystemExit(f"Unknown task: {task_name}. Choose one of: bootstrap, {', '.join(TASK_COMMANDS)}")

    if task_name == "validate_silver" and not airflow_run_id:
        raise ValueError("validate_silver requires the Airflow run_id to select staged data")

    if run_date:
        # Pass Airflow's logical date to child scripts so delayed runs retain
        # the date/month they were scheduled for instead of using wall-clock now.
        os.environ["TIKI_RUN_DATE"] = run_date

    run_id = airflow_run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{task_name}"
    s3 = client()
    ensure_bucket(s3)
    if task_name == "validate_silver":
        download_staged_silver(s3, run_id)
    else:
        download_current(s3, task_name)

    backup_root = snapshot_silver() if task_name == "preprocess" else None
    result = None
    try:
        if command:
            subprocess.run(command, cwd=PROJECT_DIR, check=True)
        if task_name == "preprocess" and not airflow_run_id:
            # Standalone CLI use has no following Airflow validation task.
            subprocess.run(
                [sys.executable, str(PROJECT_DIR / "02_process_raw_data" / "validate_silver.py")],
                cwd=PROJECT_DIR,
                check=True,
            )
    except Exception as exc:
        result = exc

    if task_name == "preprocess":
        try:
            if result:
                restore_silver(backup_root)
                print("Preprocessing failed; previous Silver files restored; nothing published.")
            else:
                count = upload_staged_silver(s3, run_id)
                print(f"Silver staged in MinIO: {count} object(s); run_id={safe_run_id(run_id)}")
                if not airflow_run_id:
                    promoted = promote_staged_silver(s3, run_id)
                    print(f"Standalone validated Silver promoted: {promoted} object(s)")
        except Exception:
            restore_silver(backup_root)
            raise
        finally:
            shutil.rmtree(backup_root, ignore_errors=True)
    elif task_name == "validate_silver":
        if result:
            print("Silver validation failed; staged generation was not promoted.")
        else:
            count = promote_staged_silver(s3, run_id)
            print(f"Validated Silver promoted to current: {count} object(s); run_id={safe_run_id(run_id)}")
    else:
        # Preserve partial Bronze/checkpoint updates after crawler or loader
        # failures so Airflow retries can resume from persisted progress.
        count = upload_changes(s3, task_name, safe_run_id(run_id))
        print(f"MinIO sync completed: {count} changed object(s); run_id={safe_run_id(run_id)}")
    if result:
        raise result


if __name__ == "__main__":
    if not 2 <= len(sys.argv) <= 4:
        raise SystemExit("Usage: python minio_task.py {bootstrap|daily_price|product|review|preprocess|validate_silver|push} [airflow_run_id] [run_date]")
    main(
        sys.argv[1],
        sys.argv[2] if len(sys.argv) >= 3 else None,
        sys.argv[3] if len(sys.argv) >= 4 else None,
    )
