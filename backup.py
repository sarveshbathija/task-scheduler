#!/usr/bin/env python3
"""
Unified backup script — dumps databases and uploads to Google Drive.

To add a new backup target, append a dict to the JOBS list below.
Each job defines how to dump and where to upload. Jobs run independently
so one failure doesn't block the others.
"""

import argparse
import os
import subprocess
import sys
import time
import json
import socket
from datetime import datetime

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ---------------------------------------------------------------------------
# Configuration — add new backup jobs here
# ---------------------------------------------------------------------------

CREDENTIALS_PATH = "/app/credentials/credentials.json"
CHUNK_SIZE = 25 * 1024 * 1024  # 25 MB
SOCKET_TIMEOUT = 600  # seconds

JOBS = [
    {
        "name": "teslamate",
        "dump_cmd": (
            "nice -n 19 ionice -c3 "
            "pg_dump -h {PG_HOST} -p {PG_PORT} -U {PG_USER} {PG_DATABASE} "
            "| gzip"
        ),
        "env_vars": {
            "PGPASSWORD": "PG_PASSWORD",  # maps env var name -> os.environ key
        },
        "file_ext": ".sql.gz",
        "token_path": "/app/credentials/teslamate_token.json",
        "drive_folder_id_env": "TESLAMATE_DRIVE_FOLDER_ID",
    },
    {
        "name": "stocks",
        "dump_cmd": (
            "nice -n 19 ionice -c3 "
            "mysqldump -h {MYSQL_HOST} -P {MYSQL_PORT} -u {MYSQL_USER} "
            "--password={MYSQL_PASSWORD} --skip-ssl "
            "--quick --single-transaction --max_allowed_packet=64M "
            "--routines --triggers --events "
            "{MYSQL_DATABASE} "
            "| gzip"
        ),
        "env_vars": {},
        "file_ext": ".sql.gz",
        "token_path": "/app/credentials/stocks_token.json",
        "drive_folder_id_env": "STOCKS_DRIVE_FOLDER_ID",
    },
]

# ---------------------------------------------------------------------------
# Google Drive helpers
# ---------------------------------------------------------------------------

def authenticate(token_path: str) -> Credentials:
    """Load and refresh OAuth credentials from token file."""
    with open(token_path, "r") as f:
        creds_json = json.load(f)

    creds = Credentials.from_authorized_user_info(creds_json)
    creds.token = creds_json["token"]

    if creds.expired and creds.refresh_token:
        print(f"  Refreshing expired token: {token_path}")
        creds.refresh(Request())
        # Persist the refreshed token
        with open(token_path, "w") as f:
            json.dump(json.loads(creds.to_json()), f, indent=2)

    return creds


def upload_to_drive(file_path: str, folder_id: str, creds: Credentials) -> str:
    """Upload a file to Google Drive with chunked resumable upload. Returns file ID."""
    socket.setdefaulttimeout(SOCKET_TIMEOUT)
    service = build("drive", "v3", credentials=creds)

    file_metadata = {
        "name": os.path.basename(file_path),
        "parents": [folder_id],
    }
    media = MediaFileUpload(file_path, chunksize=CHUNK_SIZE, resumable=True)

    request = service.files().create(body=file_metadata, media_body=media, fields="id")

    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    bar_width = 30
    last_milestone = -1

    response = None
    while response is None:
        status, response = request.next_chunk()
        if status:
            pct = status.progress() * 100
            milestone = int(pct // 5) * 5
            if milestone > last_milestone:
                last_milestone = milestone
                filled = int(bar_width * pct / 100)
                bar = "█" * filled + "░" * (bar_width - filled)
                uploaded_mb = file_size_mb * pct / 100
                print(f"  [{bar}] {pct:5.1f}%  ({uploaded_mb:.0f}/{file_size_mb:.0f} MB)")

    file_id = response.get("id")
    print(f"  [{('█' * bar_width)}] 100.0%  Upload complete. Drive file ID: {file_id}")
    return file_id


# ---------------------------------------------------------------------------
# Backup logic
# ---------------------------------------------------------------------------

def run_job(job: dict) -> bool:
    """Execute a single backup job (dump + upload). Returns True on success."""
    name = job["name"]
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    dump_file = f"/tmp/{name}_{timestamp}{job['file_ext']}"

    print(f"\n{'='*60}")
    print(f"[{timestamp}] Starting backup: {name}")
    print(f"{'='*60}")

    # --- Dump ---
    try:
        cmd = job["dump_cmd"].format(**os.environ)
    except KeyError as e:
        print(f"  ERROR: Missing environment variable: {e}")
        return False

    # Set extra env vars needed by the dump tool (e.g. PGPASSWORD)
    dump_env = os.environ.copy()
    for env_key, source_key in job.get("env_vars", {}).items():
        dump_env[env_key] = os.environ[source_key]

    print(f"  Dumping to {dump_file} ...")
    try:
        with open(dump_file, "wb") as outf:
            result = subprocess.run(
                cmd, shell=True, stdout=outf, stderr=subprocess.PIPE,
                env=dump_env, timeout=7200,  # 2 hour timeout
            )
        if result.returncode != 0:
            print(f"  ERROR: Dump failed (exit {result.returncode})")
            print(f"  stderr: {result.stderr.decode().strip()}")
            _cleanup(dump_file)
            return False
    except subprocess.TimeoutExpired:
        print("  ERROR: Dump timed out after 2 hours")
        _cleanup(dump_file)
        return False

    file_size = os.path.getsize(dump_file)
    print(f"  Dump complete. Size: {file_size / (1024*1024):.1f} MB")

    if file_size == 0:
        print("  ERROR: Dump file is empty, skipping upload")
        _cleanup(dump_file)
        return False

    # --- Upload ---
    folder_id = os.environ.get(job["drive_folder_id_env"])
    if not folder_id:
        print(f"  ERROR: Missing env var {job['drive_folder_id_env']}")
        _cleanup(dump_file)
        return False

    try:
        print(f"  Authenticating with Google Drive ...")
        creds = authenticate(job["token_path"])
        print(f"  Uploading to Drive folder {folder_id} ...")
        upload_to_drive(dump_file, folder_id, creds)
    except Exception as e:
        print(f"  ERROR: Upload failed: {e}")
        _cleanup(dump_file)
        return False

    # --- Cleanup ---
    _cleanup(dump_file)
    print(f"  Backup complete for {name}")
    return True


def _cleanup(path: str):
    """Remove a file if it exists."""
    try:
        if os.path.exists(path):
            os.remove(path)
            print(f"  Cleaned up {path}")
    except OSError as e:
        print(f"  Warning: Could not remove {path}: {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Database backup to Google Drive")
    parser.add_argument("--job", help="Run a specific job by name (default: all jobs)")
    args = parser.parse_args()

    if args.job:
        jobs_to_run = [j for j in JOBS if j["name"] == args.job]
        if not jobs_to_run:
            valid = ", ".join(j["name"] for j in JOBS)
            print(f"ERROR: Unknown job '{args.job}'. Valid jobs: {valid}")
            sys.exit(1)
    else:
        jobs_to_run = JOBS

    start = time.time()
    print(f"\n{'#'*60}")
    print(f"# Backup run started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}")

    results = {}
    for job in jobs_to_run:
        try:
            results[job["name"]] = run_job(job)
        except Exception as e:
            print(f"  UNEXPECTED ERROR in {job['name']}: {e}")
            results[job["name"]] = False

    # --- Summary ---
    elapsed = time.time() - start
    print(f"\n{'='*60}")
    print(f"Backup summary ({elapsed:.0f}s elapsed):")
    for name, ok in results.items():
        status = "OK" if ok else "FAILED"
        print(f"  {name}: {status}")
    print(f"{'='*60}\n")

    if not all(results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
