"""warranty_pipeline: simulated source -> S3 landing -> Databricks volume (bronze load and dbt are added next)."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
import pendulum

import boto3
from botocore.exceptions import ClientError
from airflow.sdk import dag, task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

REPO = Path("/workspaces/Warranty-lakehouse")
GEN = REPO / "data_gen"
OUT = GEN / "output"
STATE = GEN / "state" / "state.json"
BUCKET = "warranty-lakehouse-raw-your-initials-2026"
PREFIX = "raw"
VOLUME = "/Volumes/workspace/warranty/raw/landing"
DATABRICKS_JOB_ID = int(os.environ.get("WARRANTY_JOB_ID", "818308994668188"))  # job warranty_dbt_build
BATCHES_PER_RUN = os.environ.get("BATCHES_PER_RUN", "6")  # 6 x 4h = one simulated day per run

sys.path.insert(0, str(GEN))
import upload_to_volume as vol  # reuse clean_host / ensure_dir / upload_one (Files API helpers)

def notify_failure(context):
    """Push a phone alert when a task has failed for good (after its retries)."""
    topic = os.environ.get("NTFY_TOPIC")
    if not topic:
        print("NTFY_TOPIC is not set; skipping the alert")
        return
    ti = context["task_instance"]
    msg = f"{ti.dag_id} / {ti.task_id} failed (run {ti.run_id})"
    req = urllib.request.Request(
        f"https://ntfy.sh/{topic}",
        data=msg.encode("utf-8"),
        headers={"Title": "Warranty pipeline failed", "Priority": "high", "Tags": "rotating_light"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:  # an alert must never crash the callback
        print(f"could not send the alert: {e}")
def s3_manifest_names(s3) -> set[str]:
    names = set()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=f"{PREFIX}/_manifests/"):
        for obj in page.get("Contents", []):
            name = obj["Key"].rsplit("/", 1)[-1]
            if name.endswith(".json"):
                names.add(name)
    return names


def volume_manifest_names(host: str, token: str) -> set[str]:
    names, page_token = set(), None
    while True:
        url = f"{host}/api/2.0/fs/directories{VOLUME}/_manifests/"
        if page_token:
            url += "?page_token=" + urllib.parse.quote(page_token)
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 404:  # folder does not exist yet
                return names
            raise
        names |= {c["name"] for c in data.get("contents", []) if not c.get("is_directory")}
        page_token = data.get("next_page_token")
        if not page_token:
            return names


@dag(
    dag_id="warranty_pipeline",
    schedule="0 6 * * *",  # daily 06:00 IST
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Calcutta"),
    catchup=False,
    max_active_runs=1,  # the generator keeps state, so two runs at once would clash
    default_args={"on_failure_callback": notify_failure},
    tags=["warranty", "ingestion"],
)
def warranty_pipeline():
    @task
    def generate_batch() -> list[str]:
        """Run the generator for one batch and return the names of the new manifest files."""
        manifests = OUT / "_manifests"
    
        before = {p.name for p in manifests.glob("*.json")}
        cmd = [
            sys.executable, str(GEN / "generate_batch.py"),
            "--out-dir", str(OUT),
            "--state-file", str(STATE),
            "--batches", BATCHES_PER_RUN,
        ]
        result = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            raise RuntimeError(f"generator failed:\n{result.stderr}")
        new = sorted({p.name for p in manifests.glob("*.json")} - before)
        if not new:
            raise RuntimeError("generator ran but no new manifest appeared")
        print(f"new batches: {new}")
        return new

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def upload_to_s3(new_manifests: list[str]) -> list[str]:
        """Upload each new batch to S3: data files first, manifest last ('batch committed')."""
        s3 = boto3.client("s3")  # credentials and region come from the AWS_* environment variables
        for name in new_manifests:
            manifest_path = OUT / "_manifests" / name
            manifest = json.loads(manifest_path.read_text())
            for table, rel in manifest["files"].items():
                local = OUT / rel
                if not local.exists():
                    if manifest["rows"].get(table, 0) == 0:
                        print(f"skip {table}: 0 rows, no file")
                        continue
                    raise FileNotFoundError(f"manifest lists {rel} but it is missing")
                s3.upload_file(str(local), BUCKET, f"{PREFIX}/{rel}")
                print(f"uploaded {rel}")
            s3.upload_file(str(manifest_path), BUCKET, f"{PREFIX}/_manifests/{name}")
            print(f"uploaded manifest {name}")
        return new_manifests

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def s3_to_volume(uploaded: list[str]) -> list[str]:
        """Copy every batch that is in S3 but not yet in the Databricks volume (data first, manifest last)."""
        host_env, token = os.environ.get("DATABRICKS_HOST"), os.environ.get("DATABRICKS_TOKEN")
        if not (host_env and token):
            raise RuntimeError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")
        host = vol.clean_host(host_env)
        s3 = boto3.client("s3")

        todo = sorted(s3_manifest_names(s3) - volume_manifest_names(host, token))
        print(f"batches to copy: {todo}")
        for name in todo:
            with tempfile.TemporaryDirectory() as tmp:
                key = f"{PREFIX}/_manifests/{name}"
                manifest_local = Path(tmp) / name
                manifest_local.write_bytes(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
                manifest = json.loads(manifest_local.read_text())
                for table, rel in manifest["files"].items():
                    local = Path(tmp) / Path(rel).name
                    try:
                        local.write_bytes(s3.get_object(Bucket=BUCKET, Key=f"{PREFIX}/{rel}")["Body"].read())
                    except ClientError as e:
                        if e.response["Error"]["Code"] == "NoSuchKey" and manifest["rows"].get(table, 0) == 0:
                            print(f"skip {table}: 0 rows, no file")
                            continue
                        raise
                    vol.ensure_dir(host, token, f"{VOLUME}/{Path(rel).parent}")
                    vol.upload_one(host, token, f"{VOLUME}/{rel}", local)
                    print(f"copied {rel}")
                vol.ensure_dir(host, token, f"{VOLUME}/_manifests")
                vol.upload_one(host, token, f"{VOLUME}/_manifests/{name}", manifest_local)
                print(f"copied manifest {name}")
        return todo

    copied = s3_to_volume(upload_to_s3(generate_batch()))
    run_job = DatabricksRunNowOperator(
        task_id="run_databricks_job",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOB_ID,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )
    copied >> run_job



warranty_pipeline()