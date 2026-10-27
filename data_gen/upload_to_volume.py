#!/usr/bin/env python3
"""
upload_to_volume.py - copy generated batch files into a Databricks Unity Catalog Volume.

Uses the Databricks Files API (PUT /api/2.0/fs/files/<volume path>) with only the Python
standard library. Reads credentials from environment variables, never from code:
    DATABRICKS_HOST   e.g. https://dbc-xxxx.cloud.databricks.com
    DATABRICKS_TOKEN  a personal access token

Already-uploaded files are remembered in a small state file, so re-running only sends
new batches (safe to call from Airflow after every generator run).
Data files go first and manifests last, so a manifest in the volume means "batch complete".
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


def clean_host(host: str) -> str:
    """Keep only scheme://hostname, so a pasted URL with /?o=123 or a trailing path still works."""
    host = host.strip()
    if "://" not in host:
        host = "https://" + host
    u = urllib.parse.urlparse(host)
    return f"{u.scheme}://{u.netloc}"


def ensure_dir(host: str, token: str, dir_path: str) -> None:
    """mkdir -p inside the volume (idempotent): creates landing/, <table>/, dt=.../ as needed."""
    url = f"{host}/api/2.0/fs/directories{urllib.parse.quote(dir_path, safe='/')}"
    req = urllib.request.Request(url, data=b"", method="PUT", headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=60):
        pass


def upload_one(host: str, token: str, volume_path: str, local: Path) -> None:
    url = f"{host}/api/2.0/fs/files{urllib.parse.quote(volume_path, safe='/')}?overwrite=true"
    req = urllib.request.Request(
        url,
        data=local.read_bytes(),
        method="PUT",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/octet-stream"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        if resp.status not in (200, 201, 204):
            raise RuntimeError(f"unexpected status {resp.status}")


def main() -> int:
    p = argparse.ArgumentParser(description="Upload generator output to a Databricks Volume.")
    p.add_argument("--src", default="data_gen/output", help="local folder written by generate_batch.py")
    p.add_argument("--volume", default="/Volumes/workspace/warranty/raw/landing",
                   help="target folder inside the volume")
    p.add_argument("--state", default="data_gen/state/uploaded.json", help="remembers files already sent")
    p.add_argument("--limit", type=int, default=0, help="upload at most N files (use 1 for a first test)")
    p.add_argument("--dry-run", action="store_true", help="list what would be uploaded, send nothing")
    args = p.parse_args()

    host, token = os.environ.get("DATABRICKS_HOST"), os.environ.get("DATABRICKS_TOKEN")
    if not args.dry_run and not (host and token):
        print("DATABRICKS_HOST and DATABRICKS_TOKEN must be set (Codespaces secrets).", file=sys.stderr)
        return 2

    if host:
        host = clean_host(host)
    src, state_path = Path(args.src), Path(args.state)
    done = set(json.loads(state_path.read_text())) if state_path.exists() else set()

    files = [f for f in src.rglob("*.json") if not f.name.startswith(".")]
    # data first, manifests last; stable order by path
    files.sort(key=lambda f: (f.parts[len(src.parts)] == "_manifests", str(f)))
    todo = [f for f in files if str(f.relative_to(src)) not in done]
    if args.limit:
        todo = todo[: args.limit]
    print(f"{len(files)} files found, {len(files) - len([f for f in files if str(f.relative_to(src)) not in done])} already uploaded, {len(todo)} to upload")

    made_dirs: set[str] = set()
    sent = 0
    for f in todo:
        rel = str(f.relative_to(src))
        target = f"{args.volume.rstrip('/')}/{rel}"
        if args.dry_run:
            print("would upload", rel, "->", target)
            continue
        step = "create folder"
        try:
            parent = target.rsplit("/", 1)[0]
            if parent not in made_dirs:
                ensure_dir(host, token, parent)
                made_dirs.add(parent)
            step = "upload file"
            upload_one(host, token, target, f)
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")[:300]
            print(f"FAILED to {step} for {rel}: HTTP {e.code} {body}", file=sys.stderr)
            print(f"Host used: {host}", file=sys.stderr)
            if e.code in (401, 403):
                print("Check the token (expired? copied fully?) and your workspace URL.", file=sys.stderr)
            elif e.code == 404:
                print("Check the volume exists and the path is right: " + args.volume
                      + " (the first three parts must be catalog/schema/volume)", file=sys.stderr)
            break
        except Exception as e:  # network errors etc.
            print(f"FAILED {rel}: {e}", file=sys.stderr)
            break
        done.add(rel)
        sent += 1
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(json.dumps(sorted(done)))
    print(f"uploaded {sent} file(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())