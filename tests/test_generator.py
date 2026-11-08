"""Smoke tests for the batch simulator: same seed -> same output, manifest written, batches advance."""
import hashlib
import json
import subprocess
import sys
from pathlib import Path

GEN = Path(__file__).resolve().parents[1] / "data_gen" / "generate_batch.py"


def run_generator(tmp_path: Path, name: str, batches: int = 2, seed: int = 7) -> Path:
    out, state = tmp_path / name / "output", tmp_path / name / "state.json"
    subprocess.run(
        [sys.executable, str(GEN), "--out-dir", str(out), "--state-file", str(state),
         "--batches", str(batches), "--start", "2026-10-01T00:00:00Z", "--seed", str(seed), "--reset"],
        check=True, capture_output=True, text=True,
    )
    return out


def digest(out: Path) -> str:
    h = hashlib.sha256()
    for f in sorted(p for p in out.rglob("*") if p.is_file()):
        h.update(f.relative_to(out).as_posix().encode())
        h.update(f.read_bytes())
    return h.hexdigest()


def test_same_seed_gives_identical_output(tmp_path):
    assert digest(run_generator(tmp_path, "a")) == digest(run_generator(tmp_path, "b"))


def test_different_seed_changes_output(tmp_path):
    assert digest(run_generator(tmp_path, "a", seed=7)) != digest(run_generator(tmp_path, "b", seed=8))


def test_one_manifest_per_batch_and_valid_json(tmp_path):
    out = run_generator(tmp_path, "a", batches=3)
    manifests = sorted((out / "_manifests").glob("*.json"))
    assert len(manifests) == 3
    for m in manifests:
        json.loads(m.read_text())  # manifest is valid JSON


def test_data_files_are_newline_delimited_json(tmp_path):
    out = run_generator(tmp_path, "a")
    files = [p for p in out.rglob("*.json") if "_manifests" not in p.parts]
    assert files
    for f in files:
        for line in f.read_text().splitlines():
            assert isinstance(json.loads(line), dict)
