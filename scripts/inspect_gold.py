"""Inspect real Gold objects through MinIO's S3 API and save their counts."""
import json
import os
from pathlib import Path
from pipeline import config as C, store

report = Path("/opt/lab/report/evidence")
summary = json.loads((report / "run_summary.json").read_text())
version = summary["dataset_version"]
prefix = f"gold/datasets/{version}/"
s3 = store.s3()
listing = s3.list_objects_v2(Bucket=C.BUCKET, Prefix=prefix, Delimiter="/")
folders = [p["Prefix"].removeprefix(prefix).rstrip("/") for p in listing.get("CommonPrefixes", [])]
files = [p["Key"].removeprefix(prefix) for p in listing.get("Contents", [])]
counts = {}
for split in ("train", "val", "test"):
    counts[split] = sum(o["Key"].endswith(".jpg") for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=C.BUCKET, Prefix=f"{prefix}{split}/") for o in page.get("Contents", []))
ok = set(folders) == {"train", "val", "test"} and "manifest.json" in files and counts == summary["by_split"]
evidence = {"student_id": os.environ["LAB_STUDENT_ID"], "dataset_version": version,
            "bucket": C.BUCKET, "prefix": prefix, "folders": sorted(folders), "files": sorted(files),
            "jpg_counts": counts, "matches_run_summary": ok}
(report / "gold_dataset_contents.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
print(json.dumps(evidence, indent=2))
if not ok:
    raise SystemExit("Gold objects do not match run_summary")
