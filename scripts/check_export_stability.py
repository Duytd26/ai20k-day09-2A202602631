"""Run in the Airflow container after step all; save real export comparisons."""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from pipeline import config as C, publish, store


def main():
    report = Path("/opt/lab/report/evidence")
    summary = json.loads((report / "run_summary.json").read_text())
    version = summary["dataset_version"]
    before = json.loads(store.s3().get_object(Bucket=C.BUCKET, Key=f"gold/datasets/{version}/manifest.json")["Body"].read())
    after = publish.export_dataset()
    keys = ("dataset_version", "frames", "by_split", "trips_by_split")
    unchanged = all(before[k] == after[k] for k in keys)
    evidence = {"student_id": os.environ["LAB_STUDENT_ID"], "ran_at": datetime.now(timezone.utc).isoformat(),
                "experiment": "export_stability", "before": {k: before[k] for k in keys},
                "after": {k: after[k] for k in keys}, "unchanged": unchanged}
    (report / "export_stability.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    manifest = json.loads(store.s3().get_object(Bucket=C.BUCKET, Key=f"gold/datasets/{after['dataset_version']}/manifest.json")["Body"].read())
    (report / "dataset_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(evidence, indent=2))
    if not unchanged:
        raise SystemExit("Export changed the dataset")


if __name__ == "__main__":
    main()
