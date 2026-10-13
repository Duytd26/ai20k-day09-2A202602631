"""Add the lab's lighting analysis using columns present in the warehouse."""
from setup_metabase import MB, USER, QUESTIONS, call, wait_ready
from datetime import datetime, timezone
from pathlib import Path
import json

SQL = """select lighting,
       sum(frames) as tong_so_khung,
       sum(frames - frames_kept) as so_khung_bi_loai,
       round(sum(frames - frames_kept) * 100.0 / nullif(sum(frames), 0), 2) as ty_le_loai_pct
from marts.mart_trip_summary
group by lighting
order by lighting;"""

BONUS_SQL = """select trip_id, drive_id, frames, frames_kept,
       frames - frames_kept as frames_dropped,
       round((frames - frames_kept) * 100.0 / nullif(frames, 0), 2) as dropped_pct,
       lighting, conditions
from marts.mart_trip_summary
order by dropped_pct desc, trip_id
limit 5;"""


def main():
    wait_ready()
    token = call("POST", "/api/session", {"username": USER["email"], "password": USER["password"]})["id"]
    dbs = call("GET", "/api/database", token=token)
    dbs = dbs["data"] if isinstance(dbs, dict) else dbs
    db = next(d for d in dbs if d["name"] == "warehouse")
    name = "Ty le khung bi loai khoi Gold theo anh sang"
    cards = call("GET", "/api/card", token=token)
    card = next((c for c in cards if c["name"] == name), None)
    if card is None:
        card = call("POST", "/api/card", {"name": name, "display": "table", "visualization_settings": {},
                    "dataset_query": {"type": "native", "database": db["id"], "native": {"query": SQL}}}, token=token)
    result = call("POST", f"/api/card/{card['id']}/query", {}, token=token)
    print("query result:", result["data"]["rows"])
    dashboards = call("GET", "/api/dashboard", token=token)
    dashboard_name = "Day 9: 2A202602631 - Tran Dinh Duy"
    dashboard = next((d for d in dashboards if d["name"] == dashboard_name), None)
    if dashboard is None:
        dashboard = call("POST", "/api/dashboard", {"name": dashboard_name}, token=token)
    full = call("GET", f"/api/dashboard/{dashboard['id']}", token=token)
    dashcards = full["dashcards"]
    if not any(c.get("card_id") == card["id"] for c in dashcards):
        clean = []
        for i, (question_name, _, _) in enumerate(QUESTIONS):
            question = next(c for c in cards if c["name"] == question_name)
            clean.append({"id": -(i + 1), "card_id": question["id"], "row": 4 + (i // 2) * 6,
                          "col": (i % 2) * 12, "size_x": 12, "size_y": 6, "visualization_settings": {}, "parameter_mappings": []})
        clean.append({"id": -100, "card_id": card["id"], "row": 0,
                      "col": 0, "size_x": 24, "size_y": 4, "visualization_settings": {}, "parameter_mappings": []})
        call("PUT", f"/api/dashboard/{dashboard['id']}", {"dashcards": clean}, token=token)
    print("dashboard:", f"{MB}/dashboard/{dashboard['id']}")
    print("question:", f"{MB}/question/{card['id']}")
    bonus_name = "Top 5 trip co ty le khung bi loai cao nhat - 2A202602631"
    bonus = next((c for c in cards if c["name"] == bonus_name), None)
    if bonus is None:
        bonus = call("POST", "/api/card", {"name": bonus_name, "display": "table", "visualization_settings": {},
                     "dataset_query": {"type": "native", "database": db["id"], "native": {"query": BONUS_SQL}}}, token=token)
    result = call("POST", f"/api/card/{bonus['id']}/query", {}, token=token)
    if result.get("status") != "completed":
        raise RuntimeError(f"Bonus query failed: {result.get('error')}")
    print("bonus result:", result["data"]["rows"])
    full = call("GET", f"/api/dashboard/{dashboard['id']}", token=token)
    if not any(c.get("card_id") == bonus["id"] for c in full["dashcards"]):
        clean = [{k: c[k] for k in ("id", "card_id", "row", "col", "size_x", "size_y", "visualization_settings", "parameter_mappings") if k in c}
                 for c in full["dashcards"]]
        for c in clean:
            if c["row"] >= 4:
                c["row"] += 5
        clean.append({"id": -101, "card_id": bonus["id"], "row": 4, "col": 0, "size_x": 24, "size_y": 5,
                      "visualization_settings": {}, "parameter_mappings": []})
        call("PUT", f"/api/dashboard/{dashboard['id']}", {"dashcards": clean}, token=token)
    evidence = {"student_id": "2A202602631", "ran_at": datetime.now(timezone.utc).isoformat(),
                "experiment": "bonus_metabase_top_dropped_trips", "query": BONUS_SQL,
                "columns": [c["name"] for c in result["data"]["cols"]], "rows": result["data"]["rows"],
                "dashboard_url": f"{MB}/dashboard/{dashboard['id']}", "question_url": f"{MB}/question/{bonus['id']}"}
    path = Path(__file__).resolve().parent.parent / "report" / "evidence" / "bonus_metabase.json"
    path.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    print("bonus question:", evidence["question_url"])


if __name__ == "__main__":
    main()
