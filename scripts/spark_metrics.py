"""Summarise jobs, stages and tasks of Spark applications from the History Server REST API.

    python scripts/spark_metrics.py [--server http://localhost:18080] [--out docs/outputs/part4_metrics.md]

Used for Part IV (execution analysis): for the latest run of each application name it lists every
job with the action that triggered it, its stages (with task counts, input and shuffle sizes) and
the executors that ran the tasks.
"""

from __future__ import annotations

import argparse
import json
import urllib.request
from pathlib import Path


def get(server: str, path: str):
    with urllib.request.urlopen(f"{server}/api/v1{path}", timeout=30) as resp:
        return json.load(resp)


def kb(n: int) -> str:
    return f"{n / 1024:.1f} KB" if n else "-"


def describe(server: str, app: dict) -> str:
    app_id = app["id"]
    attempt = app["attempts"][0]
    lines = [
        f"## {app['name']}",
        "",
        f"Application `{app_id}` · duration {attempt['duration'] / 1000:.1f} s · Spark {attempt.get('appSparkVersion', '?')}",
        "",
    ]

    executors = [e for e in get(server, f"/applications/{app_id}/allexecutors") if e["id"] != "driver"]
    lines.append(
        "Executors: "
        + ", ".join(f"#{e['id']} on {e['hostPort'].split(':')[0]} ({e['totalTasks']} tasks)" for e in executors)
    )
    lines.append("")

    jobs = sorted(get(server, f"/applications/{app_id}/jobs"), key=lambda j: j["jobId"])
    stages = {s["stageId"]: s for s in get(server, f"/applications/{app_id}/stages?status=complete")}
    skipped = {s["stageId"] for s in get(server, f"/applications/{app_id}/stages?status=skipped")}

    lines += [
        "| Job | Triggered by (action) | Stages run | Stages skipped | Tasks run |",
        "|---|---|---|---|---|",
    ]
    total_tasks = 0
    for j in jobs:
        ran = [s for s in j["stageIds"] if s in stages]
        skip = [s for s in j["stageIds"] if s in skipped]
        total_tasks += j["numCompletedTasks"]
        action = j["name"].split(" at ")[0]
        where = j["name"].split(" at ")[-1]
        desc = j.get("description") or ""
        label = f"`{action}` at {where}" + (f" — {desc}" if desc else "")
        lines.append(
            f"| {j['jobId']} | {label} | {', '.join(map(str, ran)) or '-'} | "
            f"{', '.join(map(str, skip)) or '-'} | {j['numCompletedTasks']} |"
        )
    lines += ["", f"**Total: {len(jobs)} jobs, {len(stages)} stages executed, {len(skipped)} skipped, {total_tasks} tasks.**", ""]

    lines += [
        "| Stage | Name | Tasks | Input | Shuffle write | Shuffle read | Run time |",
        "|---|---|---|---|---|---|---|",
    ]
    for sid in sorted(stages):
        s = stages[sid]
        lines.append(
            f"| {sid} | `{s['name']}` | {s['numCompleteTasks']} | {kb(s['inputBytes'])} | "
            f"{kb(s['shuffleWriteBytes'])} | {kb(s['shuffleReadBytes'])} | {s['executorRunTime']} ms |"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", default="http://localhost:18080")
    parser.add_argument("--out", type=Path, default=Path("docs/outputs/part4_metrics.md"))
    args = parser.parse_args()

    apps = get(args.server, "/applications")
    latest: dict[str, dict] = {}
    for app in apps:  # newest first
        latest.setdefault(app["name"], app)

    order = sorted(latest.values(), key=lambda a: a["name"])
    body = ["# Spark execution metrics (from the History Server REST API)", ""]
    body += [describe(args.server, app) for app in order]
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text("\n".join(body), encoding="utf-8")
    print(f"Wrote {args.out} ({len(order)} applications)")


if __name__ == "__main__":
    main()
