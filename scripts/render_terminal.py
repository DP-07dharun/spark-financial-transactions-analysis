"""Render captured console logs as terminal-style HTML pages (used to screenshot execution evidence).

    python scripts/render_terminal.py <out_dir>

Each page shows a real log file from docs/outputs/ (optionally a line range) in a terminal frame.
"""

from __future__ import annotations

import html
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "outputs"

# (page name, title bar, log file, first line, last line, command shown at the top)
PAGES = [
    ("part1_hdfs", "root@hadoop-master: ~/bigdata-project", "part1_hdfs.txt", 0, 26,
     "bash part1_hdfs.sh"),
    ("part1_hdfs_head", "root@hadoop-master: ~/bigdata-project", "part1_hdfs.txt", 64, 999,
     None),
    ("part2_shell_a", "root@hadoop-master: ~/bigdata-project  —  spark-shell", "part2_spark_shell.txt", 0, 57,
     "spark-shell --master yarn --num-executors 2 -i analysis.scala"),
    ("part2_shell_b", "root@hadoop-master: ~/bigdata-project  —  spark-shell", "part2_spark_shell.txt", 57, 999,
     None),
    ("part3_batch_a", "root@hadoop-master: ~/bigdata-project", "part3_batch_jobs.txt", 0, 53,
     "bash run-batch-jobs.sh"),
    ("part3_batch_b", "root@hadoop-master: ~/bigdata-project", "part3_batch_jobs.txt", 53, 999, None),
    ("part3_job_log", "root@hadoop-master: ~/bigdata-project  —  spark-submit log", "part3_batch_jobs_full_log.txt",
     0, 60, None),
]

CSS = """
body{margin:0;background:#1f2430;font:14px/1.45 'Cascadia Mono',Consolas,'DejaVu Sans Mono',monospace}
.win{margin:24px;border-radius:10px;overflow:hidden;box-shadow:0 10px 30px #0008;background:#0d1117}
.bar{background:#2d333b;color:#c9d1d9;padding:8px 14px;font:13px 'Segoe UI',sans-serif;display:flex;gap:8px;align-items:center}
.dot{width:12px;height:12px;border-radius:50%;display:inline-block}
pre{margin:0;padding:14px 18px;color:#d1d7e0;white-space:pre-wrap;word-break:break-all}
.cmd{color:#7ee787}.hdr{color:#79c0ff;font-weight:bold}.info{color:#8b949e}.ok{color:#56d364}.warn{color:#e3b341}
"""


def colour(line: str) -> str:
    esc = html.escape(line)
    if line.startswith("$ ") or line.startswith("# spark-submit"):
        return f'<span class="cmd">{esc}</span>'
    if line.startswith("===") or line.startswith("####"):
        return f'<span class="hdr">{esc}</span>'
    if " INFO " in line:
        return f'<span class="info">{esc}</span>'
    if " WARN " in line:
        return f'<span class="warn">{esc}</span>'
    if "HEALTHY" in line or "exit 0" in line or line.startswith("Saved to") or line.startswith("["):
        return f'<span class="ok">{esc}</span>'
    return esc


def main() -> None:
    out_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "build" / "terminal"
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, title, log, start, end, command in PAGES:
        lines = (OUT / log).read_text(encoding="utf-8-sig").splitlines()[start:end]
        body = "\n".join(colour(l) for l in lines)
        if command:
            body = f'<span class="cmd">root@hadoop-master:~/bigdata-project# {html.escape(command)}</span>\n' + body
        page = (
            f"<!doctype html><meta charset=utf-8><title>{name}</title><style>{CSS}</style>"
            f'<div class=win><div class=bar><span class=dot style="background:#ff5f56"></span>'
            f'<span class=dot style="background:#ffbd2e"></span><span class=dot style="background:#27c93f"></span>'
            f"&nbsp;{html.escape(title)}</div><pre>{body}</pre></div>"
        )
        (out_dir / f"{name}.html").write_text(page, encoding="utf-8")
        print(out_dir / f"{name}.html", len(lines), "lines")


if __name__ == "__main__":
    main()
