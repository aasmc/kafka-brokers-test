#!/usr/bin/env python3
"""Builds the experiment summary from logs/app.log, logs/load/* and logs/experiment/events.log."""
import re
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "logs"


def ts(s):
    return datetime.fromisoformat(s[:19])


events = []
for line in (LOGS / "experiment/events.log").read_text().splitlines():
    t, rest = line.split(" ", 1)
    events.append((ts(t), rest))
start = next(t for t, e in events if e.startswith("LOAD_STARTED"))
down = next(t for t, e in events if e.startswith("BROKER_DOWN"))
up = next(t for t, e in events if e.startswith("BROKER_UP"))
stop = next(t for t, e in events if e.startswith("LOAD_STOPPED"))
phases = [("baseline", start, down), ("broker down", down, up), ("recovery", up, stop + timedelta(seconds=5))]


def phase_of(t):
    for name, a, b in phases:
        if a <= t < b:
            return name
    return None


# --- app log
consumed = defaultdict(list)          # topic -> [(time, seq)]
produced, produce_failed = [], []     # [(time, tookMs, error)]
errors_by_kind = defaultdict(int)
line_re = re.compile(r"^(\S+)\s+(\w+)\s+\d+ --- \[[^\]]*\] \[([^\]]*)\]\s+(\S+)\s*: (.*)$")
for line in (LOGS / "app.log").read_text(errors="replace").splitlines():
    m = line_re.match(line)
    if not m:
        continue
    t, level, thread, logger, msg = ts(m.group(1)), m.group(2), m.group(3), m.group(4), m.group(5)
    if not (start <= t <= stop + timedelta(seconds=10)):
        continue
    if msg.startswith("CONSUMED"):
        topic = re.search(r"topic=(\S+)", msg).group(1)
        seq = re.search(r"seq=(\d+)", msg)
        consumed[topic].append((t, int(seq.group(1)) if seq else None))
    elif msg.startswith("PRODUCED"):
        produced.append((t, int(re.search(r"tookMs=(\d+)", msg).group(1))))
    elif msg.startswith("PRODUCE_FAILED"):
        produce_failed.append((t, re.search(r"error=(.*)", msg).group(1)))
    if level in ("WARN", "ERROR"):
        kind = re.sub(r"\d+", "N", msg)[:150]
        errors_by_kind[(level, logger.split(".")[-1], kind)] += 1

out = []
p = out.append
p("=" * 100)
p("EXPERIMENT SUMMARY")
for t, e in events:
    p(f"  {t:%H:%M:%S}  {e}")
p("")

# --- consumers
for topic in ("topic_consumer", "topic_consumer_2"):
    sent = []
    sent_file = LOGS / f"load/sent-{topic}.log"
    if sent_file.exists():
        for line in sent_file.read_text().splitlines():
            m = re.search(r"seq=(\d+) sent_at=(\S+)", line)
            sent.append((ts(m.group(2)), int(m.group(1))))
    recs = consumed[topic]
    seqs = [s for _, s in recs if s is not None]
    sent_seqs = {s for _, s in sent}
    missing = sorted(sent_seqs - set(seqs))
    dups = len(seqs) - len(set(seqs))
    p(f"CONSUMER {topic}: sent={len(sent)} consumed={len(recs)} unique={len(set(seqs))} "
      f"missing={len(missing)} duplicates={dups}")
    if missing:
        p(f"  missing seqs (first 20): {missing[:20]}")
    for name, a, b in phases:
        in_phase = [t for t, _ in recs if a <= t < b]
        gaps = [(in_phase[i] - in_phase[i - 1]).total_seconds() for i in range(1, len(in_phase))]
        p(f"  {name:12s} consumed={len(in_phase):4d}  max gap between records={max(gaps, default=0):6.1f}s")
    # longest consumption pause and end-to-end lag (sent_at -> consumed)
    sent_at = {s: t for t, s in sent}
    lags = [(t - sent_at[s]).total_seconds() for t, s in recs if s in sent_at]
    times = [t for t, _ in recs]
    worst = max(((times[i] - times[i - 1]).total_seconds(), times[i - 1], times[i])
                for i in range(1, len(times))) if len(times) > 1 else (0, None, None)
    if worst[1]:
        p(f"  longest pause: {worst[0]:.1f}s ({worst[1]:%H:%M:%S} -> {worst[2]:%H:%M:%S}); "
          f"max end-to-end lag={max(lags, default=0):.1f}s")
    p("")

# --- producer via HTTP
http = []
for line in (LOGS / "load/http.log").read_text().splitlines():
    m = re.match(r"(\S+) seq=(\d+) http=(\d+) ([\d.]+)", line)
    if m:
        http.append((ts(m.group(1)), m.group(3), float(m.group(4))))
p(f"PRODUCER topic_producer (GET /api/v1/test): requests={len(http)} "
  f"PRODUCED={len(produced)} PRODUCE_FAILED={len(produce_failed)}")
for name, a, b in phases:
    rows = [r for r in http if a <= r[0] < b]
    codes = defaultdict(int)
    for _, c, _ in rows:
        codes[c] += 1
    p(f"  {name:12s} requests={len(rows):4d} codes={dict(codes)} "
      f"max latency={max((r[2] for r in rows), default=0):.2f}s")
bad = [r for r in http if r[1] != "200"]
if bad:
    p(f"  non-200 window: {bad[0][0]:%H:%M:%S} -> {bad[-1][0]:%H:%M:%S} ({len(bad)} requests)")
fail_kinds = defaultdict(int)
for _, e in produce_failed:
    fail_kinds[e[:160]] += 1
for k, v in fail_kinds.items():
    p(f"  failure x{v}: {k}")
p("")

p("TOP WARN/ERROR messages in app.log during the experiment:")
for (level, logger, kind), n in sorted(errors_by_kind.items(), key=lambda x: -x[1])[:25]:
    p(f"  {n:5d}  {level:5s} {logger}: {kind}")
print("\n".join(out))
