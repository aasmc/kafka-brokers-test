#!/usr/bin/env bash
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
[[ -f "$ROOT/logs/app.pid" ]] && kill "$(cat "$ROOT/logs/app.pid")" 2>/dev/null && rm -f "$ROOT/logs/app.pid" && echo "app stopped"
