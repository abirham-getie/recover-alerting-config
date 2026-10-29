#!/bin/bash
# Harbor mounts this directory at /solution and runs this file. Everything the
# reference needs is under /app/data; it writes both deliverables to
# /app/output and prints a milestone per stage, unbuffered, as it goes.
set -euo pipefail

mkdir -p /app/output
exec python3 -u /solution/solve.py
