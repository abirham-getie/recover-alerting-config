#!/bin/sh
# The sealed runner.
#
# The graded bytes are read by the shell BEFORE any program starts and again
# after the sweep, so a submission rewritten while the checks are running is
# caught by the shell rather than trusted by the checks. The reward channel is
# established and read back rather than assumed. The checks themselves run
# under an interpreter with the import path emptied and the filesystem finder
# dropped, with `os` and `stat` built here out of the compiled in `posix`
# module, so nothing on the graded path imports a module that lives in a file
# and no test framework is involved at all.

set -u

DATA="${TASK_DATA:-/app/data}"
OUTPUT="${TASK_OUTPUT:-/app/output}"
CHECKS="${TASK_CHECKS:-/tests/test_outputs.py}"

# THE REWARD CHANNEL. The harness reads /logs/verifier/reward.txt back out of
# the container, so that is where the reward is written and nowhere else. An
# earlier version of this file defaulted to /tmp/reward.txt, which no harness
# reads: every trial then reported a missing reward file rather than a score.
REWARD_ROOT="${TASK_REWARD_ROOT:-/logs}"
REWARD_DIR="${TASK_REWARD_DIR:-$REWARD_ROOT/verifier}"
REWARD="${TASK_REWARD:-$REWARD_DIR/reward.txt}"

# Resolve the tools this file runs, rather than letting a lookup path the agent
# controls resolve them. The system directories come first, so an interpreter
# dropped into /usr/local/bin cannot stand in for the real one, and the
# trailing entry keeps an image whose interpreter lives only there working.
PATH=/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/bin
export PATH

PYTHON=/usr/bin/python3
[ -x "$PYTHON" ] || PYTHON=python3

trap '' PIPE

progress() {
    printf 'verifier: %s\n' "$*"
}

# --- the reward channel is established, not assumed ------------------------
for d in "$REWARD_ROOT" "$REWARD_DIR"; do
    if [ -L "$d" ] || { [ -e "$d" ] && [ ! -d "$d" ]; }; then
        rm -f -- "$d" 2>/dev/null
        mv -f -- "$d" "$d.tampered.$$" 2>/dev/null
    fi
done
mkdir -p "$REWARD_DIR" 2>/dev/null
if [ ! -d "$REWARD_DIR" ] || [ -L "$REWARD_DIR" ]; then
    echo "REFUSING: no reward directory at $REWARD_DIR" >&2
    exit 0
fi

# The leaf is REMADE rather than written through, so a symbolic link planted in
# its place is removed instead of followed, and the value is read back.
write_reward() {
    if [ -e "$REWARD" ] || [ -L "$REWARD" ]; then
        rm -rf -- "$REWARD" 2>/dev/null
    fi
    if [ -e "$REWARD" ] || [ -L "$REWARD" ]; then
        return 1
    fi
    printf '%s' "$1" > "$REWARD" 2>/dev/null || return 1
    [ -f "$REWARD" ] || return 1
    [ -L "$REWARD" ] && return 1
    [ "$(cat "$REWARD" 2>/dev/null)" = "$1" ]
}

if ! write_reward 0; then
    echo "REFUSING: the reward channel does not read back" >&2
    exit 0
fi
progress "reward channel established at $REWARD"

# --- a framework entry point planted at / or /tests refuses outright -------
for d in / /tests; do
  for f in conftest.py pytest.ini pyproject.toml setup.cfg tox.ini; do
    # "$d$f" would read /testsconftest.py for the second root, which is how a
    # planted conftest.py at /tests survived the battery once.
    if [ -e "${d%/}/$f" ]; then
      echo "REFUSING: ${d%/}/$f is present" >&2
      write_reward 0
      exit 0
    fi
  done
done

# --- the graded bytes, before anything runs -------------------------------
BEFORE_LEDGER=$(cksum < "$OUTPUT/alert_ledger.csv" 2>/dev/null || echo none)
BEFORE_CONFIG=$(cksum < "$OUTPUT/alerting_config.csv" 2>/dev/null || echo none)
progress "both deliverables read and held"

BOOTSTRAP=$(cat <<'PYEOF'
import sys
import posix

# every finder that reaches the filesystem goes
sys.meta_path = [f for f in sys.meta_path
                 if getattr(f, "__name__", "") != "PathFinder"]
sys.path = []
sys.path_hooks = []
sys.path_importer_cache.clear()


class _Module(object):
    def __init__(self, name):
        self.__name__ = name


# stat, from the numbers and nothing else
_stat = _Module("stat")
_stat.S_IFMT = lambda mode: mode & 0o170000
_stat.S_ISREG = lambda mode: (mode & 0o170000) == 0o100000
_stat.S_ISDIR = lambda mode: (mode & 0o170000) == 0o040000
_stat.S_ISLNK = lambda mode: (mode & 0o170000) == 0o120000
sys.modules["stat"] = _stat

# os, built on posix
_os = _Module("os")
for _name in ("open", "close", "read", "write", "fstat", "listdir", "getcwd",
              "environ", "O_RDONLY", "O_WRONLY", "O_CREAT", "O_TRUNC",
              "O_DIRECTORY", "O_NOFOLLOW"):
    setattr(_os, _name, getattr(posix, _name))
_os.sep = "/"


class _Path(object):
    sep = "/"

    @staticmethod
    def abspath(path):
        if not path.startswith("/"):
            path = posix.getcwd() + "/" + path
        parts = []
        for leaf in path.split("/"):
            if leaf in ("", "."):
                continue
            if leaf == "..":
                if parts:
                    parts.pop()
                continue
            parts.append(leaf)
        return "/" + "/".join(parts)

    @staticmethod
    def join(*bits):
        return "/".join(b.rstrip("/") for b in bits if b)


_os.path = _Path
sys.modules["os"] = _os
sys.modules["posixpath"] = _Path

_checks = None
for _arg in sys.argv[1:]:
    _checks = _arg
_fd = posix.open(_checks, posix.O_RDONLY)
_chunks = []
while True:
    _got = posix.read(_fd, 1 << 20)
    if not _got:
        break
    _chunks.append(_got)
posix.close(_fd)
_source = b"".join(_chunks).decode("utf-8")

_space = {"__name__": "checks", "__builtins__": __builtins__}
exec(compile(_source, "checks", "exec"), _space)

_rows = _space["run_checks"](posix.environ[b"TASK_DATA"].decode(),
                             posix.environ[b"TASK_OUTPUT"].decode())
_bad = 0
for _name, _ok, _detail in _rows:
    sys.stdout.write("  %-6s %-46s %s\n"
                     % ("PASS" if _ok else "FAIL", _name, _detail))
    sys.stdout.flush()
    if not _ok:
        _bad += 1
sys.stdout.write("%d checks, %d failed\n" % (len(_rows), _bad))
sys.exit(1 if _bad else 0)
PYEOF
)

progress "running the checks under a sealed interpreter"
TASK_DATA="$DATA" TASK_OUTPUT="$OUTPUT" \
  "$PYTHON" -I -S -B -c "$BOOTSTRAP" "$CHECKS"
CODE=$?

# --- the graded bytes, after the sweep ------------------------------------
AFTER_LEDGER=$(cksum < "$OUTPUT/alert_ledger.csv" 2>/dev/null || echo none)
AFTER_CONFIG=$(cksum < "$OUTPUT/alerting_config.csv" 2>/dev/null || echo none)

if [ "$BEFORE_LEDGER" != "$AFTER_LEDGER" ] || \
   [ "$BEFORE_CONFIG" != "$AFTER_CONFIG" ]; then
  echo "REFUSING: a graded file changed while the checks were running" >&2
  write_reward 0
  exit 0
fi

if [ "$CODE" -eq 0 ]; then
  write_reward 1
else
  write_reward 0
fi
progress "finished, reward $(cat "$REWARD" 2>/dev/null)"
exit 0
