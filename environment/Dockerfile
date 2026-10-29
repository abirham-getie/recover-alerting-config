# The single container image, built for both the agent run and the grading run.
# It carries the scrape history, the notification history, the rule catalogue,
# the label tables, the control totals and the standard that reads them, plus a
# stock Python. Nothing else is needed to read a line of CSV.
#
# NOTHING IS INSTALLED FROM THE NETWORK BEYOND THE INTERPRETER, AND NOTHING IS
# INSTALLED AT VERIFY TIME. The verifier is a sealed shell runner, overlaid at
# verify time and never present in this image, that drives `/usr/bin/python3`
# with an emptied import path: it refuses a framework entry point outright, it
# reads its check module through its own guarded opener, and the json, os and
# stat modules the checks use are supplied by the runner itself from builtins.
# No check and no part of the reference imports a third party framework, so no
# test framework and no package manager is fetched here. What the verifier
# needs is baked, and it is `python3`.
#
# The published files are 95.5 megabytes in total and are committed in this
# repository, so the build is a copy rather than a draw.
FROM public.ecr.aws/docker/library/ubuntu:24.04@sha256:0d39fcc8335d6d74d5502f6df2d30119ff4790ebbb60b364818d5112d9e3e932

RUN apt-get update && apt-get install -y --no-install-recommends python3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY data /app/data
RUN mkdir -p /app/output

# Integrity guard on what the publication shipped. Each entry is the exact byte
# count the emission wrote; the image refuses to build if one has drifted.
# GUARD BLOCK BEGIN

RUN set -eu; \
    for entry in \
        "ALERTING_STANDARD.md:18174" \
        "control_totals.csv:108" \
        "label_registry.csv:1708" \
        "notification_log.csv:4118856" \
        "rule_catalogue.csv:14024" \
        "scrape_history.csv:91364174" \
        "series_index.csv:8146" \
        "target_labels.csv:1618" \
    ; do \
        leaf="${entry%%:*}"; want="${entry##*:}"; \
        have="$(wc -c < "/app/data/$leaf" | tr -d ' ')"; \
        [ "$have" = "$want" ] || { echo "guard: $leaf is $have bytes, want $want" >&2; exit 1; }; \
    done
RUN test "$(ls -1 /app/data | wc -l | tr -d " ")" = "8"
# GUARD BLOCK END
