#!/usr/bin/env python3
"""The reference solution.

It reads only what /app/data publishes, recovers the LOWER and UPPER notifiable
masks the notification history leaves, prices every alert variant and every
suppressor against the scrape history, writes a complete and schema valid pair
to disk from the first candidate the walk offers before it starts searching,
then searches in ascending cost, replaying the whole notification pipeline
under each candidate, and rewrites the pair from the one that reproduces the
history. No
solver library is used and no floating point value reaches either output file.
"""

import os
import sys
import time


# ---------------------------------------------------------------------------
# THE REPLAY ENGINE
#
# Mask first. A series is folded into a per instant value row once; a variant's
# condition becomes one big integer; the `for_sec` wait becomes a doubling shift
# and AND over that integer rather than a Python step per instant. Masks are
# held PER ALERTNAME BLOCK, so a mask is targets*width bits and never
# keys*width bits.
#
# Nothing here reads a clock, nothing uses floating point, and nothing depends
# on dictionary iteration order for a value it returns.
# ---------------------------------------------------------------------------

STEP = 15                 # seconds between scrapes and between evaluations
STALENESS = 60            # a sample is fresh for this many seconds after it
RATE_WINDOW = 300         # the rate window, half open, (t - RATE_WINDOW, t]
GROUP_WAIT = 30           # delay before a group's first notification
GROUP_INTERVAL = 60       # least spacing between a group's notifications

ABSENT = None

# A binary heap written out. The check module runs under a sealed interpreter
# with the filesystem finder dropped and only json, os and stat supplied from
# the compiled in posix module, so nothing here may import a module that lives
# in a file, `heapq` included.

def _heappush(heap, item):
    heap.append(item)
    i = len(heap) - 1
    while i > 0:
        parent = (i - 1) >> 1
        if heap[parent] <= heap[i]:
            break
        heap[parent], heap[i] = heap[i], heap[parent]
        i = parent


def _heappop(heap):
    last = heap.pop()
    if not heap:
        return last
    top = heap[0]
    heap[0] = last
    i = 0
    size = len(heap)
    while True:
        left = 2 * i + 1
        right = left + 1
        small = i
        if left < size and heap[left] < heap[small]:
            small = left
        if right < size and heap[right] < heap[small]:
            small = right
        if small == i:
            break
        heap[small], heap[i] = heap[i], heap[small]
        i = small
    return top

READING = {
    "staleness": STALENESS,       # 5.3, the freshness horizon in seconds
    "missing_zero": False,        # 5.2, a missing scrape read as a zero
    "counter_reset": True,        # 5.4, a lower sample read as a reset
    "rate_window_open": True,     # 5.5, the far end of the window is open
    "rate_span_width": False,     # 5.5, span taken as the width of the window
    "rate_round_up": False,       # 5.5, the quotient rounded up
    "for_strict": False,          # 6.4, the wait compared with > not >=
    "for_carry": False,           # 6.4, a failing instant does not clear it
    "absent_exact": False,        # 6.2, absent read as no sample at exactly t
    "le_strict": False,           # 6.2, value_le read as strictly less
    "silence_closed": False,      # 7.2, the silence window closed at both ends
    "silence_absent": False,      # 7.2, an absent label read as no constraint
    "inhibit_absent": False,      # 7.4, an absent label matched as equal
    "inhibit_notifiable": False,  # 7.5, the source required to be notifiable
    "rate_endpoints": False,      # 5.5, delta taken as last minus first
    "group_by": "service",        # 8.1, the label the pipeline groups by
    "wait_every": False,          # 8.3, group_wait applied to every send
    "no_interval": False,         # 8.3, group_interval ignored
    "no_flush": False,            # 8.4, no flush at the last instant
    "member_order": "standard",   # 8.9, the order inside one notification
    "ids_per_group": False,       # 8.11, ids numbered per group
    # --- the budget and what a resolve carries ---
    # 8.5 and 8.7 are NORMATIVE CONSTANTS of the standard, not knobs. The
    # reference reads them from here and never from the generator's dial: an
    # earlier version left the default repeat at 60 while the instance was
    # drawn at 34, and the reference then replayed a different pipeline from
    # the one that produced the history and matched nothing at all.
    "budget": 3,                  # 8.5, members one notification may carry
    "budget_flush": False,        # 8.6, the flush also obeys the budget
    "repeat": 34,                 # 8.7, instants after which firing is re-said
    "no_repeat": False,           # 8.7, a still firing episode is never re-said
    "resolve_at_stop": False,     # 8.8, a resolve carries the instant it
                                  #      stopped rather than the instant said
    "queue_order": "arrival",     # 8.5, the order the queue is drained in
}


def reading(key):
    return READING[key]


# --- big integer helpers --------------------------------------------------

def bits_of(flags):
    """A row of booleans as an integer, bit i for instant i."""
    if not flags:
        return 0
    return int("".join("1" if f else "0" for f in reversed(flags)), 2)


def and_run(mask, count):
    """AND of `mask >> j` for j in 0 .. count-1, by doubling.

    Bit i of the result is set exactly when `mask` carries a one at every
    position i, i+1, ... i+count-1, which is what a continuously held condition
    is. Six or so big integer operations replace one Python step per instant.
    """
    if count <= 1:
        return mask
    result = mask
    done = 1
    while done < count:
        step = done if done * 2 <= count else count - done
        result &= result >> step
        done += step
    return result


def runs_of(mask, width):
    """Maximal runs of ones in one row, as (first, last) index pairs."""
    out = []
    if not mask:
        return out
    text = bin(mask)[2:]
    length = len(text)
    i = 0
    # walk from the low end
    position = 0
    live = mask
    while live:
        low = (live & -live).bit_length() - 1
        # find the end of the run
        run = live >> low
        stop = low
        # the run ends at the first zero above `low`
        inverted = ~run
        gap = (inverted & -inverted).bit_length() - 1
        stop = low + gap - 1
        if stop >= width:
            stop = width - 1
        out.append((low, stop))
        live &= ~(((1 << (stop - low + 1)) - 1) << low)
    return out


# --- section 5: reading the scrape history -------------------------------

def value_row(samples, ticks):
    """The fresh value of one series at every evaluation instant, or None.

    `samples` is ascending (t, value). Section 5.3: the deciding sample is the
    greatest not after `t`, and it is fresh while `t` is inside the horizon.
    """
    horizon = READING["staleness"]
    zero = READING["missing_zero"]
    exact = READING["absent_exact"]
    out = []
    j = 0
    count = len(samples)
    last_t = None
    last_v = None
    for t in ticks:
        while j < count and samples[j][0] <= t:
            last_t, last_v = samples[j]
            j += 1
        if last_t is None:
            out.append(0 if zero else None)
            continue
        if last_t != t:
            if zero:
                out.append(0)
                continue
            if exact:
                out.append(None)
                continue
        out.append(last_v if t - last_t <= horizon else None)
    return out


def rate_row(samples, ticks):
    """The rate of section 5.5 at every evaluation instant, or None."""
    open_far = READING["rate_window_open"]
    reset = READING["counter_reset"]
    endpoints = READING["rate_endpoints"]
    span_width = READING["rate_span_width"]
    round_up = READING["rate_round_up"]
    out = []
    start = 0
    stop = 0
    count = len(samples)
    for t in ticks:
        lower = t - RATE_WINDOW
        while stop < count and samples[stop][0] <= t:
            stop += 1
        if open_far:
            while start < stop and samples[start][0] <= lower:
                start += 1
        else:
            while start < stop and samples[start][0] < lower:
                start += 1
        if stop - start < 2:
            out.append(None)
            continue
        if endpoints:
            delta = samples[stop - 1][1] - samples[start][1]
        else:
            delta = 0
            for i in range(start + 1, stop):
                before = samples[i - 1][1]
                after = samples[i][1]
                if after >= before or not reset:
                    delta += after - before
                else:
                    delta += after
        span = (RATE_WINDOW if span_width
                else samples[stop - 1][0] - samples[start][0])
        if span <= 0:
            out.append(None)
            continue
        out.append(-((-delta * 1000) // span) if round_up
                   else (delta * 1000) // span)
    return out


# --- section 6: what a rule says -----------------------------------------

def condition_mask(rule, values, rates, width):
    """The instants at which one alert rule's condition holds, as an integer."""
    expr = rule["expr"]
    threshold = rule["threshold"]
    if expr == "absent":
        return bits_of([v is None for v in values])
    if expr == "rate_ge":
        return bits_of([r is not None and r >= threshold for r in rates])
    if expr == "value_ge":
        return bits_of([v is not None and v >= threshold for v in values])
    if expr == "value_le":
        if READING["le_strict"]:
            return bits_of([v is not None and v < threshold for v in values])
        return bits_of([v is not None and v <= threshold for v in values])
    raise ValueError("unknown expression %r" % (expr,))


def firing_mask(rule, condition, width):
    """The instants at which one alert rule FIRES on one target.

    Section 6.4: the condition has to have held continuously for `for_sec`. A
    run of ones starting at i and `k+1` long means the rule fires from i+k on,
    which is one shift of the folded mask.
    """
    steps = rule["for_sec"] // STEP
    if READING["for_strict"]:
        steps += 1
    if READING["for_carry"]:
        # a failing instant does not clear the wait: the condition is read as
        # having held since its first instant, so every instant at or after the
        # first one at which it held, and at which it holds, fires
        first = (condition & -condition).bit_length() - 1 if condition else 0
        keep = ((1 << width) - 1) << min(first + steps, width)
        return condition & keep & ((1 << width) - 1)
    held = and_run(condition, steps + 1)
    return (held << steps) & ((1 << width) - 1)


# --- section 7: suppression ----------------------------------------------

def label_of(alertname, service, instance, labels, name):
    if name == "alertname":
        return alertname
    if name == "service":
        return service
    if name == "instance":
        return instance
    value = labels.get((service, instance), {}).get(name)
    if value is None or value == "":
        return ABSENT
    return value


def silence_row(rule, alertname, service, instance, labels, ticks):
    """The instants at which one silence suppresses one alert instance."""
    value = label_of(alertname, service, instance, labels, rule["match_label"])
    if value is ABSENT:
        if not READING["silence_absent"]:
            return 0
    elif value != rule["match_value"]:
        return 0
    closed = READING["silence_closed"]
    start = rule["window_start"]
    end = rule["window_end"]
    return bits_of([(start <= t <= end) if closed else (start <= t < end)
                    for t in ticks])


def merge_cover(per_rule):
    """The union of what every selected suppressor covers, per alert instance."""
    covered = {}
    for rid, rows in per_rule.items():
        for key, row in rows.items():
            covered[key] = covered.get(key, 0) | row
    return covered


def inhibition_rows(rule, keys, live, labels):
    """What one inhibition covers, given the mask its source counts as live on.
    """
    out = {}
    for tkey in keys.get(rule["target_alertname"], ()):
        joined = 0
        for skey in keys.get(rule["source_alertname"], ()):
            if inhibition_matches(rule, tkey, skey, labels):
                joined |= live.get(skey, 0)
        if joined:
            out[tkey] = joined
    return out


def inhibition_matches(rule, target, source, labels):
    if target == source:
        return False
    t_alert, t_service, t_instance = target
    s_alert, s_service, s_instance = source
    for name in rule["equal_labels"]:
        left = label_of(t_alert, t_service, t_instance, labels, name)
        right = label_of(s_alert, s_service, s_instance, labels, name)
        if left is ABSENT or right is ABSENT:
            if READING["inhibit_absent"] and left is right:
                continue
            return False
        if left != right:
            return False
    return True


# --- section 8: the notification pipeline, with the budget ---------------

def group_label(key, labels):
    name = READING["group_by"]
    if name == "service":
        return key[1]
    if name == "alertname":
        return key[0]
    return label_of(key[0], key[1], key[2], labels, name)


def notification_stream(episodes, ticks, keys, labels=None):
    """Replay the pipeline and return the notifications it sends.

    EVENT DRIVEN. The only instants at which a group can do anything are the
    instants an episode opens, the instants one closes, the instant a still
    firing episode falls due to be said again, and the instant a notification
    is due. Everything between them is skipped outright, so the sweep costs
    what the history contains rather than what the window is long.

    Sections 8.5 to 8.8 are the budget and the queue. A group's outstanding work is a
    QUEUE. One notification carries at most `budget` members; the rest stay
    queued, in order, and go in the next one. Nothing is ever dropped: the
    flush at the last evaluation instant is exempt from the budget. A member is
    reported with its status AS OF THE INSTANT IT IS SENT, so an episode that
    stopped while it was queued is reported resolved carrying the instant the
    group said so, and not the instant it stopped.
    """
    budget = READING["budget"]
    repeat = READING["repeat"]
    no_repeat = READING["no_repeat"]
    per_group = {}
    for key in keys:
        per_group.setdefault(group_label(key, labels or {}), []).append(key)

    width = len(ticks)
    last_index = width - 1
    sent = []

    for group in sorted(per_group):
        opens = {}
        closes = {}
        for key in sorted(per_group[group]):
            for first, last in episodes.get(key, ()):
                opens.setdefault(first, []).append((key, first))
                if last < last_index:
                    closes.setdefault(last + 1, []).append((key, first))
        if not opens:
            continue

        live = set()
        said = {}
        due_repeat = []           # (instant, mark), a heap
        queue = []
        queued = set()
        due = None
        last_sent = None
        marks = sorted(set(list(opens) + list(closes) + [last_index]))

        def enqueue(i, mark):
            queue.append((i, mark[0][0], mark[0][2], mark[1], mark))
            queued.add(mark)

        i = marks[0]
        pos = 0
        while i <= last_index:
            t = ticks[i]
            for mark in opens.get(i, ()):
                live.add(mark)
                if mark not in queued:
                    enqueue(i, mark)
            for mark in closes.get(i, ()):
                live.discard(mark)
                if mark in said and mark not in queued:
                    enqueue(i, mark)
            while due_repeat and due_repeat[0][0] <= i:
                when, mark = _heappop(due_repeat)
                if mark in live and mark in said and mark not in queued \
                        and i - said[mark] >= repeat:
                    enqueue(i, mark)
            if queue and due is None:
                if last_sent is None or READING["wait_every"]:
                    due = t + GROUP_WAIT
                elif READING["no_interval"]:
                    due = t
                else:
                    due = max(t, ticks[last_sent] + GROUP_INTERVAL)
            final = (i == last_index) and not READING["no_flush"]
            if queue and due is not None and (t >= due or final):
                if READING["queue_order"] == "label":
                    queue.sort(key=lambda row: (row[1], row[2], row[3]))
                take = (queue if (final and not READING["budget_flush"])
                        else queue[:budget])
                rows = []
                for entry in take:
                    mark = entry[4]
                    key, first = mark
                    starts_at = ticks[first]
                    if mark in live:
                        rows.append((key, starts_at, "firing", starts_at, 0))
                        said[mark] = i
                        if not no_repeat:
                            _heappush(due_repeat, (i + repeat, mark))
                    else:
                        stop = ticks[first]
                        for a, b in episodes.get(key, ()):
                            if a == first:
                                stop = ticks[min(b + 1, last_index)]
                                break
                        ends = (stop if READING["resolve_at_stop"] else t)
                        rows.append((key, starts_at, "resolved", starts_at,
                                     ends))
                        said.pop(mark, None)
                order = READING["member_order"]
                if order == "instance_first":
                    rows.sort(key=lambda row: (row[0][2], row[0][0], row[1]))
                else:
                    rows.sort(key=lambda row: (row[0][0], row[0][2], row[1]))
                sent.append({"sent_at": t, "group_key": group,
                             "members": rows})
                for entry in take:
                    queued.discard(entry[4])
                queue = queue[len(take):]
                last_sent = i
                due = (ticks[i] + GROUP_INTERVAL) if queue else None
            # jump to the next instant at which anything can happen
            nxt = last_index + 1
            while pos < len(marks) and marks[pos] <= i:
                pos += 1
            if pos < len(marks):
                nxt = min(nxt, marks[pos])
            if due_repeat:
                nxt = min(nxt, max(i + 1, due_repeat[0][0]))
            if queue and due is not None:
                want = due
                lo, hi = i + 1, last_index
                while lo < hi:
                    mid = (lo + hi) // 2
                    if ticks[mid] < want:
                        lo = mid + 1
                    else:
                        hi = mid
                nxt = min(nxt, max(i + 1, lo))
                nxt = min(nxt, last_index)
            if nxt <= i:
                nxt = i + 1
            i = nxt

    sent.sort(key=lambda record: (record["sent_at"], record["group_key"]))
    if READING["ids_per_group"]:
        counters = {}
        for record in sent:
            counters[record["group_key"]] = counters.get(
                record["group_key"], 0) + 1
            record["notification_id"] = counters[record["group_key"]]
    else:
        for number, record in enumerate(sent, 1):
            record["notification_id"] = number
    return sent


def stream_matches(episodes, ticks, keys, labels, expected,
                   masks=None, verdicts=None, signature=None):
    """True when this candidate's stream equals `expected`, bailing early.

    `expected` is the target grouped by `group_key`, each a list of
    (sent_at, member tuple). A candidate that diverges does so at some
    notification, and there is nothing to learn from the rest of the history,
    so the sweep is abandoned at the first difference rather than built out and
    compared afterwards. The notification ids are a function of the global sort
    over (sent_at, group_key), so agreeing on every group's (sent_at, members)
    is agreeing on the ids too.
    """
    budget = READING["budget"]
    repeat = READING["repeat"]
    no_repeat = READING["no_repeat"]
    per_group = {}
    for key in keys:
        per_group.setdefault(group_label(key, labels or {}), []).append(key)
    width = len(ticks)
    last_index = width - 1
    if sorted(per_group) != sorted(expected):
        pass

    # CHEAPEST GROUP FIRST. Every group has to match, so the order they are
    # checked in cannot change the answer, only what a rejection costs. A wrong
    # candidate is rejected wherever it first diverges, and rebuilding the
    # stream of the busiest group to find that out is the dearest way to learn
    # it. Checking the quietest group first makes almost every rejection cheap.
    for group in sorted(per_group,
                        key=lambda g: (len(expected.get(g, ())), g)):
        if verdicts is not None:
            mark = signature(group)
            got = verdicts.get(mark)
            if got is False:
                return False
            if got is True:
                continue
        want = expected.get(group, ())
        seen = 0
        opens = {}
        closes = {}
        for key in sorted(per_group[group]):
            if callable(masks) and key not in episodes:
                episodes[key] = runs_of(masks(key), len(ticks))
            elif masks is not None and key not in episodes:
                # LAZY. Turning a notifiable mask into episodes is the dear
                # part of testing a candidate, and a candidate that is going to
                # diverge diverges in the first group looked at, so the runs
                # are taken one GROUP at a time rather than for every alert
                # instance up front.
                episodes[key] = runs_of(masks.get(key, 0), len(ticks))
            for first, last in episodes.get(key, ()):
                opens.setdefault(first, []).append((key, first))
                if last < last_index:
                    closes.setdefault(last + 1, []).append((key, first))
        if not opens:
            if want:
                return False
            continue
        live = set()
        said = {}
        due_repeat = []
        queue = []
        queued = set()
        due = None
        last_sent = None
        marks = sorted(set(list(opens) + list(closes) + [last_index]))
        pos = 0
        i = marks[0]
        while i <= last_index:
            t = ticks[i]
            for mark in opens.get(i, ()):
                live.add(mark)
                if mark not in queued:
                    queue.append((i, mark[0][0], mark[0][2], mark[1], mark))
                    queued.add(mark)
            for mark in closes.get(i, ()):
                live.discard(mark)
                if mark in said and mark not in queued:
                    queue.append((i, mark[0][0], mark[0][2], mark[1], mark))
                    queued.add(mark)
            while due_repeat and due_repeat[0][0] <= i:
                when, mark = _heappop(due_repeat)
                if mark in live and mark in said and mark not in queued \
                        and i - said[mark] >= repeat:
                    queue.append((i, mark[0][0], mark[0][2], mark[1], mark))
                    queued.add(mark)
            if queue and due is None:
                if last_sent is None:
                    due = t + GROUP_WAIT
                else:
                    due = max(t, ticks[last_sent] + GROUP_INTERVAL)
            final = (i == last_index)
            if queue and due is not None and (t >= due or final):
                take = queue if final else queue[:budget]
                rows = []
                for entry in take:
                    mark = entry[4]
                    key, first = mark
                    starts_at = ticks[first]
                    if mark in live:
                        rows.append((key[0], key[1], key[2], "firing",
                                     starts_at, 0))
                        said[mark] = i
                        if not no_repeat:
                            _heappush(due_repeat, (i + repeat, mark))
                    else:
                        rows.append((key[0], key[1], key[2], "resolved",
                                     starts_at, t))
                        said.pop(mark, None)
                rows.sort(key=lambda row: (row[0], row[2], row[4]))
                if seen >= len(want) or want[seen] != (t, tuple(rows)):
                    if verdicts is not None:
                        verdicts[signature(group)] = False
                    return False
                seen += 1
                for entry in take:
                    queued.discard(entry[4])
                queue = queue[len(take):]
                last_sent = i
                due = (ticks[i] + GROUP_INTERVAL) if queue else None
            nxt = last_index + 1
            while pos < len(marks) and marks[pos] <= i:
                pos += 1
            if pos < len(marks):
                nxt = min(nxt, marks[pos])
            if due_repeat:
                nxt = min(nxt, max(i + 1, due_repeat[0][0]))
            if queue and due is not None:
                lo, hi = i + 1, last_index
                while lo < hi:
                    mid = (lo + hi) // 2
                    if ticks[mid] < due:
                        lo = mid + 1
                    else:
                        hi = mid
                nxt = min(nxt, max(i + 1, min(lo, last_index)))
            if nxt <= i:
                nxt = i + 1
            i = nxt
        if seen != len(want):
            if verdicts is not None:
                verdicts[signature(group)] = False
            return False
        if verdicts is not None:
            verdicts[signature(group)] = True
    return True


def expected_by_group(stream):
    """The target stream, grouped, in the form `stream_matches` compares."""
    out = {}
    for record in stream:
        members = record["members"]
        if members and isinstance(members[0], dict):
            # read back from the published log rather than replayed here
            rows = tuple((m["alertname"], m["service"], m["instance"],
                          m["status"], m["starts_at"], m["ends_at"])
                         for m in members)
        else:
            rows = tuple((k[0], k[1], k[2], s, sa, ea)
                         for k, st, s, sa, ea in members)
        out.setdefault(record["group_key"], []).append(
            (record["sent_at"], rows))
    return out


# --- section 9: what the log pins down -----------------------------------

def bounds_from_log(log, keys, ticks):
    """The LOWER and UPPER notifiable masks the published log leaves.

    LOWER carries the instants the log PROVES were notifiable: from an
    episode's `starts_at` to the latest instant at which it was reported
    firing. UPPER carries the instants the log PERMITS: from `starts_at` up to
    but excluding the instant a resolve was said, or to the last evaluation
    instant where no resolve was ever said. Under section 8.8 a resolve carries
    the instant the group said so, so the tail between the two is not pinned.
    """
    place = {t: i for i, t in enumerate(ticks)}
    width = len(ticks)
    proof = {}
    span = {}
    closed = set()
    for record in log:
        t = record["sent_at"] if "sent_at" in record else record["sent_at"]
        for member in record["members"]:
            if isinstance(member, dict):
                key = (member["alertname"], member["service"],
                       member["instance"])
                status = member["status"]
                starts_at = member["starts_at"]
                ends_at = member["ends_at"]
            else:
                key, starts_at, status, starts_at, ends_at = member
            mark = (key, starts_at)
            if status == "firing":
                proof[mark] = max(proof.get(mark, starts_at), t)
                span[mark] = max(span.get(mark, t), t)
            else:
                closed.add(mark)
                span[mark] = max(span.get(mark, ends_at), ends_at)
    lower = {key: 0 for key in keys}
    upper = {key: 0 for key in keys}
    for (key, starts_at), until in proof.items():
        a, b = place[starts_at], place[until]
        lower[key] |= ((1 << (b - a + 1)) - 1) << a
    for mark, until in span.items():
        key, starts_at = mark
        a = place[starts_at]
        b = place[until] if mark in closed else width
        if b <= a:
            upper[key] |= 1 << a
        else:
            upper[key] |= ((1 << (b - a)) - 1) << a
        upper[key] |= 1 << a
    for key in keys:
        upper[key] |= lower[key]
    return lower, upper


import sys
import time

# import engine as E


def split_rows(text, header):
    lines = text.split("\n")
    while lines and lines[-1] == "":
        lines.pop()
    if not lines or lines[0] != header:
        raise ValueError("unexpected header: %r" % (lines[0] if lines else "",))
    return [line.split(",") for line in lines[1:]]


def timeline_of(samples):
    seen = set()
    for rows in samples.values():
        for when, _ in rows:
            seen.add(when)
    return sorted(seen)


# import readers as READ
# import engine as E


def evaluation_instants(path):
    """Pass one: the instants at which the history records a sample."""
    seen = set()
    with open(path, "r", encoding="utf-8") as handle:
        handle.readline()
        for line in handle:
            seen.add(int(line[:line.index(",")]))
    return sorted(seen)


def price_streaming(path, series_of, catalogue, targets, ticks,
                    only=None):
    """Pass two: one series at a time, into the masks and no further.

    Returns the firing mask and the condition mask of every alert variant on
    every target, and nothing else. The samples are gone by the time the
    function returns.
    """
    width = len(ticks)
    by_metric = {}
    for rule in catalogue:
        if rule["kind"] != "alert":
            continue
        # A mutant only ever needs the ANSWER's rules priced again, not the
        # whole catalogue. Pricing all ninety two took each pricing mutant 24
        # seconds and the sweep past the agent's window; pricing the answer's
        # twenty one takes a quarter of that.
        if only is not None and rule["rule_id"] not in only:
            continue
        by_metric.setdefault(rule["metric"], []).append(rule)

    firing = {}
    condition = {}
    counters = {m for m in by_metric if m.endswith("_total")}

    current = None
    rows = []

    def flush(sid, rows):
        if sid is None:
            return
        key = series_of[sid]
        metric, service, instance = key
        wanted = by_metric.get(metric)
        if not wanted:
            return
        values = value_row(rows, ticks)
        rates = rate_row(rows, ticks) if metric in counters else None
        for rule in wanted:
            probe = {"expr": rule["expr"], "threshold": rule["threshold"],
                     "for_sec": rule["for_sec"]}
            cond = condition_mask(probe, values, rates, width)
            condition[(rule["rule_id"], service, instance)] = cond
            firing[(rule["rule_id"], service, instance)] = firing_mask(
                probe, cond, width)

    with open(path, "r", encoding="utf-8") as handle:
        handle.readline()
        for line in handle:
            a = line.index(",")
            b = line.index(",", a + 1)
            sid = int(line[a + 1:b])
            if sid != current:
                flush(current, rows)
                current = sid
                rows = []
            rows.append((int(line[:a]), int(line[b + 1:-1])))
    flush(current, rows)
    return firing, condition


def read_without_samples(root):
    """Every published table except the scrape history, which is streamed."""
    def text(name):
        with open("%s/%s" % (root, name), "r", encoding="utf-8") as handle:
            return handle.read()

    series_of = {}
    for sid, metric, service, instance in split_rows(
            text("series_index.csv"), "series_id,metric,service,instance"):
        series_of[int(sid)] = (metric, service, instance)

    targets = []
    labels = {}
    for service, instance, tier, shard in split_rows(
            text("target_labels.csv"), "service,instance,tier,shard"):
        targets.append((service, instance, tier, shard))
        labels[(service, instance)] = {"tier": tier, "shard": shard}

    canonical = {}
    for instance, canon in split_rows(text("label_registry.csv"),
                                           "instance,canonical_instance"):
        canonical[instance] = canon

    catalogue = []
    for row in split_rows(text("rule_catalogue.csv"),
                               ("rule_id,kind,alertname,metric,expr,threshold,"
                                "for_sec,source_alertname,target_alertname,"
                                "equal_labels,match_label,match_value,"
                                "window_start,window_end,cost")):
        catalogue.append({
            "rule_id": int(row[0]), "kind": row[1], "alertname": row[2],
            "metric": row[3], "expr": row[4],
            "threshold": int(row[5]) if row[5] else 0,
            "for_sec": int(row[6]) if row[6] else 0,
            "source_alertname": row[7], "target_alertname": row[8],
            "equal_labels": tuple(p for p in row[9].split("|") if p),
            "match_label": row[10], "match_value": row[11],
            "window_start": int(row[12]) if row[12] else 0,
            "window_end": int(row[13]) if row[13] else 0,
            "cost": int(row[14])})

    records = {}
    for row in split_rows(text("notification_log.csv"),
                               ("notification_id,sent_at,group_key,"
                                "member_index,alertname,service,instance,"
                                "status,starts_at,ends_at")):
        number = int(row[0])
        record = records.setdefault(number, {"notification_id": number,
                                             "sent_at": int(row[1]),
                                             "group_key": row[2],
                                             "members": []})
        record["members"].append({"member_index": int(row[3]),
                                  "alertname": row[4], "service": row[5],
                                  "instance": row[6], "status": row[7],
                                  "starts_at": int(row[8]),
                                  "ends_at": int(row[9])})
    log = [records[k] for k in sorted(records)]
    for record in log:
        record["members"].sort(key=lambda m: m["member_index"])

    totals = {}
    for name, value in split_rows(text("control_totals.csv"),
                                       "total_name,value"):
        totals[name] = int(value)

    return {"series_of": series_of, "targets": targets, "labels": labels,
            "canonical": canonical, "catalogue": catalogue, "log": log,
            "totals": totals}


import itertools

# import engine as E


def masks_for(w, name, threshold, wait):
    metric, expr, _ = w.alert_shape[name]
    rule = {"expr": expr, "threshold": threshold, "for_sec": wait}
    out = {}
    for service, instance, _, _ in w.targets:
        key = (metric, service, instance)
        if key not in w.values:
            continue
        cond = condition_mask(rule, w.values[key], w.rates[key], w.width)
        out[(name, service, instance)] = firing_mask(rule, cond, w.width)
    return out


def block_of(w, name, per_key):
    """Pack one alertname's per target masks into a single block integer."""
    width = w.width
    mask = 0
    for j, (service, instance, _, _) in enumerate(w.targets):
        row = per_key.get((name, service, instance), 0)
        if row:
            mask |= row << (j * width)
    return mask


def classify(w, name, threshold, wait, lower_block, true_block):
    per_key = masks_for(w, name, threshold, wait)
    block = block_of(w, name, per_key)
    covers_lower = not (lower_block & ~block)
    covers_true = not (true_block & ~block)
    return block, per_key, covers_lower, covers_true


def grid(w, name):
    """Every threshold worth trying for one alertname: the sample values."""
    metric, expr, wait = w.alert_shape[name]
    seen = set()
    for service, instance, _, _ in w.targets:
        source = (w.rates if expr == "rate_ge" else w.values)
        row = source.get((metric, service, instance))
        if row is None:
            continue
        for v in row:
            if v is not None:
                seen.add(v)
    out = sorted(seen)
    return out


def separating_interval(w, out, lower, name):
    """The threshold interval that separates PROVEN instants from TRUE ones.

    For `value_ge`, a threshold above the least value seen at an unproven
    notifiable instant and at or below the least value seen at a proven one
    fires through every instant the log proves and stops before at least one it
    does not. That is exactly a separator, and it is read off the draw rather
    than guessed at from a fraction of the value grid, which is what failed at
    the full scale where the grid is dense.
    """
    metric, expr, wait = w.alert_shape[name]
    if expr not in ("value_ge", "value_le"):
        return None
    proven = []
    unproven = []
    for key in w.keys[name]:
        vals = w.values.get((metric, key[1], key[2]))
        if vals is None:
            continue
        lo = lower[key]
        live = out["notifiable"][key]
        while live:
            bit = live & -live
            i = bit.bit_length() - 1
            live ^= bit
            v = vals[i]
            if v is None:
                continue
            if (lo >> i) & 1:
                proven.append(v)
            else:
                unproven.append(v)
    if not proven or not unproven:
        return None
    if expr == "value_ge":
        low, high = min(unproven), min(proven)
        return (low, high) if high > low else None
    low, high = max(proven), max(unproven)
    return (low, high) if high > low else None


# from engine import _heappush, _heappop


def covers_at_most(required, pool, ceiling, cap=4000):
    """Every cover of `required` from `pool` costing at most `ceiling`."""
    if not required:
        return [(0, ())]
    order = sorted(pool, key=lambda item: (item[1], item[0]))
    found = []
    seen = set()

    def walk(left, spent, chosen):
        if spent > ceiling or len(found) >= cap:
            return
        if left == 0:
            mark = tuple(sorted(chosen))
            if mark not in seen:
                seen.add(mark)
                found.append((spent, mark))
            return
        cell = left & -left
        for rule_id, cost, mask in order:
            if rule_id in chosen or not (mask & cell):
                continue
            chosen.append(rule_id)
            walk(left & ~mask, spent + cost, chosen)
            chosen.pop()

    walk(required, 0, [])
    found.sort()
    return found


_BOUND_CACHE = {}


def least_cover(required, pool, ceiling=None, pool_id=None):
    """The exact least cost cover, or None. Branch on the least open cell.

    MEMOISED on (requirement, pool id). The A* prices a cover at every node it
    expands and the prefixes of an assignment repeat constantly, so the same
    pair is asked for again and again. The id is precomputed by `pool_key`,
    which matters: an earlier attempt keyed on the pool itself and building the
    key cost more than the solve it was meant to save.
    """
    if not required:
        return 0
    if pool_id is not None:
        key = (required, pool_id)
        if key in _BOUND_CACHE:
            return _BOUND_CACHE[key]
    order = sorted(pool, key=lambda item: (item[1], item[0]))
    best = [ceiling if ceiling is not None else (1 << 40)]
    # A BUDGET on the solve. The cover is exact when it finishes inside the
    # budget; when it does not, zero is returned, which is still an admissible
    # bound and merely a weaker one. Without this a single node at the full
    # scale, where the cover is over three hundred classes, can hold the whole
    # search up: nineteen minutes of node expansion without one candidate
    # reaching the replay.
    budget = [220000]

    def walk(left, spent):
        budget[0] -= 1
        if budget[0] <= 0:
            return
        if spent >= best[0]:
            return
        if left == 0:
            best[0] = spent
            return
        cell = left & -left
        for rule_id, cost, mask in order:
            if not (mask & cell):
                continue
            walk(left & ~mask, spent + cost)

    walk(required, 0)
    limit = ceiling if ceiling is not None else (1 << 40)
    if budget[0] <= 0 and best[0] >= limit:
        answer = 0
    else:
        answer = None if best[0] >= limit else best[0]
    if pool_id is not None and len(_BOUND_CACHE) < 2_000_000:
        _BOUND_CACHE[key] = answer
    return answer


class Problem(object):
    """Everything the search needs, in per alertname block form.

    order            the alertnames, hardest first
    options[name]    list of (rule_id, cost, block) admissible for that name,
                     with rule_id 0 meaning nothing bought
    required(name, rule_id)      the cells that variant must have covered
    silences         list of (rule_id, cost, {name: block})
    inhibits         list of (rule_id, cost, source, {(variant): {name: block}})
    """

    def __init__(self, order, options, need, silences, inhibits, offset,
                 lower_block):
        self.order = order
        self.options = options
        self.need = need
        self.silences = silences
        self.inhibits = inhibits
        self.offset = offset
        self.lower_block = lower_block
        self._pool_ids = {}
        self.cheapest = {}
        for name in order:
            self.cheapest[name] = min(c for _, c, _ in options[name])
        self.tail = [0] * (len(order) + 1)
        for i in range(len(order) - 1, -1, -1):
            self.tail[i] = self.tail[i + 1] + self.cheapest[order[i]]

    def flat(self, name, block):
        """In class space the blocks are already disjoint bit ranges."""
        return block

    def pool_key(self, chosen):
        """A small id for the pool this assignment prefix sees.

        The pool changes only with the variants chosen for the SOURCE
        alertnames of the inhibitions, so that, and nothing else, identifies
        it. Building the id is cheap; building a key out of the pool itself
        costs more than the solve it was meant to save.
        """
        mark = tuple(chosen.get(src, -1)
                     for _, _, src, _ in self.inhibits)
        got = self._pool_ids.get(mark)
        if got is None:
            got = len(self._pool_ids)
            self._pool_ids[mark] = got
        return got

    def pool_for(self, chosen, depth):
        """The suppressors on offer given the assignment so far.

        A suppressor that reaches a PROVEN instant is out. An inhibition whose
        source is not yet assigned is credited optimistically with the union
        over that source's variants, which keeps the bound admissible.
        """
        pool = []
        for rid, cost, per in self.silences:
            mask = 0
            for name, block in per.items():
                mask |= self.flat(name, block)
            if mask:
                pool.append((rid, cost, mask))
        for rid, cost, source, table in self.inhibits:
            if source in chosen:
                per = table.get(chosen[source], {})
            else:
                per = table.get("__any__", {})
            mask = 0
            for name, block in per.items():
                mask |= self.flat(name, block)
            if mask:
                pool.append((rid, cost, mask))
        return pool

    def required_for(self, chosen):
        mask = 0
        for name, rid in chosen.items():
            if rid:
                mask |= self.flat(name, self.need[(name, rid)])
        return mask


def ascending(problem, cap=200000, ceiling=None):
    """Yield (cost, chosen, cover) in ascending cost. A* over the bound."""
    order = problem.order
    start_pool = problem.pool_for({}, 0)
    heap = []
    counter = [0]

    def push(bound, depth, chosen, spent):
        counter[0] += 1
        _heappush(heap, (bound, counter[0], depth, chosen, spent, None))

    push(problem.tail[0], 0, {}, 0)
    seen_leaf = 0
    while heap:
        bound, tag, depth, chosen, spent, buffered = _heappop(heap)
        if ceiling is not None and bound > ceiling:
            return
        if depth == len(order):
            required = problem.required_for(chosen)
            pool = problem.pool_for(chosen, depth)
            if buffered is None:
                room = bound - spent + 120
                buffered = covers_at_most(required, pool, room)
            hit = [c for c in buffered if spent + c[0] == bound]
            for cost, mark in hit:
                seen_leaf += 1
                yield spent + cost, dict(chosen), mark
                if seen_leaf >= cap:
                    return
            nxt = [spent + c[0] for c in buffered if spent + c[0] > bound]
            if nxt:
                counter[0] += 1
                _heappush(heap, (min(nxt), counter[0], depth,
                                      chosen, spent, buffered))
            continue
        name = order[depth]
        for rid, cost, block in problem.options[name]:
            child = dict(chosen)
            child[name] = rid
            spent2 = spent + cost
            required = problem.required_for(child)
            pool = problem.pool_for(child, depth + 1)
            k = least_cover(required, pool,
                            pool_id=problem.pool_key(child))
            if k is None:
                continue
            push(spent2 + k + problem.tail[depth + 1], depth + 1, child,
                 spent2)


# `runs_of` comes from the engine block. In the build tree that is a
# module and this line reaches it; in the shipped programs the blocks
# are one namespace and the assembler comments the line out.
# from engine import runs_of


# The two deliverables, rendered. Sections 11 and 12 fix every column
# and section 13 the order of the rows.

def ledger_rows_of(w, by_id, ids, res, canonical):
    carried = {}
    resolved = {}
    for record in res["stream"]:
        for index, (key, start, status, starts_at, ends_at) in enumerate(
                record["members"], 1):
            carried.setdefault(key, []).append(
                (starts_at, record["notification_id"], index))
            if status == "resolved":
                resolved.setdefault(key, []).append((starts_at, ends_at))
    rows = []
    for key in w.all_keys:
        rid = res["alert_of"].get(key[0])
        if rid is None:
            continue
        mask = res["firing"].get(key, 0)
        if not mask:
            continue
        cond = res["condition"].get(key, 0)
        starts = {}
        for a, b in runs_of(cond, w.width):
            starts[(a, b)] = a
        cov = res["covered"].get(key, 0)
        notif = res["notifiable"].get(key, 0)
        for first, last in runs_of(mask, w.width):
            span = ((1 << (last - first + 1)) - 1) << first
            since = first
            for a, b in starts:
                if a <= first <= b:
                    since = a
                    break
            hidden = cov & span
            if hidden:
                bit = (hidden & -hidden).bit_length() - 1
                marks = sorted(r for r, rowset in res["per_rule"].items()
                               if (rowset.get(key, 0) >> bit) & 1)
                suppressed_by = str(marks[0]) if marks else "-"
            else:
                suppressed_by = "-"
            numbers = [(n, i) for starts_at, n, i in carried.get(key, ())
                       if w.ticks[first] <= starts_at <= w.ticks[last]]
            closed = [(s_at, e_at) for s_at, e_at in resolved.get(key, ())
                      if w.ticks[first] <= s_at <= w.ticks[last]]
            said = str(max(closed)[1]) if closed else "-"
            rows.append((key[0], key[1], canonical[key[2]], rid,
                         w.ticks[since], w.ticks[first], w.ticks[last],
                         last - first + 1, bin(notif & span).count("1"),
                         suppressed_by,
                         str(min(numbers)[0]) if numbers else "-",
                         str(max(numbers)[0]) if numbers else "-",
                         str(min(numbers)[1]) if numbers else "-",
                         said))
    rows.sort(key=lambda row: (row[0], row[2], row[5]))
    return rows


def config_rows_of(w, by_id, ids, res, ledger):
    produced = {}
    suppressed = {}
    for row in ledger:
        produced[row[3]] = produced.get(row[3], 0) + 1
        if row[9] != "-":
            suppressed[int(row[9])] = suppressed.get(int(row[9]), 0) + 1
    carried = {}
    for record in res["stream"]:
        for key, start, status, starts_at, ends_at in record["members"]:
            carried.setdefault(key[0], set()).add(record["notification_id"])
    rows = []
    for rid in sorted(ids):
        rule = by_id[rid]
        if rule["kind"] == "alert":
            name = rule["alertname"]
            rows.append((rid, "alert", name, rule["cost"],
                         produced.get(rid, 0), suppressed.get(rid, 0),
                         len(carried.get(name, ()))))
        elif rule["kind"] == "inhibit":
            rows.append((rid, "inhibit", rule["target_alertname"],
                         rule["cost"], 0, suppressed.get(rid, 0), 0))
        else:
            rows.append((rid, "silence", "-", rule["cost"], 0,
                         suppressed.get(rid, 0), 0))
    return rows


def render_ledger_of(rows):
    out = [("position,alertname,service,instance,rule_id,active_since,"
            "firing_from,firing_to,firing_samples,notified_samples,"
            "suppressed_by,first_notification,last_notification,"
            "first_member_index,resolved_at")]
    for position, row in enumerate(rows, 1):
        out.append("%d,%s,%s,%s,%d,%d,%d,%d,%d,%d,%s,%s,%s,%s,%s"
                   % ((position,) + row))
    return "\n".join(out) + "\n"


def render_config_of(rows):
    out = [("position,rule_id,kind,alertname,cost,episodes_produced,"
            "episodes_suppressed,notifications_carried")]
    for position, row in enumerate(rows, 1):
        out.append("%d,%d,%s,%s,%d,%d,%d,%d" % ((position,) + row))
    return "\n".join(out) + "\n"


import os
import sys
import time

# import catalogue as C
# import engine as E
# import readers as READ
# import search as S
# import stream as STREAM
# import render as RN

# The SHIPPED defaults, and not the generator's staging directory. The
# generator passes both in the environment; the image passes neither, so a
# default of anything but these two paths makes the reference fail on the
# first read inside the container.
DATA = os.environ.get("DYNAMO_DATA", "/app/data")
OUTPUT = os.environ.get("DYNAMO_OUTPUT", "/app/output")


def announce(text):
    sys.stdout.write("solve: %s\n" % text)
    sys.stdout.flush()


class World(object):
    pass


def world_from(data):
    """The objects the engine needs, built from the published tables only."""
    w = World()
    w.ticks = timeline_of(data["samples"])
    w.width = len(w.ticks)
    w.targets = data["targets"]
    w.labels = data["labels"]
    w.canonical = data["canonical"]
    w.metrics = []
    seen = set()
    for metric, service, instance in data["samples"]:
        if metric not in seen:
            seen.add(metric)
            w.metrics.append((metric, "", 0))
    w.alert_shape = {}
    for rule in data["catalogue"]:
        if rule["kind"] == "alert":
            w.alert_shape.setdefault(rule["alertname"],
                                     (rule["metric"], rule["expr"],
                                      rule["for_sec"]))
    w.alertnames = sorted(w.alert_shape)
    w.keys = {name: [(name, s, i) for s, i, _, _ in w.targets]
              for name in w.alertnames}
    w.all_keys = sorted(k for name in w.alertnames for k in w.keys[name])
    w.metric_targets = {}
    for ti, (service, instance, _, _) in enumerate(w.targets):
        for metric, service2, instance2 in data["samples"]:
            if service2 == service and instance2 == instance:
                w.metric_targets.setdefault(metric, set()).add(ti)
    w.values = {}
    w.rates = {}
    for key, rows in data["samples"].items():
        w.values[key] = value_row(rows, w.ticks)
        w.rates[key] = (rate_row(rows, w.ticks)
                        if key[0].endswith("_total") else None)
    return w


def main():
    started = time.time()
    announce("reading the published files under %s" % DATA)
    data = read_without_samples(DATA)
    ticks = evaluation_instants(DATA + "/scrape_history.csv")
    data["samples"] = {}
    w = world_from(data)
    w.ticks = ticks
    w.width = len(ticks)
    w.metrics = []
    seen = set()
    for metric, service, instance in data["series_of"].values():
        if metric not in seen:
            seen.add(metric)
            w.metrics.append((metric, "", 0))
    w.metric_targets = {}
    place = {(s, i): ti for ti, (s, i, _, _) in enumerate(w.targets)}
    for metric, service, instance in data["series_of"].values():
        w.metric_targets.setdefault(metric, set()).add(place[(service,
                                                              instance)])
    announce("read: %d series, %d evaluation instants, %d catalogue rows, "
             "%d notifications" % (len(data["samples"]), w.width,
                                   len(data["catalogue"]), len(data["log"])))

    by_id_early = {r["rule_id"]: r for r in data["catalogue"]}
    announce("recovering what the notification log pins down")
    lower, upper = bounds_from_log(data["log"], w.all_keys, w.ticks)
    proven = sum(bin(m).count("1") for m in lower.values())
    permits = sum(bin(m).count("1") for m in upper.values())
    announce("the log proves %d notifiable instants and permits %d, so the "
             "timeline is bounded and not pinned" % (proven, permits))

    announce("pricing every alert variant against the scrape history, "
             "one series at a time so the history is never held")
    raw_firing, raw_condition = price_streaming(
        DATA + "/scrape_history.csv", data["series_of"], data["catalogue"],
        w.targets, w.ticks)
    per_key = {}
    condition_of = {}
    for (rid, service, instance), mask in raw_firing.items():
        name = by_id_early[rid]["alertname"]
        per_key.setdefault(rid, {})[(name, service, instance)] = mask
    for (rid, service, instance), mask in raw_condition.items():
        name = by_id_early[rid]["alertname"]
        condition_of.setdefault(rid, {})[(name, service, instance)] = mask
    blocks = {}
    for rid, rows in per_key.items():
        blocks[rid] = block_of(w, by_id_early[rid]["alertname"], rows)
    announce("priced %d alert variants" % len(blocks))

    announce("pricing every silence and every inhibition")
    sil_rows = {}
    for rule in data["catalogue"]:
        if rule["kind"] != "silence":
            continue
        rows = {}
        for key in w.all_keys:
            row = silence_row(rule, key[0], key[1], key[2], w.labels,
                                w.ticks)
            if row:
                rows[key] = row
        sil_rows[rule["rule_id"]] = rows
    inh_rows = {}
    for rule in data["catalogue"]:
        if rule["kind"] != "inhibit":
            continue
        target = rule["target_alertname"]
        source = rule["source_alertname"]
        variants = [r for r in data["catalogue"]
                    if r["kind"] == "alert" and r["alertname"] == source]
        for variant in variants + [None]:
            vid = variant["rule_id"] if variant else 0
            rows = {}
            for tkey in w.keys.get(target, ()):
                joined = 0
                if variant:
                    for skey in w.keys.get(source, ()):
                        if inhibition_matches(rule, tkey, skey, w.labels):
                            joined |= per_key[variant["rule_id"]].get(skey, 0)
                if joined:
                    rows[tkey] = joined
            inh_rows[(rule["rule_id"], vid)] = rows
    announce("priced %d silences and %d inhibitions"
             % (len(sil_rows), sum(1 for r in data["catalogue"]
                                   if r["kind"] == "inhibit")))

    announce("compressing the cover universe to its signature classes")
    lower_block = {n: block_of(w, n, {k: lower[k] for k in w.keys[n]})
                   for n in w.alertnames}
    upper_block = {n: block_of(w, n, {k: upper[k] for k in w.keys[n]})
                   for n in w.alertnames}
    by_id = {r["rule_id"]: r for r in data["catalogue"]}
    by_name = {}
    for rule in data["catalogue"]:
        if rule["kind"] == "alert":
            by_name.setdefault(rule["alertname"], []).append(rule)

    want = {}
    for name in w.alertnames:
        got = 0
        for rule in by_name.get(name, ()):
            got |= blocks[rule["rule_id"]] & ~upper_block[name]
        want[name] = got

    def block_for(rid, name, per):
        rows = per.get(rid, {})
        keep = {k: v for k, v in rows.items() if k[0] == name}
        return block_of(w, name, keep) if keep else 0

    classes = {}
    for name in w.alertnames:
        live = want[name]
        if not live:
            classes[name] = []
            continue
        parts = [live]
        masks = []
        for rid in sil_rows:
            got = block_for(rid, name, sil_rows) & live
            if got:
                masks.append(got)
        for key in inh_rows:
            got = block_for(key, name, inh_rows) & live
            if got:
                masks.append(got)
        for m in masks:
            nxt = []
            for c in parts:
                a = c & m
                if a:
                    nxt.append(a)
                b = c & ~m
                if b:
                    nxt.append(b)
            parts = nxt
        classes[name] = parts
    bit_of = {}
    universe = 0
    for name in w.alertnames:
        for c in classes[name]:
            bit_of[(name, c)] = universe
            universe += 1
    announce("the cover is over %d classes" % universe)

    def to_classes(name, block):
        if not block:
            return 0
        out = 0
        for c in classes[name]:
            if c & block:
                out |= 1 << bit_of[(name, c)]
        return out

    options = {}
    need = {}
    for name in w.alertnames:
        picks = []
        for rule in by_name.get(name, ()):
            rid = rule["rule_id"]
            if lower_block[name] & ~blocks[rid]:
                continue
            picks.append((rid, rule["cost"], blocks[rid]))
            need[(name, rid)] = to_classes(name,
                                           blocks[rid] & ~upper_block[name])
        if not lower_block[name]:
            picks.append((0, 0, 0))
            need[(name, 0)] = 0
        options[name] = picks

    silence_rows = []
    for rule in data["catalogue"]:
        if rule["kind"] != "silence":
            continue
        rid = rule["rule_id"]
        per = {}
        bad = False
        for name in w.alertnames:
            raw = block_for(rid, name, sil_rows)
            if raw & lower_block[name]:
                bad = True
                break
            got = to_classes(name, raw)
            if got:
                per[name] = got
        if not bad and per:
            silence_rows.append((rid, rule["cost"], per))

    inhibit_rows = []
    for rule in data["catalogue"]:
        if rule["kind"] != "inhibit":
            continue
        rid = rule["rule_id"]
        source = rule["source_alertname"]
        table = {}
        union = {}
        for variant in by_name.get(source, ()):
            vid = variant["rule_id"]
            per = {}
            bad = False
            for name in w.alertnames:
                raw = block_for((rid, vid), name, inh_rows)
                if raw & lower_block[name]:
                    bad = True
                    break
                got = to_classes(name, raw)
                if got:
                    per[name] = got
            if bad:
                per = {}
            table[vid] = per
            for n, b in per.items():
                union[n] = union.get(n, 0) | b
        table[0] = {}
        table["__any__"] = union
        if union:
            inhibit_rows.append((rid, rule["cost"], source, table))

    order = sorted(w.alertnames,
                   key=lambda n: (len(options[n]),
                                  -bin(lower_block[n]).count("1")))
    problem = Problem(order, options, need, silence_rows, inhibit_rows,
                        {n: 0 for n in w.alertnames}, lower_block)
    space = 1
    for name in order:
        space *= len(options[name])
    announce("%d admissible assignments to walk in ascending cost" % space)

    announce("searching, replaying every candidate against the history")
    grouped = expected_by_group(data["log"])
    touches = {}
    for rid, rows in sil_rows.items():
        touches[rid] = {key[1] for key in rows}
    for (rid, vid), rows in inh_rows.items():
        touches.setdefault(rid, set()).update(key[1] for key in rows)
    verdicts = {}
    tried = 0
    answer = None
    relaxed = None
    def emit_pair(ids, note):
        """Render both deliverables from `ids` and put them on disk.

        Section 11.4 asks for a complete, schema valid pair to be on disk
        before the search begins and to be rewritten in place as the answer
        improves, so that whatever moment the clock stops on, the disk holds
        a whole answer rather than a partial one. This is called once on the
        first candidate the walk offers and again on the candidate that
        reproduces the history, so the reference obeys the clause it states.
        """
        alert_of = {}
        firing = {}
        condition = {}
        for rid in ids:
            rule = by_id[rid]
            if rule["kind"] != "alert":
                continue
            name = rule["alertname"]
            alert_of[name] = rid
            metric, expr, _ = w.alert_shape[name]
            probe = {"expr": expr, "threshold": rule["threshold"],
                     "for_sec": rule["for_sec"]}
            condition.update(condition_of.get(rid, {}))
            firing.update(per_key.get(rid, {}))
        per_rule = {}
        for rid in ids:
            rule = by_id[rid]
            if rule["kind"] == "silence":
                per_rule[rid] = sil_rows.get(rid, {})
            elif rule["kind"] == "inhibit":
                per_rule[rid] = inh_rows.get(
                    (rid, alert_of.get(rule["source_alertname"], 0)), {})
        covered = {}
        for rid, rows in per_rule.items():
            for key, row in rows.items():
                covered[key] = covered.get(key, 0) | row
        notifiable = {}
        for key in w.all_keys:
            got = firing.get(key, 0)
            notifiable[key] = got & ~covered.get(key, 0) if got else 0
        episodes = {k: runs_of(m, w.width) for k, m in notifiable.items() if m}
        stream = notification_stream(episodes, w.ticks, w.all_keys, w.labels)
        res = {"firing": firing, "condition": condition, "covered": covered,
               "notifiable": notifiable, "stream": stream, "per_rule": per_rule,
               "alert_of": alert_of}

        ledger = ledger_rows_of(w, by_id, ids, res, w.canonical)
        conf = config_rows_of(w, by_id, ids, res, ledger)
        os.makedirs(OUTPUT, exist_ok=True)
        with open(OUTPUT + "/alert_ledger.csv", "w", encoding="utf-8") as handle:
            handle.write(render_ledger_of(ledger))
        with open(OUTPUT + "/alerting_config.csv", "w", encoding="utf-8") as handle:
            handle.write(render_config_of(conf))
        announce("%s: %d ledger rows and %d configuration rows into %s"
                 % (note, len(ledger), len(conf), OUTPUT))

    for cost, chosen, mark in ascending(problem, cap=400000):
        if relaxed is None:
            relaxed = cost
        ids = tuple(sorted([r for r in chosen.values() if r] + list(mark)))
        tried += 1
        if tried == 1:
            emit_pair(ids, "wrote the first candidate before searching")
        firing = {}
        alert_of = {}
        selected = []
        for rid in ids:
            rule = by_id[rid]
            if rule["kind"] == "alert":
                firing.update(per_key[rid])
                alert_of[rule["alertname"]] = rid
        for rid in ids:
            rule = by_id[rid]
            if rule["kind"] == "silence":
                rows = sil_rows.get(rid)
                if rows:
                    selected.append(rows)
            elif rule["kind"] == "inhibit":
                rows = inh_rows.get((rid,
                                     alert_of.get(rule["source_alertname"],
                                                  0)))
                if rows:
                    selected.append(rows)

        def mask_for(key, _f=firing, _s=selected):
            got = _f.get(key, 0)
            if not got:
                return 0
            hidden = 0
            for rows in _s:
                row = rows.get(key)
                if row:
                    hidden |= row
            return got & ~hidden

        alerts = tuple(sorted(alert_of.values()))
        supps = [r for r in ids if by_id[r]["kind"] != "alert"]

        def sig(group, _a=alerts, _s=supps):
            return (_a, tuple(r for r in _s if group in touches.get(r, ())))

        if stream_matches({}, w.ticks, w.all_keys, w.labels, grouped,
                            masks=mask_for, verdicts=verdicts, signature=sig):
            answer = (cost, ids)
            break
        if tried % 500 == 0:
            announce("   %d candidates replayed, at cost %d, %.0f s"
                     % (tried, cost, time.time() - started))
    if answer is None:
        announce("no configuration reproduces the history, which cannot be")
        return 1
    announce("search done: cost %d over %d rules, %d candidates replayed, "
             "the relaxed bound was %d"
             % (answer[0], len(answer[1]), tried, relaxed))
    announce("finished in %.0f s" % (time.time() - started))

    announce("rendering the two deliverables")
    emit_pair(answer[1], "wrote the answer")
    import resource
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
    announce("peak %.0f megabytes" % peak)
    announce("finished in %.0f s" % (time.time() - started))
    return 0


if __name__ == "__main__":
    sys.exit(main())
