# ALERTING STANDARD

This document is normative. Every quantity in `/app/output/alerting_config.csv`
and `/app/output/alert_ledger.csv` is defined here. Where this document and a
published table both speak, they agree. Nothing outside this document decides
anything.

## 1. What survives and what does not

The monitoring stack that produced the incident history scraped a set of
targets, evaluated a set of alerting rules over the samples, and sent the
notifications the history records. The scrape history and the notification
history survive. The alerting rules do not. `/app/data/rule_catalogue.csv`
lists every rule the stack could have been configured with, each with a cost.
You recover the least cost configuration that reproduces the notification
history exactly.

## 2. The published files

```
/app/data/series_index.csv       series_id,metric,service,instance
/app/data/scrape_history.csv     sample_at,series_id,value
/app/data/notification_log.csv   notification_id,sent_at,group_key,
                                 member_index,alertname,service,instance,
                                 status,starts_at,ends_at
/app/data/rule_catalogue.csv     rule_id,kind,alertname,metric,expr,threshold,
                                 for_sec,source_alertname,target_alertname,
                                 equal_labels,match_label,match_value,
                                 window_start,window_end,cost
/app/data/target_labels.csv      service,instance,tier,shard
/app/data/label_registry.csv     instance,canonical_instance
/app/data/control_totals.csv     total_name,value
```

Every table is UTF-8, line feed terminated, with no quoting, no padding and no
space beside any comma. Every timestamp is an integer number of seconds since
the epoch. No published file carries a fractional value. The rows of
`/app/data/scrape_history.csv` are ordered by `series_id` ascending and then by
`sample_at` ascending.

## 3. Time

3.1 The scrape interval is 15 seconds. Every sample the history records was
taken at an instant of the form `first_instant + 15k`.

3.2 The EVALUATION INSTANTS are exactly the instants at which the history
records at least one sample, ascending. There are no evaluation instants before
the first or after the last.

3.3 The rules are evaluated at every evaluation instant, in ascending order,
and at no other time.

## 4. Targets and labels

4.1 A TARGET is one row of `/app/data/target_labels.csv`. It carries a
`service` label and an `instance` label with the values in that row, and it
carries a `tier` label and a `shard` label with the values in that row.

4.2 An empty cell in `/app/data/target_labels.csv` means the label is ABSENT.
An absent label is not a label whose value is the empty string: no matcher and
no equality test ever matches it.

4.3 An ALERT INSTANCE is a pair of an alertname and a target. It carries the
labels `alertname`, `service` and `instance`, and the `tier` and `shard` of its
target.

4.4 `/app/data/label_registry.csv` relates each target's `instance` label to
one other name. Every matcher, every equality test and every published total
uses the `instance` label as it stands.

4.5 A metric is not exported by every target. A series exists only where
`/app/data/series_index.csv` records one, and an alert instance whose series
does not exist is treated by section 6.2 as a series that is ABSENT at every
evaluation instant.

## 5. Reading the scrape history

5.1 A SERIES is one row of `/app/data/series_index.csv`: a metric on a target.
Its samples are the rows of `/app/data/scrape_history.csv` carrying its
`series_id`, in ascending `sample_at`.

5.2 A scrape that produced no sample left no row. A missing sample is NOT a
zero, and it is not an interpolation between its neighbours.

5.3 FRESHNESS. The sample that decides an evaluation of a series at instant `t`
is the one with the greatest `sample_at` not after `t`. That sample is FRESH at
`t` when `t - sample_at` is at most 60 seconds. If there is no such sample, or
the greatest one is older than that, the series is ABSENT at `t`. A run of four
or fewer missing scrapes therefore leaves the series present, carrying the last
value forward; a run of five or more takes it past the horizon.

5.4 COUNTER RESETS. A counter series is one whose metric name ends in `_total`.
A sample lower than the sample before it is a reset, and the increase it
records is its own value, because the counter began again from zero and reached
that value.

5.5 THE RATE. For a counter series and an evaluation instant `t`, take the
samples with `sample_at` in the HALF OPEN window `(t - 300, t]`, ascending.
Fewer than two samples leaves the rate UNDEFINED. Otherwise let `delta` be the
sum, over consecutive pairs in the window, of the later value minus the earlier
value where the later is not lower, and of the later value alone where it is
lower. Let `span` be the `sample_at` of the last sample in the window minus the
`sample_at` of the first. The rate is

```
rate = floor(delta * 1000 / span)
```

which is the increase per 1000 seconds as an integer. `span` is the distance
between the first and the last sample present, never the width of the window.

## 6. Alert rules

6.1 A catalogue row with `kind` of `alert` is an ALERT RULE. It names an
`alertname`, a `metric`, an `expr`, a `threshold` and a `for_sec`. It is
evaluated on every target.

6.2 THE CONDITION of an alert rule on one target at instant `t`:

```
value_ge   the series is present at t and its fresh value is at least threshold
value_le   the series is present at t and its fresh value is at most threshold
rate_ge    the rate of section 5.5 is defined at t and is at least threshold
absent     the series is ABSENT at t
```

Any other reading of a series that is absent leaves the condition FALSE.

6.3 The ACTIVE SINCE of an alert instance at `t` is the earliest instant `s`
such that the condition held at every evaluation instant from `s` to `t`
inclusive. If the condition does not hold at `t` there is no active since.

6.4 FIRING. The alert instance is FIRING at `t` when the condition holds at `t`
and `t - active_since` is at least `for_sec`. One evaluation instant at which
the condition does not hold clears the active since outright, and the wait
starts again from nothing the next time the condition holds. Nothing else
clears it: the state of an alert instance at an instant is a function of the
whole history of that series before it, and never of the sample at that instant
alone.

6.5 A configuration carries AT MOST ONE alert rule for any one alertname. An
alertname with no alert rule never fires.

## 7. Suppression

7.1 A catalogue row with `kind` of `silence` is a SILENCE. It names a
`match_label`, a `match_value`, a `window_start` and a `window_end`.

7.2 A silence SUPPRESSES an alert instance at instant `t` when
`window_start <= t < window_end`, the alert instance carries the label
`match_label`, and that label's value equals `match_value`. The window is half
open. An alert instance that does not carry the label is never suppressed by
that silence, whatever `match_value` says.

7.3 A catalogue row with `kind` of `inhibit` is an INHIBITION. It names a
`source_alertname`, a `target_alertname` and a list of `equal_labels`,
separated by a vertical bar and possibly empty.

7.4 An inhibition SUPPRESSES an alert instance `T` at instant `t` when the
alertname of `T` is `target_alertname` and there is an alert instance `S`,
other than `T`, whose alertname is `source_alertname`, which is FIRING at `t`,
and such that for every label named in `equal_labels` both `S` and `T` carry
that label and the two values are equal. A rule naming no label is matched by
any firing source. An alert instance that does not carry one of the named
labels is never inhibited by that rule.

7.5 What decides an inhibition is whether the source is FIRING. Whether the
source is itself suppressed does not enter into it.

7.6 An alert instance is NOTIFIABLE at `t` when it is firing at `t` and no
selected silence and no selected inhibition suppresses it at `t`. A suppressed
alert instance does not reach the notification pipeline of section 8.

7.7 A NOTIFIABLE EPISODE of an alert instance is a maximal run of consecutive
evaluation instants at which it is notifiable. Its `starts_at` is the first
instant of the run.

## 8. The notification pipeline

8.1 Alerts are grouped by the `service` label alone, so every alert instance on
one service shares one group, whatever its alertname, and the `group_key` of a
notification is that service.

8.2 A group has a NOTIFIED SET, which is the set of notifiable episodes the
group's last notification reported as firing, and is empty before its first
notification.

8.3 A group has a QUEUE of OUTSTANDING episodes, in the order they became
outstanding, then by `alertname` ascending, then by `instance` ascending, then
by `starts_at` ascending. An episode becomes outstanding

```
when it becomes notifiable;
when it stops being notifiable, if the group has reported it firing and has
    not since reported it resolved;
when 34 evaluation instants have passed since the group last reported it
    firing and it is still notifiable, which is the REPEAT of section 8.7.
```

An episode already in the queue does not enter it a second time.

8.4 When the queue is not empty and no notification is already due, one becomes
due: at `t + 30` if the group has never sent a notification, and otherwise at
the later of `t` and 60 seconds after its last notification.

8.5 THE BUDGET. At the instant a notification is due, the group sends one
notification carrying AT MOST THREE members, taken from the front of the queue.
Episodes beyond the third stay in the queue, in the same order, and go in a
later notification. Nothing is ever dropped. When the queue is still not empty
after a notification is sent, the next one becomes due 60 seconds later.

8.6 THE FLUSH. At the last evaluation instant of the window, a group whose
queue is not empty sends one notification carrying EVERY episode in it,
whatever is due and however many there are. The flush is not subject to the
budget of section 8.5, so no episode is left unsaid.

8.7 THE REPEAT. An episode that is still notifiable 34 evaluation instants
after the group last reported it firing becomes outstanding again and is
reported again.

8.8 A member is reported with its status AS OF THE INSTANT THE NOTIFICATION IS
SENT. An episode notifiable at that instant is reported with `status` of
`firing` and `ends_at` of `0`. An episode that is no longer notifiable at that
instant is reported with `status` of `resolved` and `ends_at` equal to THE
INSTANT THE NOTIFICATION IS SENT, which is when the group said so, and not the
instant at which the episode stopped being notifiable. Both carry the
`starts_at` of the episode.

8.9 The members of one notification are ordered by `alertname` ascending, then
by `instance` ascending, then by `starts_at` ascending, and are numbered
`member_index` from 1. The ordering uses the `instance` label as it stands.

8.10 Having sent, the group's notified set becomes the episodes it has just
reported as firing, and every episode carried by that notification leaves the
queue.

8.11 The notifications of all groups are ordered by `sent_at` ascending and
then by `group_key` ascending, and are numbered `notification_id` from 1 over
the whole history. A notification's number therefore depends on every
notification any group sent before it.

## 9. What a configuration is

9.1 A CONFIGURATION is a set of catalogue rows carrying at most one alert rule
per alertname and any number of inhibitions and silences.

9.2 A configuration REPRODUCES the history when replaying sections 5 to 8 under
it yields, for every notification of `/app/data/notification_log.csv` and no
others, the same `sent_at`, the same `group_key`, and the same ordered list of
members with the same `alertname`, `service`, `instance`, `status`, `starts_at`
and `ends_at`.

9.3 The COST of a configuration is the sum of the `cost` of its rows.

9.4 THE REPRODUCED HISTORY of a configuration is the notification history that
replaying sections 5 to 8 under that configuration yields: the notifications it
sends, each carrying the `notification_id` that 8.11 assigns it, its `sent_at`,
its `group_key` and its ordered members. Where a column of section 11 or of
section 12 names the reproduced history it means that produced history, and
never `/app/data/notification_log.csv` as published. For the answer
configuration the two agree on every field 9.2 names, which is exactly what 9.2
requires of it; for a configuration that does not reproduce the history they
need not agree at all.

## 10. The answer

10.1 The ANSWER CONFIGURATION is a configuration that reproduces the history
and whose cost is least among all such configurations.

10.2 Where more than one configuration reaches that cost, the answer is the one
whose rule ids, written in ascending order, form the least sequence: compare
them position by position, and where one is a prefix of the other the shorter
sequence is the lesser.

## 11. `/app/output/alerting_config.csv`

One row for every rule of the answer configuration, under the header

```
position,rule_id,kind,alertname,cost,episodes_produced,episodes_suppressed,notifications_carried
```

11.1 `position` numbers the rows from 1 in the order they are written.

11.2 `rule_id`, `kind` and `cost` are copied from the catalogue row.

11.3 `alertname` is the rule's own `alertname` for an alert rule, the
`target_alertname` for an inhibition, and a single hyphen for a silence.

11.4 `episodes_produced` is, for an alert rule, the number of rows of
`/app/output/alert_ledger.csv` carrying its `rule_id`, and 0 for any other row.

11.5 `episodes_suppressed` is the number of rows of the ledger whose
`suppressed_by` column carries this `rule_id`, and 0 where none does.

11.6 `notifications_carried` is, for an alert rule, the number of distinct
`notification_id` of the reproduced history carrying at least one member whose
`alertname` is this rule's alertname, and 0 for any other row.

11.7 The rows are ordered by `rule_id` ascending.

## 12. `/app/output/alert_ledger.csv`

One row for every FIRING EPISODE the answer configuration produced, where a
firing episode is a maximal run of consecutive evaluation instants at which one
alert instance is firing. A firing episode is a row whether or not any of it
was notifiable. The header is

```
position,alertname,service,instance,rule_id,active_since,firing_from,firing_to,firing_samples,notified_samples,suppressed_by,first_notification,last_notification,first_member_index,resolved_at
```

12.1 `position` numbers the rows from 1 in the order they are written.

12.2 `alertname` and `service` are the alert instance's labels as they stand.

12.3 `rule_id` is the alert rule of the answer configuration that produced the
episode.

12.4 `instance` is the value that `/app/data/label_registry.csv` gives in its
`canonical_instance` column for the row whose `instance` column is the alert
instance's `instance` label.

12.5 `active_since` is the active since of section 6.3 at the first instant of
the episode.

12.6 `firing_from` and `firing_to` are the first and the last evaluation
instant of the episode.

12.7 `firing_samples` is the number of evaluation instants in the episode.

12.8 `notified_samples` is the number of those instants at which the alert
instance was notifiable.

12.9 `suppressed_by` is the least `rule_id` of a selected rule that suppresses
the alert instance at the FIRST instant of the episode at which it was
suppressed, and a single hyphen where no instant of the episode was suppressed.
More than one selected rule can suppress the same instant; the least id is the
one written.

12.10 `first_notification` and `last_notification` are the least and the
greatest `notification_id` of the reproduced history carrying a member whose
alertname, service and instance are this alert instance's and whose `starts_at`
lies at or after `firing_from` and at or before `firing_to`; where no
notification does, both are a single hyphen.

12.11 `first_member_index` is the `member_index` of that member inside the
notification `first_notification` names, and a single hyphen where
`first_notification` is one. Section 8.9 is what fixes it.

12.12 `resolved_at` is the `ends_at` of the resolved member of the reproduced
history whose alertname, service and instance are this alert instance's and
whose `starts_at` is the greatest lying at or after `firing_from` and at or
before `firing_to`, and a single hyphen where no resolved member does. Section
8.8 is what fixes it: this column carries the instant the group SAID the
episode had stopped, which is not in general the instant it stopped.

## 13. The order of the rows

13.1 The rows of `/app/output/alerting_config.csv` are ordered by `rule_id`
ascending.

13.2 The rows of `/app/output/alert_ledger.csv` are ordered by `alertname`
ascending, then by `instance` ascending, then by `firing_from` ascending.
Ordering is by the bytes of the UTF-8 encoding.

13.3 Both files are UTF-8, line feed terminated including the last row, with no
quoting, no padding and no space beside any comma. `position`, `rule_id`,
`cost`, `episodes_produced`, `episodes_suppressed`, `notifications_carried`,
`active_since`, `firing_from`, `firing_to`, `firing_samples` and
`notified_samples` are plain unsigned decimal integers with no sign and no
leading zero. `suppressed_by`, `first_notification`, `last_notification`,
`first_member_index` and `resolved_at` are either a plain unsigned decimal
integer or a single hyphen.

## 14. The control totals

`/app/data/control_totals.csv` publishes four totals. Each counts what the
published files contain, and none of them is a function of the configuration.

```
notifications         the number of distinct notification_id in the log
notification_members  the number of member rows in the log
scrape_samples        the number of rows in the scrape history
missing_scrapes       the number of pairs of a series and an evaluation
                      instant for which the scrape history has no row
```
