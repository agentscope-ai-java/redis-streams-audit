# redis-streams-audit

An auditor for Redis Streams consumer groups. It reads the Pending Entries
List (PEL) -- the thing that actually breaks in production -- and tells you,
per stream and group, what's wrong and which command fixes it.

```
$ python examples/live_demo.py
== orders / workers ==
  pending: 3  lag: 0
  age: min=514ms max=514ms mean=514ms  [<1s=3]
  consumer 'doomed-worker': pending=3 idle=514ms inactive=514ms
  [WARN] dead-consumer-candidate [doomed-worker]: Consumer 'doomed-worker' owns 3 pending
         entries and has had no read/ack/claim activity for 514 ms (at or above 300 ms) --
         a likely-dead consumer, not just a slow one.
         -> XAUTOCLAIM orders workers <recovery-consumer> 300 0 COUNT 100
            Redis never expires a consumer or notices it died -- XGROUP DELCONSUMER and
            reassignment are the only ways its entries move. XAUTOCLAIM with a matching
            min-idle-time reassigns everything this idle in one sweep, not just this
            consumer's share.

== events / g ==
  pending: 3  lag: 0
  age: min=6ms max=6ms mean=6ms  [<1s=3]
  consumer 'c1': pending=3 idle=6ms inactive=6ms
  [CRIT] orphaned-pel-entry: 3 pending entries in group 'g' point to stream ids that no
         longer exist -- they were removed by XTRIM/MAXLEN (or XDEL) while still
         unacknowledged. XTRIM does not clean the PEL; these will stay pending, with
         their data unrecoverable, until something claims or acks them.
         -> XAUTOCLAIM events g <recovery-consumer> 0 0 COUNT 100
            XAUTOCLAIM (and XCLAIM) silently drop a pending entry from the PEL when its
            underlying stream entry is gone -- this is the only way to clear these, since
            the data cannot be reprocessed. Redis 7+ reports which ids it deleted this
            way in XAUTOCLAIM's third reply element.

2 finding(s): 1 critical, 1 warning, 0 info; 0 error(s); exit 2
```

That's the real, unedited output of `examples/live_demo.py` against a real
`redis:7-alpine` container (`docker run -d -p 16379:6379 redis:7-alpine`),
doing exactly what's described below: a consumer that crashed before `XACK`,
and a stream trimmed to zero entries while three were still pending.

## The problem

A Streams consumer group tracks every message it has delivered but not yet
`XACK`'d in a Pending Entries List. That list is where the actual production
incidents live, and none of them look like an error:

- **A consumer crashes between `XREADGROUP` and `XACK`.** Its entries sit in
  the PEL forever. Nothing is wrong from Redis's point of view -- it's
  working exactly as designed -- until a memory or lag alert fires, by
  which point the PEL has been growing quietly for days.
- **A dead consumer still owns entries.** Redis has no concept of a
  consumer dying. `XINFO CONSUMERS` will list a consumer that hasn't done
  anything in a week, still holding pending entries, forever -- until
  `XGROUP DELCONSUMER` or a claim moves them. Nothing does that for you.
- **A poison message gets retried forever.** An entry that fails every time
  it's processed just gets its delivery count bumped by every
  `XCLAIM`/`XAUTOCLAIM` retry. There's no built-in cap and no built-in
  dead-letter step -- without one, it retries until the heat death of the
  universe, or your on-call notices the same message id in every alert.
- **`XTRIM`/`MAXLEN` and pending entries interact in a way that surprised
  us.** See below -- it's the reason this tool has a category most
  Streams tooling doesn't.

## What we found, reproduced against a real server

Every claim below was checked against a live `redis:7-alpine` container
(`docker run -d -p 16379:6379 redis:7-alpine`), not recalled from
documentation. `examples/live_demo.py` reproduces all of it end to end.

**`XTRIM` does not touch the Pending Entries List. At all.** Three entries
were read (not acked) by a consumer group, then the stream was trimmed to
zero entries -- real `redis-cli` session against `redis:7-alpine`, ids
elided to their tail for readability:

```
> XADD events * k v1                     (and two more: ...986-0, ...019-0)
1790452986952-0
> XGROUP CREATE events g 0
OK
> XREADGROUP GROUP g c1 COUNT 3 STREAMS events >
(3 entries returned)
> XTRIM events MAXLEN 0
(integer) 3
> XLEN events
(integer) 0
> XRANGE events - +
(empty array)
> XPENDING events g
1) (integer) 3                          <- still 3. The data is gone; the PEL doesn't know.
2) "1790452986952-0"
3) "1790452987019-0"
4) 1) 1) "c1"
      2) "3"
```

The stream is empty. The PEL still lists all three entries as pending, at
full age-since-delivery, forever, because nothing prunes a PEL entry when
its underlying stream data disappears -- `XTRIM` (and `XDEL`) only touch
the stream body. A naive "PEL size" alert on this group would keep firing
for data that no longer exists and can never be reprocessed.

**Only `XCLAIM`/`XAUTOCLAIM` clear a dangling entry, and they do it
silently.** Claiming a pending id whose stream entry is gone returns nothing
for that id (there's no data to return) but *does* remove it from the PEL as
a side effect -- continuing the same session:

```
> XCLAIM events g c2 0 1790452986952-0
(empty array)                            <- no data returned
> XPENDING events g - + 10
1) 1) "1790452986986-0"                  <- the claimed id (...952-0) is gone
   2) "c1"                                  from the PEL now; the other two remain
   3) (integer) 4583
   4) (integer) 1
2) 1) "1790452987019-0"
   2) "c1"
   3) (integer) 4583
   4) (integer) 1
```

`XAUTOCLAIM` goes further on Redis 7+: its reply's third element explicitly
lists which ids it deleted this way, rather than leaving you to infer it.
Sweeping the same group again, with the other two still pending:

```
> XAUTOCLAIM events g janitor 0 0
1) "0-0"
2) (empty array)                         <- nothing claimable: no data left
3) 1) "1790452986986-0"                  <- both remaining ids purged from
   2) "1790452987019-0"                     the PEL because their stream
                                            data is gone
```

`XACK` still works directly on a dangling id too (it only needs the id, not
the data) -- so if a consumer somehow still has the payload cached, acking
it is fine. But nothing calls `XACK` for you; the practical fix is a claim
sweep. This is why `redis-streams-audit` reports **orphaned-pel-entry**
(critical) as its own category, separate from ordinary stale entries: the
remedy is "run a claim sweep to clear bookkeeping you can never act on
otherwise," not "reprocess this," and conflating the two hides that the
data is unrecoverable.

**A consumer keeps existing after everything it holds is reclaimed.**
`XINFO CONSUMERS` still lists a consumer with `pending: 0` after
`XAUTOCLAIM` takes all of its entries -- Redis only forgets a consumer on
an explicit `XGROUP DELCONSUMER`, never automatically. Combined with the
`inactive` field (Redis 6.2+: time since that consumer's last read, ack, or
claim, as distinct from `idle`, which is about its oldest pending entry),
this is the closest thing to "is this consumer dead" the protocol offers --
a heuristic, never a certainty. A consumer stuck in a long GC pause looks
identical to one whose process was `kill -9`'d.

## Existing tooling in this space

Searching for prior art turned up
[RedisStreamScope](https://github.com/ghkdqhrbals/redisstreamscope), a
zero-star, actively-developed project (first commit late July, most recent
update late August, at the time of writing): a self-hosted Go server with a
React UI, SQLite-backed history, and real depth on exactly this problem --
lag and pending-entry monitoring, previewed `XACK`/`XCLAIM`/`XAUTOCLAIM`/
`XGROUP SETID` recovery, DLQ quarantine-and-replay, consumer join/leave/
stalled history, alerting with Slack webhooks. It is a genuinely strong
answer to "give me a console for operating Streams," and if that's what you
need, it may already be the better tool.

It is a different kind of thing, though, not a subset or superset of this
one: it's a long-running service you deploy (Docker or Helm), with its own
database, its own user accounts, and a license (PolyForm Noncommercial)
that excludes commercial use without a separate agreement. There's nothing
to `pip install` and call from a script. `redis-streams-audit` is the other
shape: a zero-dependency library and a one-shot CLI you point at a client
you already have, that exits nonzero so a CI job or a cron check can gate
on it -- no server to run, no data store of its own, no license
restriction, and (per its own reasoning above) an explicit, tested category
for PEL entries that `XTRIM` orphaned, which we did not find called out
anywhere else. If you want a dashboard, use that project. If you want a
Python-native check your pipeline can fail on, this is that check.

## Install

```sh
pip install redis-streams-audit
```

Python >= 3.11. Zero runtime dependencies -- see [How it works](#how-it-works).

## Use

As a library, against a client you already have (any object shaped like
redis-py's -- see [Options](#the-duck-typed-client)):

```python
import redis
from redis_streams_audit import audit, Thresholds

client = redis.Redis(host="prod-redis", decode_responses=True)
report = audit(client, ["orders", "events"], thresholds=Thresholds(poison_delivery_count=3))

for finding in report.findings:
    print(finding.severity, finding.stream, finding.group, finding.message)
    if finding.remedy:
        print("  ->", finding.remedy.command)

raise SystemExit(report.exit_code)  # 0 clean, 1 warnings, 2 critical, 3 couldn't audit
```

As a CLI, with no `redis` package installed at all (see
[the built-in client](#the-built-in-zero-dependency-client)):

```sh
redis-streams-audit --host prod-redis --stream orders --stream events --json
echo $?   # 0 / 1 / 2 / 3, straight into a CI gate
```

## What it checks

| Finding | Severity | What it means | Remedy this tool gives you |
| --- | --- | --- | --- |
| `pel-growth` | warning / critical | Total pending count for a group crossed a threshold. | `XAUTOCLAIM` sweep in batches; check consumer throughput. |
| `orphaned-pel-entry` | critical | Pending entries whose stream data was removed by `XTRIM`/`XDEL`. Unrecoverable. | `XAUTOCLAIM` (or `XCLAIM`) to purge the dangling bookkeeping. |
| `poison-candidate` | critical | Entries delivered more than the configured threshold, never acked. | Claim, copy to a dead-letter stream, then `XACK` the original. |
| `stale-pending-entry` | warning | Entries idle past the configured threshold, not (yet) poison or orphaned. | Check whether the owning consumer is alive or just slow. |
| `dead-consumer-candidate` | warning | A consumer with pending entries and no activity (`inactive`) past threshold. | `XAUTOCLAIM` with a matching `min-idle-time`. |
| `group-lag` | warning / critical | `XINFO GROUPS`' `lag` (entries added but not yet delivered) past threshold. | Check consumer count/throughput against ingestion rate. |
| `lag-unavailable` | info | Redis reports `lag: null` (usually after `XGROUP SETID`/`XSETID`, or pre-7.0). | Compare `last-delivered-id` against the stream tail by timestamp instead. |

Every finding carries a stream, a group, a severity, a human-readable
message, and -- except for the two `lag` findings, which are diagnostic
rather than actionable on their own -- a `remedy` with the exact command to
run next.

## Options

### Thresholds

| Field | Default | Applies to |
| --- | --- | --- |
| `stale_after_ms` | 60,000 (1 min) | `stale-pending-entry` |
| `dead_consumer_after_ms` | 300,000 (5 min) | `dead-consumer-candidate` |
| `poison_delivery_count` | 5 | `poison-candidate` |
| `lag_warn` / `lag_critical` | 1,000 / 10,000 | `group-lag` |
| `pel_size_warn` / `pel_size_critical` | 1,000 / 10,000 | `pel-growth` |

Pass a `Thresholds(...)` to `audit()`/`audit_group()`, or the matching
`--stale-after-seconds`, `--dead-consumer-after-seconds`,
`--poison-threshold`, `--lag-warn`, `--lag-critical`, `--pel-warn`,
`--pel-critical` CLI flags.

### The duck-typed client

`audit()`/`audit_group()` take anything shaped like
`redis_streams_audit.RedisStreamsClient`: `xlen`, `xinfo_stream`,
`xinfo_groups`, `xinfo_consumers`, `xpending`, `xpending_range`, spelled and
shaped exactly like redis-py's client (method names, argument order,
`dict`-with-`str`-keys replies -- i.e. `decode_responses=True`). Pass a real
`redis.Redis`/`redis.asyncio.Redis` and it works as-is. The audit never
calls a write command -- no `XCLAIM`, `XAUTOCLAIM`, `XACK`, or `XTRIM` --
it only reads state and tells you what to run.

Streams to audit are always named explicitly, never discovered by scanning
the keyspace -- the same reasoning that makes `KEYS *` a footgun applies to
any full-keyspace walk, and naming your streams is one line.

### The built-in, zero-dependency client

The CLI has nothing to `import redis` with, so `redis_streams_audit.resp`
ships a small real RESP2 client over a raw socket -- `XLEN`, the three
`XINFO` subcommands, and both forms of `XPENDING`, and nothing else. It
shapes replies identically to redis-py so `audit()` can't tell the two
apart (checked directly in `tests/test_integration.py`). It has no
pipelining, no RESP3, no TLS, no Cluster redirection, and `AUTH` takes a
password only (no ACL username). If you need any of that, use a real
client -- `redis_streams_audit.resp.MinimalRedisClient` exists for the CLI
and for people who want the zero-dependency path in their own code, not to
replace redis-py.

## What it does not do

- **It does not discover streams for you.** No `SCAN`, no `KEYS`. Name
  them.
- **It never mutates anything.** No auto-remediation, no automatic
  `XAUTOCLAIM`/dead-lettering. It tells you the command; you decide when
  to run it against production.
- **"Dead consumer" is a heuristic, not a fact.** Redis cannot tell you a
  consumer's process died. A long-idle consumer with pending entries and a
  GC-stalled-but-alive one look identical from the PEL's point of view.
- **No Cluster-awareness beyond what a Cluster-aware client already gives
  you.** Point it at one node (or a client that routes for you); it does
  not fan out across shards itself.
- **The built-in RESP client is deliberately minimal** -- see above. It is
  not a general-purpose Redis client.

## Develop

```sh
python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'
docker run -d -p 16379:6379 redis:7-alpine   # for the integration tests
.venv/bin/python -m pytest -q
.venv/bin/python -m mypy src --strict
```

Tests without a reachable Redis skip loudly (with a warning naming the
missing host/port), never silently.

## License

MIT
