---
name: verify-sql
description: Measure the SQL a change actually issues with sql-tap, and back claims like "fixed the N+1" or "reduced queries" with a before/after report. Use when changing code that talks to a database (ORM queries, repositories, data loaders, query optimization, N+1 fixes) in a project where sql-tapd is available, and before reporting that SQL behavior improved or did not regress.
---

# Verify SQL changes with sql-tap

Measure the same command before and after the change with `sql-tap run`, compare with `sql-tap diff`, and report only what the reports show.

## 1. Check the setup

Before measuring, confirm all of these. If one cannot be confirmed, stop and ask the user instead of guessing.

- **sql-tapd is running** and you know its gRPC address (`-grpc`, default `localhost:9091`). A project may configure it in `.sql-tap.yaml` (`grpc:`).
- **The command connects through the proxy.** Its database address must be sql-tapd's `-listen` port, not the database's own port. For example, if sql-tapd listens on `:5433` in front of PostgreSQL on `:5432`, the command's `DATABASE_URL` must use `5433`. If it points at the database directly, nothing is captured.
- **No other application uses the same sql-tapd.** Its queries would be counted too.
- **Results are not cached.** A cached test run does not touch the database. For Go, pass `-count=1` to `go test`.

## 2. Measure before the change

Pick the narrowest command that exercises the code you are changing, and use exactly the same command afterwards.

```bash
sql-tap run -out=<tmp>/before.json <addr> -- go test -count=1 ./order/...
```

Write reports to a scratch location, not into the repository, and create its directory first: `run` does not create it.

## 3. Make the change, then measure again

```bash
sql-tap run -out=<tmp>/after.json <addr> -- go test -count=1 ./order/...
```

## 4. Read the exit codes of `run`

`run` passes the command's own exit code through when the command fails, so a code like `1` or `2` may come from the command, not from sql-tap. **Check the command's output first** (e.g. `FAIL` lines from `go test`). If the command failed, the code is the command's: fix that first; the SQL numbers do not matter yet.

Only when the command succeeded, read the code as sql-tap's:

| Exit code | Meaning | What to do |
|---|---|---|
| `2` | No statements captured, sql-tapd did not confirm the subscription, the measurement may be incomplete, or another error (bad arguments, the command could not start, `-out` could not be written) | Do not use the report. Read sql-tap's `Error:` line on stderr: no statements means the command bypassed the proxy; an incomplete measurement (`incomplete` / `dropped_events` in the JSON) means measure again; an unconfirmed subscription means sql-tapd may be older than sql-tap; anything else names the problem to fix |
| `1` | N+1 or slow queries found | The report is valid; these are findings, not errors |
| `0` | No problems found | The report is valid |

## 5. Compare

```bash
sql-tap diff -format=markdown <tmp>/before.json <tmp>/after.json
```

| Exit code | Meaning |
|---|---|
| `0` | No regressions |
| `1` | Regressions: more statements, more N+1 queries, or a query newly flagged as N+1 |
| `2` | The comparison is invalid: different commands, no statements, or an incomplete report |

- On `1`, investigate and fix before reporting. Do not claim the change is fine.
- On `2`, measure again; do not work around it.
- Do not pass `-allow-command-mismatch`, `-allow-increase` or `-fail-on-slow` unless the user asked for it, and say so in the report when you do.
- Never edit a report by hand.

## 6. Report

Include the Markdown output of `diff` as is, then state separately:

- **Verified:** what the numbers show, for the measured command only (e.g. "statements 41 → 2 and N+1 36 → 0 in `go test ./order/...`").
- **Not verified:** what the measurement does not cover, such as code paths the command does not exercise, production data volumes, and query latency.

Keep these limits in mind:

- Slow queries and total time depend on timing and vary between runs. `diff` only warns about slow queries; do not present either as a measured improvement.
- The numbers describe the measured command, not the whole application.
