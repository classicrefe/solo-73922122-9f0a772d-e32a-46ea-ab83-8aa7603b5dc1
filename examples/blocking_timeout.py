#!/usr/bin/env python3
"""Blocked statement + 10 second auto-stop.

A runs pg_sleep(30).  While it is blocked, B keeps proceeding - the two
paths are independent.  After 10 seconds the monitor stops the whole
round: A's statement is cancelled (SQLSTATE 57014), A's queued second
statement is marked canceled WITHOUT executing, uncommitted work is
rolled back, connections are released, and only then does the round
report its final state.  A fresh round afterwards proves that neither
locks nor data leaked into the next round.

Run:  python3 examples/blocking_timeout.py
"""

from lab_client import (begin, commit, new_round, req, show_round, sql,
                        wait_round, wait_step)

rid = new_round()
begin(rid, "A")
begin(rid, "B")

blocked = sql(rid, "A", "SELECT pg_sleep(30)")
queued = sql(rid, "A", "SELECT 1")  # stays queued behind the blocked one

# B is not affected by A's blocked statement.
wait_step(rid, sql(rid, "B", "SELECT count(*) AS n_readings FROM readings"))
wait_step(rid, commit(rid, "B"))

print("waiting for the 10s auto-stop ...")
rnd = wait_round(rid, timeout=45)
assert rnd["reason"] == "timeout", rnd
show_round(rid)

print("\n=== fresh round: baseline intact, no leftover locks or data ===")
rid2 = new_round()
begin(rid2, "A")
wait_step(rid2, sql(rid2, "A", "SELECT * FROM batches ORDER BY id"))
wait_step(rid2, sql(rid2, "A", "SELECT count(*) AS n_readings FROM readings"))
wait_step(rid2, commit(rid2, "A"))
req("POST", f"/rounds/{rid2}/finish")
wait_round(rid2)
show_round(rid2)
