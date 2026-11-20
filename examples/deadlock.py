#!/usr/bin/env python3
"""Deadlock between the two transaction paths.

A locks batches(id=1), B locks readings(id=1); then A wants readings(id=1)
while B wants batches(id=1).  PostgreSQL detects the cycle and aborts one
statement with SQLSTATE 40P01.  The record shows exactly WHICH transaction
took the error; the lab keeps using the same connection for each path and
never swaps connections to keep a failed transaction alive.

Note how A's blocked UPDATE does not prevent B from proceeding.

Run:  python3 examples/deadlock.py
"""

import time

from lab_client import (begin, commit, new_round, req, rollback, show_round,
                        sql, wait_round, wait_step)

rid = new_round()
begin(rid, "A")
begin(rid, "B")

s1 = wait_step(rid, sql(rid, "A", "UPDATE batches SET ready = false WHERE id = 1"))
s2 = wait_step(rid, sql(rid, "B", "UPDATE readings SET value = 99 WHERE id = 1"))

# A now blocks on B's row lock; it stays 'running' while B keeps moving.
s3 = sql(rid, "A", "UPDATE readings SET value = 88 WHERE id = 1")
time.sleep(0.5)  # make sure A's statement is really waiting on the lock
# B closes the cycle -> PostgreSQL kills one of the two statements.
s4 = sql(rid, "B", "UPDATE batches SET ready = true WHERE id = 1")

r3 = wait_step(rid, s3)
r4 = wait_step(rid, s4)

# Both transactions are wound down (the aborted one MUST be rolled back).
wait_step(rid, rollback(rid, "A"))
wait_step(rid, rollback(rid, "B"))

req("POST", f"/rounds/{rid}/finish")
wait_round(rid)
show_round(rid)

loser = r3 if r3["status"] == "error" else r4
print(f"\ndeadlock victim: tx {loser['tx']} step {loser['seq']} "
      f"(pgcode={loser['pgcode']})")
