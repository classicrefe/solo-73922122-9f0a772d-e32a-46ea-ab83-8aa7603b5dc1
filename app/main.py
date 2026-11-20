"""Half-Batch Transaction Lab.

Experiment API for reproducing "half-batch read" anomalies (and other
concurrency hazards such as deadlocks) that occur when a data-warehouse
loader writes a batch master table (``batches``) and its measurement
details (``readings``) while a reader queries them.

Each *round* starts from the same baseline data in its own schema and
offers two independent transaction paths (A and B).  Every path can be
started, stepped through single trusted SQL statements, and committed or
rolled back.  All inputs, results, raw errors and transaction outcomes
are recorded for later analysis of the load boundary.

Interactive docs: /docs
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Path
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

from .core import BusyError, Round, RoundManager, RoundStateError

manager: RoundManager


@asynccontextmanager
async def lifespan(app: FastAPI):
    global manager
    manager = RoundManager(
        dsn=os.environ.get(
            "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres"),
        step_timeout=float(os.environ.get("STEP_TIMEOUT_SECONDS", "10")),
        max_rounds=int(os.environ.get("MAX_PARALLEL_ROUNDS", "2")),
    )
    manager.start()
    yield
    manager.shutdown()


app = FastAPI(
    title="Half-Batch Transaction Lab",
    version="1.0.0",
    description=__doc__,
    lifespan=lifespan,
)


class SqlIn(BaseModel):
    sql: str = Field(
        ..., min_length=1,
        description="One trusted SQL statement, executed as-is on this "
                    "transaction's connection. Unqualified table names "
                    "resolve to the round's own schema.",
        examples=["SELECT * FROM batches WHERE ready = true ORDER BY id"])


def get_round_or_404(round_id: str) -> Round:
    rnd = manager.get_round(round_id)
    if rnd is None:
        raise HTTPException(404, f"round {round_id!r} not found")
    return rnd


def check_tx(tx: str) -> str:
    tx = tx.upper()
    if tx not in ("A", "B"):
        raise HTTPException(422, "tx must be 'A' or 'B'")
    return tx


def check_single_statement(sql: str) -> str:
    s = sql.strip()
    if not s:
        raise HTTPException(400, "empty SQL")
    body = s[:-1] if s.endswith(";") else s
    if ";" in body:
        raise HTTPException(
            400, "only a single statement per step is allowed "
                 "(semicolons inside string literals are not supported)")
    return s


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")


@app.post("/rounds", status_code=201, tags=["rounds"],
          summary="Create a round, reset to the baseline data")
def create_round():
    """Creates a new round with its own schema preloaded with the baseline:

    - `batches`: `(1, ready=true)`, `(2, ready=false)`
    - `readings`: `(1,1,10.5)`, `(2,1,11.0)`, `(3,1,9.8)`

    At most two rounds may be in flight at the same time; data and
    connections are never shared between rounds.
    """
    try:
        rnd = manager.create_round()
    except BusyError as exc:
        raise HTTPException(409, str(exc))
    except Exception as exc:
        raise HTTPException(500, f"failed to create round: {exc}")
    return rnd.summary()


@app.get("/rounds", tags=["rounds"], summary="List all rounds")
def list_rounds():
    return [r.summary() for r in manager.list_rounds()]


@app.get("/rounds/{round_id}", tags=["rounds"],
         summary="Round detail incl. the full step record")
def round_detail(round_id: str):
    return get_round_or_404(round_id).snapshot()


@app.post("/rounds/{round_id}/tx/{tx}/begin", tags=["tx"],
          summary="Begin transaction A or B on its own connection")
def begin_tx(round_id: str, tx: str = Path(pattern="^[ABab]$")):
    rnd = get_round_or_404(round_id)
    try:
        rnd.begin_tx(check_tx(tx))
    except RoundStateError as exc:
        raise HTTPException(409, str(exc))
    return rnd.summary()


@app.post("/rounds/{round_id}/tx/{tx}/sql", status_code=202, tags=["tx"],
          summary="Queue one trusted SQL statement on this transaction")
def run_sql(round_id: str, body: SqlIn, tx: str = Path(pattern="^[ABab]$")):
    """The statement runs on this transaction's dedicated connection.  The
    call returns immediately with the step record (status `queued`); poll
    `GET /rounds/{round_id}/steps/{seq}` for the outcome.  A blocked
    statement does not prevent the other transaction from proceeding."""
    rnd = get_round_or_404(round_id)
    sql = check_single_statement(body.sql)
    try:
        rec = rnd.submit_step(check_tx(tx), "sql", sql)
    except RoundStateError as exc:
        raise HTTPException(409, str(exc))
    return {k: v for k, v in rec.items() if not k.startswith("_")}


@app.post("/rounds/{round_id}/tx/{tx}/commit", status_code=202, tags=["tx"],
          summary="Commit this transaction")
def commit_tx(round_id: str, tx: str = Path(pattern="^[ABab]$")):
    rnd = get_round_or_404(round_id)
    try:
        rec = rnd.submit_step(check_tx(tx), "commit")
    except RoundStateError as exc:
        raise HTTPException(409, str(exc))
    return {k: v for k, v in rec.items() if not k.startswith("_")}


@app.post("/rounds/{round_id}/tx/{tx}/rollback", status_code=202, tags=["tx"],
          summary="Roll back this transaction")
def rollback_tx(round_id: str, tx: str = Path(pattern="^[ABab]$")):
    rnd = get_round_or_404(round_id)
    try:
        rec = rnd.submit_step(check_tx(tx), "rollback")
    except RoundStateError as exc:
        raise HTTPException(409, str(exc))
    return {k: v for k, v in rec.items() if not k.startswith("_")}


@app.get("/rounds/{round_id}/steps/{seq}", tags=["records"],
         summary="Fetch one step record")
def step_detail(round_id: str, seq: int):
    rec = get_round_or_404(round_id).find_step(seq)
    if rec is None:
        raise HTTPException(404, f"no step {seq} in round {round_id!r}")
    return rec


@app.post("/rounds/{round_id}/stop", tags=["rounds"],
          summary="Stop the round (accepted promptly)")
def stop_round(round_id: str):
    """Accepted immediately.  In-flight statements are cancelled, queued
    steps are marked `canceled` without executing, uncommitted work is
    rolled back and connections are released.  The round reports its final
    state (`finished`) only after all of that completed - poll
    `GET /rounds/{round_id}`.  Records already written are never rewritten
    by late results; committed transactions keep their records."""
    rnd = get_round_or_404(round_id)
    rnd.stop("stopped")
    return rnd.summary()


@app.post("/rounds/{round_id}/finish", tags=["rounds"],
          summary="Finish the round normally and clean up")
def finish_round(round_id: str):
    rnd = get_round_or_404(round_id)
    rnd.stop("completed")
    return rnd.summary()
