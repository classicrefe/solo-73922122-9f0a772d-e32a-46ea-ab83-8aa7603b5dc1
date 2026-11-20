"""Core engine for the half-batch transaction lab.

A *round* is an isolated experiment: it gets its own PostgreSQL schema
(``r_<round_id>``) preloaded with the baseline ``batches`` / ``readings``
tables, and two independent transaction paths (``A`` and ``B``).  Each
transaction path is served by exactly one dedicated worker thread holding
exactly one dedicated connection for the whole life of the transaction.
If a statement fails or deadlocks, the error is reported on that very
connection/transaction - the lab never swaps in a fresh connection to
impersonate the original transaction.

Records are append-only.  Once a step record reaches a terminal state
(``done`` / ``error`` / ``canceled``) it is never rewritten, so late
results arriving after a stop cannot falsify the log.
"""

from __future__ import annotations

import queue
import threading
import time
import uuid
from datetime import datetime, timezone

import psycopg2
import psycopg2.extensions

TERMINAL_STEP_STATES = frozenset({"done", "error", "canceled"})
# tx states in which the path no longer holds resources that must be
# awaited before the round may report its final state
IDLE_TX_STATES = frozenset({"not_started", "committed", "rolled_back", "dead", "closed"})
MAX_ROWS = 500  # per-SELECT row cap kept in the record

# SQLSTATEs that mean the source schema/table disappeared under us
_MISSING_RELATION_CODES = frozenset({"3F000", "42P01"})

BASELINE_BATCHES = [(1, True), (2, False)]
BASELINE_READINGS = [(1, 1, 10.5), (2, 1, 11.0), (3, 1, 9.8)]

_SHUTDOWN = object()  # queue sentinel telling a worker to wind down


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def jsonable(value):
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


class BusyError(Exception):
    """Raised when the maximum number of parallel rounds is reached."""


class RoundStateError(Exception):
    """Raised when an operation is not allowed in the current round/tx state."""


class SourceRoundGone(Exception):
    """Raised when the source round is cleaned up before its snapshot was read."""


class ForkFailed(Exception):
    """Raised when copying the snapshot into a new round failed.

    The partial schema has already been dropped before this is raised, so the
    caller only has to report the reason; no residual data is left behind.
    """


class TxWorker(threading.Thread):
    """One worker thread == one connection == one transaction path."""

    def __init__(self, rnd: "Round", tx_name: str, dsn: str):
        super().__init__(daemon=True, name=f"tx-{rnd.id}-{tx_name}")
        self.rnd = rnd
        self.tx_name = tx_name
        self.dsn = dsn
        self.q: "queue.Queue" = queue.Queue()
        self.conn = None
        self.backend_pid = None
        self.running = False  # True while a statement executes on the conn
        self.stop_requested = threading.Event()
        self.ready_event = threading.Event()
        self.connect_error = None

    # ---- called from API threads ----
    def submit(self, item: dict) -> None:
        self.q.put(item)

    def request_stop(self) -> None:
        """Ask the worker to finish promptly: cancel any in-flight statement,
        discard queued steps, roll back and release the connection."""
        self.stop_requested.set()
        conn = self.conn
        if conn is not None and self.running and not conn.closed:
            try:
                conn.cancel()  # safe to call from another thread
            except Exception:
                pass
        self.q.put(_SHUTDOWN)

    # ---- worker thread body ----
    def run(self) -> None:
        try:
            self.conn = psycopg2.connect(self.dsn)
            self.conn.autocommit = False
            self.backend_pid = self.conn.get_backend_pid()
            with self.conn.cursor() as cur:
                cur.execute(f'SET search_path TO "{self.rnd.schema}"')
            self.conn.commit()
            self.rnd.tx_ready(self.tx_name)
        except Exception as exc:  # connection could not be established
            self.connect_error = exc
            self.rnd.tx_connect_failed(self.tx_name, exc)
            return
        try:
            while True:
                item = self.q.get()
                if item is _SHUTDOWN:
                    break
                if self.stop_requested.is_set():
                    # stopped before this step started: never execute it
                    self.rnd.finalize_step(
                        item["rec"], "canceled",
                        error="round stopped before this step started",
                    )
                    continue
                self._run_item(item)
                if self.conn is None or self.conn.closed:
                    # Connection is gone.  We must NOT reconnect to impersonate
                    # the original transaction; drain what is left as canceled.
                    self.rnd.tx_connection_lost(self.tx_name)
                    self._drain("connection lost; step not executed")
                    break
        finally:
            self._cleanup()

    def _drain(self, message: str) -> None:
        while True:
            try:
                pending = self.q.get_nowait()
            except queue.Empty:
                return
            if pending is _SHUTDOWN:
                return
            self.rnd.finalize_step(pending["rec"], "canceled", error=message)

    def _run_item(self, item: dict) -> None:
        rec, kind, sql = item["rec"], item["kind"], item.get("sql")
        self.rnd.mark_running(rec)
        self.running = True
        try:
            if kind == "sql":
                self.rnd.finalize_step(rec, "done", result=self._exec_sql(sql))
            elif kind == "commit":
                self.conn.commit()
                self.rnd.finalize_step(rec, "done", result={"outcome": "committed"})
                self.rnd.tx_finished(self.tx_name, "committed")
            elif kind == "rollback":
                self.conn.rollback()
                self.rnd.finalize_step(rec, "done", result={"outcome": "rolled_back"})
                self.rnd.tx_finished(self.tx_name, "rolled_back")
        except psycopg2.Error as exc:
            # Raw database error, kept verbatim for the record.  The
            # transaction (if any) is now aborted on THIS connection; it is
            # up to the user to roll it back - we never silently reconnect.
            self.rnd.finalize_step(
                rec, "error",
                error=str(exc).strip(),
                error_class=type(exc).__name__,
                pgcode=getattr(exc, "pgcode", None),
            )
        except Exception as exc:  # e.g. InterfaceError on a broken connection
            self.rnd.finalize_step(
                rec, "error", error=str(exc), error_class=type(exc).__name__,
            )
        finally:
            self.running = False

    def _exec_sql(self, sql: str) -> dict:
        with self.conn.cursor() as cur:
            cur.execute(sql)
            if cur.description is not None:
                cols = [d.name for d in cur.description]
                rows = cur.fetchmany(MAX_ROWS + 1)
                truncated = len(rows) > MAX_ROWS
                return {
                    "columns": cols,
                    "rows": [[jsonable(v) for v in row] for row in rows[:MAX_ROWS]],
                    "rowcount": cur.rowcount,
                    "truncated": truncated,
                }
            return {"rowcount": cur.rowcount, "status": cur.statusmessage}

    def _cleanup(self) -> None:
        conn, self.conn = self.conn, None
        if conn is not None and not conn.closed:
            try:
                if conn.get_transaction_status() != psycopg2.extensions.TRANSACTION_STATUS_IDLE:
                    conn.rollback()  # uncommitted work is rolled back
            except Exception:
                pass
            try:
                conn.close()  # connection (and its locks) released
            except Exception:
                pass
        self.rnd.tx_closed(self.tx_name)


class Round:
    def __init__(self, manager: "RoundManager", round_id: str, schema: str,
                 source_round_id: str | None = None,
                 copied: dict | None = None):
        self.manager = manager
        self.id = round_id
        self.schema = schema
        self.source_round_id = source_round_id  # None = baseline round
        self.copied = copied  # {"batches": n, "readings": n} for forked rounds
        self.lock = threading.RLock()
        self.state = "active"  # active -> stopping -> finished
        self.reason = None     # completed | stopped | timeout | shutdown
        self.detail = None
        self.created_at = utcnow_iso()
        self.finished_at = None
        self.stopping_since = None  # monotonic, for the terminate fallback
        self.cleanup_started = False  # guards against duplicate cleanup
        self.records: list[dict] = []
        self.seq = 0
        self.txs = {
            name: {"state": "not_started", "worker": None, "closing": False, "detail": None}
            for name in ("A", "B")
        }

    # ---------------- API-facing ----------------
    def begin_tx(self, tx: str) -> None:
        with self.lock:
            self._require_active()
            slot = self.txs[tx]
            if slot["state"] != "not_started":
                raise RoundStateError(f"tx {tx} already started (state={slot['state']})")
            worker = TxWorker(self, tx, self.manager.dsn)
            slot["worker"] = worker
            slot["state"] = "connecting"
            worker.start()
        if not worker.ready_event.wait(timeout=5.0):
            raise RoundStateError("timed out waiting for the database connection")
        if worker.connect_error is not None:
            raise RoundStateError(f"connection failed: {worker.connect_error}")

    def submit_step(self, tx: str, kind: str, sql: str | None = None) -> dict:
        with self.lock:
            self._require_active()
            slot = self.txs[tx]
            if slot["state"] != "open":
                raise RoundStateError(f"tx {tx} is not open (state={slot['state']})")
            if slot["closing"]:
                raise RoundStateError(
                    f"tx {tx} already received commit/rollback; no further steps")
            self.seq += 1
            rec = {
                "seq": self.seq, "tx": tx, "kind": kind, "sql": sql,
                "status": "queued", "result": None, "error": None,
                "error_class": None, "pgcode": None, "elapsed_ms": None,
                "queued_at": utcnow_iso(), "started_at": None, "ended_at": None,
            }
            self.records.append(rec)
            if kind in ("commit", "rollback"):
                slot["closing"] = True
            slot["worker"].submit({"rec": rec, "kind": kind, "sql": sql})
            return rec

    def stop(self, reason: str) -> None:
        """Accept a stop promptly: flip state, cancel in-flight statements,
        then let the workers wind down.  The final state is reported only
        after both paths ended and connections were released."""
        with self.lock:
            if self.state == "finished":
                return
            if self.state == "active":
                self.state = "stopping"
                self.reason = reason
                self.stopping_since = time.monotonic()
            workers = [s["worker"] for s in self.txs.values() if s["worker"] is not None]
        for w in workers:
            w.request_stop()
        self._maybe_finish()

    # ---------------- worker callbacks ----------------
    def tx_ready(self, tx: str) -> None:
        with self.lock:
            slot = self.txs[tx]
            slot["state"] = "open"
            slot["worker"].ready_event.set()

    def tx_connect_failed(self, tx: str, exc: Exception) -> None:
        with self.lock:
            slot = self.txs[tx]
            slot["state"] = "dead"
            slot["detail"] = f"connect failed: {exc}"
            slot["worker"].ready_event.set()
        self._maybe_finish()

    def tx_finished(self, tx: str, outcome: str) -> None:
        with self.lock:
            self.txs[tx]["state"] = outcome  # committed | rolled_back

    def tx_connection_lost(self, tx: str) -> None:
        with self.lock:
            slot = self.txs[tx]
            if slot["state"] not in ("committed", "rolled_back"):
                slot["state"] = "dead"
                slot["detail"] = "connection lost; transaction cannot continue"

    def tx_closed(self, tx: str) -> None:
        with self.lock:
            slot = self.txs[tx]
            if slot["state"] not in ("committed", "rolled_back", "dead"):
                slot["state"] = "closed"  # was open; rolled back and released
        self._maybe_finish()

    def mark_running(self, rec: dict) -> None:
        with self.lock:
            if rec["status"] in TERMINAL_STEP_STATES:
                return
            rec["status"] = "running"
            rec["started_at"] = utcnow_iso()
            rec["_mono"] = time.monotonic()

    def finalize_step(self, rec: dict, status: str, *, result=None, error=None,
                      error_class=None, pgcode=None) -> None:
        with self.lock:
            if rec["status"] in TERMINAL_STEP_STATES:
                return  # a late result must not rewrite the record
            rec["status"] = status
            rec["ended_at"] = utcnow_iso()
            mono = rec.pop("_mono", None)
            if mono is not None:
                rec["elapsed_ms"] = round((time.monotonic() - mono) * 1000, 1)
            if result is not None:
                rec["result"] = result
            if error is not None:
                rec["error"] = error
            if error_class is not None:
                rec["error_class"] = error_class
            if pgcode is not None:
                rec["pgcode"] = pgcode

    # ---------------- internals ----------------
    def _require_active(self) -> None:
        if self.state != "active":
            raise RoundStateError(
                f"round is {self.state}; no new steps are accepted")

    def _maybe_finish(self) -> None:
        with self.lock:
            if self.state != "stopping" or self.cleanup_started:
                return
            for slot in self.txs.values():
                if slot["state"] not in IDLE_TX_STATES:
                    return  # a path is still running / holding a connection
            # Both SQL paths ended, uncommitted work was rolled back and the
            # connections were released.  Run the cleanup exactly once.
            self.cleanup_started = True
        try:
            self.manager.drop_schema(self.schema)
            error = None
        except Exception as exc:
            error = str(exc)
        with self.lock:
            if error:
                # keep the round in "stopping" so the monitor retries the
                # cleanup instead of leaking the schema into later rounds
                self.cleanup_started = False
                self.detail = f"schema cleanup failed, retrying: {error}"
                return
            self.state = "finished"
            self.finished_at = utcnow_iso()

    # ---------------- views ----------------
    def summary(self) -> dict:
        with self.lock:
            return {
                "id": self.id,
                "state": self.state,
                "reason": self.reason,
                "schema": self.schema,
                "source_round_id": self.source_round_id,
                "copied": self.copied,
                "created_at": self.created_at,
                "finished_at": self.finished_at,
                "detail": self.detail,
                "txs": {
                    name: {"state": s["state"], "detail": s["detail"]}
                    for name, s in self.txs.items()
                },
            }

    def snapshot(self) -> dict:
        with self.lock:
            data = self.summary()
            data["records"] = [
                {k: v for k, v in rec.items() if not k.startswith("_")}
                for rec in self.records
            ]
            return data

    def find_step(self, seq: int) -> dict | None:
        with self.lock:
            for rec in self.records:
                if rec["seq"] == seq:
                    return {k: v for k, v in rec.items() if not k.startswith("_")}
            return None


class RoundManager:
    def __init__(self, dsn: str, step_timeout: float = 10.0, max_rounds: int = 2):
        self.dsn = dsn
        self.step_timeout = step_timeout
        self.max_rounds = max_rounds
        self.lock = threading.Lock()
        self.rounds: dict[str, Round] = {}
        # rounds being created right now (schema copy in progress).  They
        # already occupy a parallel-round slot but are not visible yet.
        self._pending = 0
        self._halt = threading.Event()
        self._monitor = threading.Thread(
            target=self._monitor_loop, daemon=True, name="round-monitor")

    def start(self) -> None:
        self._monitor.start()

    # ---------------- round lifecycle ----------------
    def _reserve_slot(self) -> None:
        with self.lock:
            active = sum(1 for r in self.rounds.values() if r.state != "finished")
            if active + self._pending >= self.max_rounds:
                raise BusyError(
                    f"already {active} round(s) in flight; "
                    f"at most {self.max_rounds} parallel rounds are allowed")
            self._pending += 1

    def _release_slot(self) -> None:
        with self.lock:
            self._pending -= 1

    def _register(self, schema: str, source_round_id: str | None,
                  copied: dict | None) -> Round:
        round_id = schema[2:]  # schema = r_<round_id>
        rnd = Round(self, round_id, schema, source_round_id=source_round_id,
                    copied=copied)
        with self.lock:
            self.rounds[round_id] = rnd
            self._pending -= 1
        return rnd

    def create_round(self) -> Round:
        """Create a round preloaded with the fixed baseline data."""
        self._reserve_slot()
        schema = f"r_{uuid.uuid4().hex[:8]}"
        try:
            self._create_schema(schema)
        except Exception:
            self._drop_schema_quietly(schema)  # no residue on failure
            self._release_slot()
            raise
        return self._register(schema, None, None)

    def fork_round(self, source_round_id: str) -> Round:
        """Create a new round whose data is a copy of a *committed snapshot*
        of the (still active) source round.

        Both tables are read in one REPEATABLE READ transaction, so the copy
        always reflects a single consistent committed state - never a
        half-applied batch.  The copy happens in its own transaction; the
        source round is not locked out and may still commit or roll back.
        The new round is exposed only after both tables were copied
        completely.  If the source schema disappears (cleaned up first) or
        the copy fails, the partial schema is dropped and the reserved slot
        is returned.
        """
        source = self.get_round(source_round_id)
        if source is None:
            raise KeyError(source_round_id)
        with source.lock:
            if source.state != "active":
                raise RoundStateError(
                    f"source round {source.id!r} is {source.state}; only an "
                    "active round can be forked")
        self._reserve_slot()  # 409 BusyError when both slots are taken

        schema = f"r_{uuid.uuid4().hex[:8]}"
        copied = {"batches": 0, "readings": 0}
        try:
            copied = self._copy_snapshot(source.schema, schema)
        except psycopg2.Error as exc:
            self._drop_schema_quietly(schema)
            self._release_slot()
            if getattr(exc, "pgcode", None) in _MISSING_RELATION_CODES \
                    and not self._schema_exists(source.schema):
                raise SourceRoundGone(
                    f"source round {source.id!r} was cleaned up "
                    f"(schema {source.schema} no longer exists) before its "
                    "snapshot could be read; no new round was opened"
                ) from exc
            raise ForkFailed(
                f"copying the snapshot of round {source.id!r} failed "
                f"({type(exc).__name__}, pgcode={getattr(exc, 'pgcode', None)}): "
                f"{str(exc).strip()}; partial data was removed and the slot "
                "was released"
            ) from exc
        except Exception as exc:
            self._drop_schema_quietly(schema)
            self._release_slot()
            raise ForkFailed(
                f"copying the snapshot of round {source.id!r} failed "
                f"({type(exc).__name__}): {exc}; partial data was removed and "
                "the slot was released"
            ) from exc
        return self._register(schema, source.id, copied)

    def get_round(self, round_id: str) -> Round | None:
        with self.lock:
            return self.rounds.get(round_id)

    def list_rounds(self) -> list[Round]:
        with self.lock:
            return sorted(self.rounds.values(), key=lambda r: r.created_at)

    def shutdown(self) -> None:
        for rnd in self.list_rounds():
            rnd.stop("shutdown")
        deadline = time.monotonic() + self.step_timeout + 10
        while time.monotonic() < deadline:
            if all(r.state == "finished" for r in self.list_rounds()):
                break
            time.sleep(0.1)
        self._halt.set()
        self._monitor.join(timeout=5)

    # ---------------- schema / baseline ----------------
    def _admin_connect(self):
        conn = psycopg2.connect(self.dsn)
        conn.autocommit = True
        return conn

    def _create_schema(self, schema: str) -> None:
        conn = self._admin_connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA "{schema}"')
                cur.execute(
                    f'CREATE TABLE "{schema}".batches('
                    "id INT PRIMARY KEY, ready BOOLEAN NOT NULL)")
                cur.execute(
                    f'CREATE TABLE "{schema}".readings('
                    "id INT PRIMARY KEY, "
                    f'batch_id INT NOT NULL REFERENCES "{schema}".batches(id), '
                    "value NUMERIC NOT NULL)")
                cur.executemany(
                    f'INSERT INTO "{schema}".batches(id, ready) VALUES (%s, %s)',
                    BASELINE_BATCHES)
                cur.executemany(
                    f'INSERT INTO "{schema}".readings(id, batch_id, value)'
                    " VALUES (%s, %s, %s)",
                    BASELINE_READINGS)
        finally:
            conn.close()

    def drop_schema(self, schema: str) -> None:
        conn = self._admin_connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            conn.close()

    def _drop_schema_quietly(self, schema: str) -> None:
        try:
            self.drop_schema(schema)
        except Exception:
            pass  # best effort; the schema may never have been created

    def _schema_exists(self, schema: str) -> bool:
        try:
            conn = self._admin_connect()
        except Exception:
            return True  # cannot tell; report the original copy failure instead
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.schemata "
                    "WHERE schema_name = %s", (schema,))
                return cur.fetchone() is not None
        finally:
            conn.close()

    def _copy_snapshot(self, source_schema: str, target_schema: str) -> dict:
        """Copy ``batches`` + ``readings`` from source to target in one
        dedicated REPEATABLE READ transaction.

        Both tables are therefore read from the SAME committed snapshot, so
        the new round never inherits a half-applied batch.  The transaction
        also creates the target schema and tables, so a failure aborts and
        leaves nothing behind (and the source is only ever read).
        """
        conn = psycopg2.connect(self.dsn)
        try:
            conn.autocommit = False
            with conn.cursor() as cur:
                # one snapshot for both SELECTs -> both tables copied from
                # the very same committed state of the source round
                cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                cur.execute(f'CREATE SCHEMA "{target_schema}"')
                # same key constraints as the baseline tables
                cur.execute(
                    f'CREATE TABLE "{target_schema}".batches('
                    "id INT PRIMARY KEY, ready BOOLEAN NOT NULL)")
                cur.execute(
                    f'CREATE TABLE "{target_schema}".readings('
                    "id INT PRIMARY KEY, "
                    f'batch_id INT NOT NULL REFERENCES "{target_schema}".batches(id), '
                    "value NUMERIC NOT NULL)")
                cur.execute(
                    f'INSERT INTO "{target_schema}".batches(id, ready) '
                    f'SELECT id, ready FROM "{source_schema}".batches ORDER BY id')
                n_batches = cur.rowcount
                cur.execute(
                    f'INSERT INTO "{target_schema}".readings(id, batch_id, value) '
                    f'SELECT id, batch_id, value FROM "{source_schema}".readings '
                    "ORDER BY id")
                n_readings = cur.rowcount
            conn.commit()
            return {"batches": n_batches, "readings": n_readings}
        finally:
            conn.close()

    # ---------------- monitor: 10s step timeout + terminate fallback ----------------
    def _monitor_loop(self) -> None:
        while not self._halt.wait(0.25):
            now = time.monotonic()
            for rnd in self.list_rounds():
                if rnd.state == "active":
                    self._check_step_timeout(rnd, now)
                elif rnd.state == "stopping":
                    self._check_stuck_round(rnd, now)
                    rnd._maybe_finish()  # retries a failed cleanup

    def _check_step_timeout(self, rnd: Round, now: float) -> None:
        with rnd.lock:
            if rnd.state != "active":
                return
            for rec in rnd.records:
                mono = rec.get("_mono")
                if rec["status"] == "running" and mono is not None \
                        and now - mono > self.step_timeout:
                    break
            else:
                return
        rnd.stop("timeout")

    def _check_stuck_round(self, rnd: Round, now: float) -> None:
        # Safety net: if a connection ignores cancel(), force-terminate its
        # backend so a failed round never leaks locks into the next round.
        with rnd.lock:
            if rnd.stopping_since is None or now - rnd.stopping_since < 15:
                return
            pids = [w.backend_pid for s in rnd.txs.values()
                    if (w := s["worker"]) is not None and w.is_alive()
                    and w.backend_pid]
        if not pids:
            return
        try:
            conn = self._admin_connect()
            try:
                with conn.cursor() as cur:
                    for pid in pids:
                        cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
            finally:
                conn.close()
        except Exception:
            pass
