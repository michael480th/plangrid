"""PlanGrid mailroom: an MCP server in front of a Postgres filing cabinet.
Tools: list_options, get_grid, save_cells, get_history. Every save is checked for locks and
for clashes with someone else's change, then written as one commit. A database trigger logs every cell change."""
import os
from pathlib import Path
import psycopg
from psycopg.rows import dict_row

DB = os.environ["DATABASE_URL"]
LOCKED = {s.strip() for s in os.environ.get("PLANGRID_LOCKED", "Actuals,Agent 2+10").split(",") if s.strip()}
LOCK_BEFORE = int(os.environ.get("PLANGRID_LOCK_BEFORE", "0") or 0) or None
DEFAULT_MEASURE = "M99"


def conn():
    return psycopg.connect(DB, row_factory=dict_row, autocommit=False)


def init_db():
    with conn() as c:
        c.execute(Path(__file__).with_name("schema.sql").read_text())
    seed = Path(__file__).with_name("seed.csv.gz")
    with conn() as c:
        if seed.exists() and c.execute("SELECT count(*) AS n FROM plan_cells").fetchone()["n"] == 0:
            import gzip
            cid = c.execute("INSERT INTO plan_commits(scenario,markets,cell_count,net_change,comment,updated_by) "
                            "VALUES ('Forecast,Agent 2+10,Plan,Actuals','ALL',0,0,'Seed: depletion demo data loaded from Aimplan model, Nov 20 2026','Setup') RETURNING commit_id").fetchone()["commit_id"]
            c.execute("ALTER TABLE plan_cells DISABLE TRIGGER plan_cells_audit")  # skip per-row audit for the initial load
            with c.cursor().copy("COPY plan_cells(scenario,measure,market,item,period,value) FROM STDIN WITH (FORMAT csv, HEADER true)") as cp:
                cp.write(gzip.open(seed, "rb").read())
            n = c.execute("UPDATE plan_cells SET commit_id=%s, updated_by='Setup' WHERE commit_id IS NULL", (cid,)).rowcount
            c.execute("UPDATE plan_commits SET cell_count=%s, net_change=(SELECT coalesce(sum(value),0) FROM plan_cells) WHERE commit_id=%s", (n, cid))
            c.execute("ALTER TABLE plan_cells ENABLE TRIGGER plan_cells_audit")


def settings():
    with conn() as c:
        rows = {r["key"]: r["value"] for r in c.execute("SELECT key,value FROM plan_settings")}
    locked = set(filter(None, rows["locked"].split(","))) if "locked" in rows else set(LOCKED)
    lb = rows.get("lock_before", str(LOCK_BEFORE or ""))
    return locked, (int(lb) if lb else None)


def is_locked(scenario, period, st=None):
    locked, lock_before = st or settings()
    if scenario in locked:
        return f"Scenario {scenario} is read-only"
    if lock_before and int(period) < lock_before:
        return f"Period {period} is closed"
    return None


def _same(a, b):
    if a is None or b is None:
        return (a or 0) == 0 and (b or 0) == 0
    return abs(float(a) - float(b)) < 1e-6


def list_options():
    with conn() as c:
        sc = [r["scenario"] for r in c.execute("SELECT DISTINCT scenario FROM plan_cells ORDER BY 1")]
        mk = [r["market"] for r in c.execute("SELECT DISTINCT market FROM plan_cells ORDER BY 1")]
        pe = [r["period"] for r in c.execute("SELECT DISTINCT period FROM plan_cells ORDER BY 1")]
    locked, lb = settings()
    return {"scenarios": sc, "markets": mk, "periods": pe, "measures": [DEFAULT_MEASURE],
            "lockedScenarios": sorted(locked), "lockBefore": lb, "store": "cloud database", "user": None}


def get_grid(scenario, market, compare=None, measure=DEFAULT_MEASURE):
    scen = [scenario] + ([compare] if compare and compare != scenario else [])
    with conn() as c:
        rows = c.execute("SELECT scenario,item,period,value FROM plan_cells WHERE market=%s AND measure=%s AND scenario = ANY(%s)",
                         (market, measure, scen)).fetchall()
        now = c.execute("SELECT now() AS t").fetchone()["t"]
    cells, comp, periods, items = {}, {}, set(), set()
    for r in rows:
        k = f'{r["item"]}|{r["period"]}'
        periods.add(r["period"]); items.add(r["item"])
        (cells if r["scenario"] == scenario else comp)[k] = r["value"]
    return {"scenario": scenario, "compare": compare, "market": market, "measure": measure,
            "periods": sorted(periods), "items": sorted(items), "cells": cells, "compareCells": comp,
            "readOnly": scenario in settings()[0], "lockBefore": settings()[1], "readAt": now.isoformat()}


def save_cells(scenario, changes, comment="", user="Plan grid", measure=DEFAULT_MEASURE):
    if not changes:
        return {"saved": 0, "conflicts": [], "locked": []}
    st = settings()
    locked = [{**c, "reason": is_locked(scenario, c["period"], st)} for c in changes if is_locked(scenario, c["period"], st)]
    if locked:
        return {"saved": 0, "conflicts": [], "locked": locked}
    user = (user or "Plan grid")[:200]
    with conn() as c:
        keys = [(c_["market"], c_["item"], int(c_["period"])) for c_ in changes]
        cur = {(r["market"], r["item"], r["period"]): r["value"] for r in c.execute(
            "SELECT market,item,period,value FROM plan_cells WHERE scenario=%s AND measure=%s "
            "AND (market,item,period) IN (SELECT * FROM unnest(%s::text[],%s::text[],%s::int[])) FOR UPDATE",
            (scenario, measure, [k[0] for k in keys], [k[1] for k in keys], [k[2] for k in keys]))}
        conflicts = [{**c_, "current": cur.get(k)} for c_, k in zip(changes, keys) if not _same(cur.get(k), c_.get("expected"))]
        if conflicts:
            c.rollback()
            return {"saved": 0, "conflicts": conflicts, "locked": []}
        net = sum((float(c_["value"]) if c_["value"] is not None else 0) - (cur.get(k) or 0) for c_, k in zip(changes, keys))
        cid = c.execute("INSERT INTO plan_commits(scenario,markets,cell_count,net_change,comment,updated_by) "
                        "VALUES (%s,%s,%s,%s,%s,%s) RETURNING commit_id,updated_at",
                        (scenario, ",".join(sorted({k[0] for k in keys})), len(changes), net, (comment or "")[:1000], user)).fetchone()
        with c.cursor() as cu:
            cu.executemany(
                "INSERT INTO plan_cells(scenario,measure,market,item,period,value,commit_id,updated_by,updated_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,now()) ON CONFLICT (scenario,measure,market,item,period) "
                "DO UPDATE SET value=EXCLUDED.value, commit_id=EXCLUDED.commit_id, updated_by=EXCLUDED.updated_by, updated_at=now()",
                [(scenario, measure, k[0], k[1], k[2], c_["value"], cid["commit_id"], user) for c_, k in zip(changes, keys)])
    return {"saved": len(changes), "commitId": cid["commit_id"], "netChange": net,
            "updatedAt": cid["updated_at"].isoformat(), "updatedBy": user, "conflicts": [], "locked": []}


def get_history(scenario=None, market=None, limit=20):
    q, a = "SELECT commit_id AS \"CommitId\", scenario AS \"ScenarioKey\", markets AS \"Markets\", cell_count AS \"CellCount\", " \
           "net_change AS \"NetChange\", comment AS \"Comment\", updated_by AS \"UpdatedBy\", updated_at AS \"UpdatedAt\" FROM plan_commits WHERE true", []
    if scenario:
        q += " AND (scenario=%s OR scenario LIKE %s)"; a += [scenario, f"%{scenario}%"]
    if market:
        q += " AND (markets='ALL' OR %s = ANY(string_to_array(markets, ',')))"; a.append(market)
    q += " ORDER BY commit_id DESC LIMIT %s"; a.append(max(1, min(int(limit), 200)))
    with conn() as c:
        rows = c.execute(q, a).fetchall()
    for r in rows:
        r["UpdatedAt"] = r["UpdatedAt"].isoformat()
    return {"commits": rows}


# ---------- admin ----------
def _admin_log(c, action, detail, user):
    c.execute("INSERT INTO plan_admin_log(action,detail,updated_by) VALUES (%s,%s,%s)", (action, detail[:2000], (user or "Admin")[:200]))


def admin_overview():
    locked, lb = settings()
    with conn() as c:
        sc = c.execute("SELECT scenario, count(*) AS cells, coalesce(sum(value),0) AS total, count(DISTINCT market) AS markets, "
                       "min(period) AS first_period, max(period) AS last_period, max(updated_at) AS last_change FROM plan_cells GROUP BY scenario ORDER BY scenario").fetchall()
        stats = c.execute("SELECT (SELECT count(*) FROM plan_commits) AS saves, (SELECT count(*) FROM plan_log) AS cell_changes").fetchone()
        alog = c.execute("SELECT action,detail,updated_by,updated_at FROM plan_admin_log ORDER BY log_id DESC LIMIT 20").fetchall()
    for r in sc:
        r["locked"] = r["scenario"] in locked
        r["last_change"] = r["last_change"].isoformat() if r["last_change"] else None
    for r in alog:
        r["updated_at"] = r["updated_at"].isoformat()
    return {"scenarios": sc, "lockBefore": lb, "saves": stats["saves"], "cellChanges": stats["cell_changes"], "adminLog": alog}


def set_locks(locked_scenarios, lock_before=None, user="Admin"):
    locked = sorted({x.strip() for x in locked_scenarios if x and x.strip()})
    lb = str(int(lock_before)) if lock_before else ""
    with conn() as c:
        for k, v in (("locked", ",".join(locked)), ("lock_before", lb)):
            c.execute("INSERT INTO plan_settings(key,value,updated_by) VALUES (%s,%s,%s) ON CONFLICT (key) DO UPDATE "
                      "SET value=EXCLUDED.value, updated_by=EXCLUDED.updated_by, updated_at=now()", (k, v, user))
        _admin_log(c, "Locks changed", f"Read-only: {', '.join(locked) or 'none'}. Closed before: {lb or 'none'}", user)
    return {"lockedScenarios": locked, "lockBefore": int(lb) if lb else None}


def copy_scenario(source, target, comment="", user="Admin"):
    target = (target or "").strip()
    if not target or target == source:
        return {"copied": 0, "error": "Give the new version a name different from the source"}
    with conn() as c:
        if c.execute("SELECT 1 FROM plan_cells WHERE scenario=%s LIMIT 1", (target,)).fetchone():
            return {"copied": 0, "error": f"A version called {target} already exists"}
        n = c.execute("SELECT count(*) AS n FROM plan_cells WHERE scenario=%s", (source,)).fetchone()["n"]
        if not n:
            return {"copied": 0, "error": f"No version called {source}"}
        cid = c.execute("INSERT INTO plan_commits(scenario,markets,cell_count,net_change,comment,updated_by) "
                        "SELECT %s,'ALL',count(*),coalesce(sum(value),0),%s,%s FROM plan_cells WHERE scenario=%s RETURNING commit_id",
                        (target, (comment or f"Copied from {source}")[:1000], user, source)).fetchone()["commit_id"]
        c.execute("INSERT INTO plan_cells(scenario,measure,market,item,period,value,commit_id,updated_by) "
                  "SELECT %s,measure,market,item,period,value,%s,%s FROM plan_cells WHERE scenario=%s", (target, cid, user, source))
        _admin_log(c, "Version copied", f"{source} to {target}, {n} cells", user)
    return {"copied": n, "commitId": cid, "target": target}


def get_commit(commit_id, limit=500):
    with conn() as c:
        head = c.execute("SELECT commit_id,scenario,markets,cell_count,net_change,comment,updated_by,updated_at FROM plan_commits WHERE commit_id=%s", (commit_id,)).fetchone()
        if not head:
            return {"error": "No such save"}
        rows = c.execute("SELECT market,item,period,old_value,new_value,updated_by,updated_at FROM plan_log WHERE commit_id=%s ORDER BY market,item,period LIMIT %s",
                         (commit_id, max(1, min(int(limit), 5000)))).fetchall()
    head["updated_at"] = head["updated_at"].isoformat()
    for r in rows:
        r["updated_at"] = r["updated_at"].isoformat()
    return {"commit": head, "changes": rows, "truncated": len(rows) >= limit}


def revert_commit(commit_id, user="Admin"):
    """Puts back the old values of one save, as a new save. Refuses if any of those cells changed since."""
    with conn() as c:
        head = c.execute("SELECT scenario FROM plan_commits WHERE commit_id=%s", (commit_id,)).fetchone()
        rows = c.execute("SELECT market,item,period,measure,old_value,new_value FROM plan_log WHERE commit_id=%s", (commit_id,)).fetchall()
    if not head or not rows:
        return {"saved": 0, "error": "Nothing to reverse: this save has no cell-level history (the initial load and copies cannot be reversed)"}
    changes = [{"market": r["market"], "item": r["item"], "period": r["period"], "value": r["old_value"], "expected": r["new_value"]} for r in rows]
    res = save_cells(head["scenario"], changes, f"Reversal of save {commit_id}", user, rows[0]["measure"])
    if res.get("saved"):
        with conn() as c:
            _admin_log(c, "Save reversed", f"Save {commit_id}, {res['saved']} cells, new save {res['commitId']}", user)
    return res


def build_server(**kw):
    from mcp.server.fastmcp import FastMCP
    from mcp.types import ToolAnnotations
    mcp = FastMCP("plangrid", **kw)
    ro, rw = ToolAnnotations(readOnlyHint=True), ToolAnnotations(readOnlyHint=False, destructiveHint=False)

    @mcp.tool(name="list_options", annotations=ro, description="Scenarios, markets and periods in the plan, plus locks.")
    def _a() -> dict: return list_options()

    @mcp.tool(name="get_grid", annotations=ro, description="One market's item x period values for a scenario, with an optional comparison scenario. Cells keyed 'item|period'.")
    def _b(scenario: str, market: str, compare: str | None = None, measure: str = DEFAULT_MEASURE) -> dict:
        return get_grid(scenario, market, compare, measure)

    @mcp.tool(name="save_cells", annotations=rw, description="Save changed cells as one commit. Each change: {market, item, period, value, expected}. "
              "Nothing is saved if any stored value differs from 'expected' or a scenario or period is locked. 'user' is who made the change.")
    def _c(scenario: str, changes: list[dict], comment: str = "", user: str = "Plan grid", measure: str = DEFAULT_MEASURE) -> dict:
        return save_cells(scenario, changes, comment, user, measure)

    @mcp.tool(name="get_history", annotations=ro, description="Recent commits, newest first: who, when, comment, cells, net change.")
    def _d(scenario: str | None = None, market: str | None = None, limit: int = 20) -> dict:
        return get_history(scenario, market, limit)
    @mcp.tool(name="admin_overview", annotations=ro, description="Admin view: each version with cells, total, markets, lock status; closed-period setting; counts; recent admin actions.")
    def _e() -> dict: return admin_overview()

    @mcp.tool(name="set_locks", annotations=rw, description="Set which versions are read-only and the first open month (DateIntKey, e.g. 20260301). Replaces the current settings.")
    def _f(locked_scenarios: list[str], lock_before: int | None = None, user: str = "Admin") -> dict:
        return set_locks(locked_scenarios, lock_before, user)

    @mcp.tool(name="copy_scenario", annotations=rw, description="Copy every cell of one version into a new version name. Refuses if the target exists.")
    def _g(source: str, target: str, comment: str = "", user: str = "Admin") -> dict:
        return copy_scenario(source, target, comment, user)

    @mcp.tool(name="get_commit", annotations=ro, description="One save with every cell change: market, item, period, old value, new value.")
    def _h(commit_id: int, limit: int = 500) -> dict: return get_commit(commit_id, limit)

    @mcp.tool(name="revert_commit", annotations=rw, description="Reverse one save by writing back its old values as a new save. Refuses if any of those cells changed since.")
    def _i(commit_id: int, user: str = "Admin") -> dict: return revert_commit(commit_id, user)
    return mcp
