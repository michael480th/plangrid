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


def is_locked(scenario, period):
    if scenario in LOCKED:
        return f"Scenario {scenario} is read-only"
    if LOCK_BEFORE and int(period) < LOCK_BEFORE:
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
    return {"scenarios": sc, "markets": mk, "periods": pe, "measures": [DEFAULT_MEASURE],
            "lockedScenarios": sorted(LOCKED), "lockBefore": LOCK_BEFORE, "store": "cloud database", "user": None}


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
            "readOnly": scenario in LOCKED, "lockBefore": LOCK_BEFORE, "readAt": now.isoformat()}


def save_cells(scenario, changes, comment="", user="Plan grid", measure=DEFAULT_MEASURE):
    if not changes:
        return {"saved": 0, "conflicts": [], "locked": []}
    locked = [{**c, "reason": is_locked(scenario, c["period"])} for c in changes if is_locked(scenario, c["period"])]
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
    return mcp
