"""PlanGrid save service: an MCP server in front of a Postgres database.

Planning workflow per market and plan version:
  10 Agent Draft -> 20 Planner Submitted -> 30 FP&A Reviewed -> 40 CFO Approved
Planners edit their own markets while a market is at 10. FP&A can adjust up to 20.
Every save is checked for closed versions, closed months, workflow status and clashing edits,
then written as one transaction. A database trigger logs every cell change.
"""
import json
import os
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

DB = os.environ["DATABASE_URL"]
LOCK_BEFORE_DEFAULT = int(os.environ.get("PLANGRID_LOCK_BEFORE", "0") or 0) or None
DEFAULT_MEASURE = "M99"
PLAN_YEAR = 2026
WORKFLOW_SCENARIO = "Forecast"
STATUS = {10: "Agent Draft", 20: "Planner Submitted", 30: "FP&A Reviewed", 40: "CFO Approved"}
ROLES = ["Planner", "FP&A", "CFO", "Admin"]
LY = "Last year actuals"  # virtual comparison: Actuals shifted forward one year
HERE = Path(__file__).parent


def conn():
    return psycopg.connect(DB, row_factory=dict_row, autocommit=False)


def _iso(r, *keys):
    for k in keys:
        if r.get(k) is not None and hasattr(r[k], "isoformat"):
            r[k] = r[k].isoformat()
    return r


# ---------------------------------------------------------------- setup
def init_db():
    with conn() as c:
        c.execute((HERE / "schema.sql").read_text())
    seed = HERE / "seed.csv.gz"
    with conn() as c:
        if seed.exists() and c.execute("SELECT count(*) AS n FROM plan_cells").fetchone()["n"] == 0:
            import gzip
            cid = c.execute("INSERT INTO plan_commits(scenario,markets,cell_count,net_change,comment,updated_by) VALUES "
                            "('Forecast,Agent 2+10,Plan,Actuals','ALL',0,0,'Initial load: depletion demo data from the Aimplan model','Setup') "
                            "RETURNING commit_id").fetchone()["commit_id"]
            c.execute("ALTER TABLE plan_cells DISABLE TRIGGER plan_cells_audit")
            with c.cursor().copy("COPY plan_cells(scenario,measure,market,item,period,value) FROM STDIN WITH (FORMAT csv, HEADER true)") as cp:
                cp.write(gzip.open(seed, "rb").read())
            n = c.execute("UPDATE plan_cells SET commit_id=%s WHERE commit_id IS NULL", (cid,)).rowcount
            c.execute("UPDATE plan_commits SET cell_count=%s WHERE commit_id=%s", (n, cid))
            c.execute("ALTER TABLE plan_cells ENABLE TRIGGER plan_cells_audit")
    # actuals added after the first load (2025 for last-year comparisons)
    with conn() as c:
        if seed.exists() and not c.execute("SELECT 1 FROM plan_cells WHERE scenario='Actuals' AND period < 20260101 LIMIT 1").fetchone():
            import gzip, csv, io
            rows = [r for r in csv.DictReader(io.StringIO(gzip.open(seed, "rt").read())) if int(r["DateIntKey"]) < 20260101]
            if rows:
                cid = c.execute("INSERT INTO plan_commits(scenario,markets,cell_count,net_change,comment,updated_by) VALUES "
                                "('Actuals','ALL',%s,%s,'Load 2025 actuals for last-year comparisons','Setup') RETURNING commit_id",
                                (len(rows), sum(float(r["Value"]) for r in rows))).fetchone()["commit_id"]
                c.execute("ALTER TABLE plan_cells DISABLE TRIGGER plan_cells_audit")
                with c.cursor() as cu:
                    cu.executemany("INSERT INTO plan_cells(scenario,measure,market,item,period,value,commit_id) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                                   [(r["ScenarioKey"], r["MeasureKey"], r["market_key"], r["item_key"], int(r["DateIntKey"]), float(r["Value"]), cid) for r in rows])
                c.execute("ALTER TABLE plan_cells ENABLE TRIGGER plan_cells_audit")
    extra = HERE / "seed_extra.json"
    with conn() as c:
        if not c.execute("SELECT 1 FROM plan_scenarios LIMIT 1").fetchone():
            for s, st, kind, desc, sort in [
                ("Forecast", "Open", "Plan", "Planners' plan of record, 2+10", 1),
                ("Agent 2+10", "Closed", "Agent", "Agent draft. Written only by the agent", 2),
                ("Plan", "Closed", "Plan", "Annual plan", 3),
                ("Actuals", "Closed", "Actual", "Depletion actuals", 4)]:
                c.execute("INSERT INTO plan_scenarios(scenario,status,kind,description,sort) VALUES (%s,%s,%s,%s,%s)", (s, st, kind, desc, sort))
        for s in [r["scenario"] for r in c.execute("SELECT DISTINCT scenario FROM plan_cells WHERE scenario NOT IN (SELECT scenario FROM plan_scenarios)")]:
            c.execute("INSERT INTO plan_scenarios(scenario,status) VALUES (%s,'Closed')", (s,))
        if not c.execute("SELECT 1 FROM plan_settings WHERE key='lock_before'").fetchone():
            c.execute("INSERT INTO plan_settings(key,value) VALUES ('lock_before',%s)", (str(LOCK_BEFORE_DEFAULT or 20260301),))
        if extra.exists() and not c.execute("SELECT 1 FROM plan_markets LIMIT 1").fetchone():
            x = json.loads(extra.read_text())
            for m in x["markets"]:
                c.execute("INSERT INTO plan_markets VALUES (%s,%s,%s,%s,%s,%s)",
                          (m["market"], m["region"], m["business_unit"], m["regional_manager"], m["bu_manager"], m["channel"]))
            by_mgr = {}
            for m in x["markets"]:
                by_mgr.setdefault(m["regional_manager"], []).append(m["market"])
            for name, mk in sorted(by_mgr.items()):
                c.execute("INSERT INTO plan_users(name,role,markets) VALUES (%s,'Planner',%s) ON CONFLICT (name) DO NOTHING", (name, ",".join(sorted(mk))))
            for name, role in [("Michael Parrott", "Admin"), ("Kevin Malone", "FP&A"), ("David Wallace", "CFO")]:
                c.execute("INSERT INTO plan_users(name,role,markets) VALUES (%s,%s,'ALL') ON CONFLICT (name) DO UPDATE SET role=EXCLUDED.role, markets=EXCLUDED.markets",
                          (name, role))
            for cm in x["comments"]:
                agent = cm["scenario"] == "Agent 2+10"
                c.execute("INSERT INTO plan_comments(scenario,market,author,role,kind,text,created_at) VALUES ('Forecast',%s,%s,%s,%s,%s,%s)",
                          (cm["market"], "Agent" if agent else "Planner", "Agent" if agent else "Planner", "agent" if agent else "note", cm["text"], cm["at"]))
            c.execute("INSERT INTO plan_status(scenario,market,status) SELECT 'Forecast', market, 10 FROM plan_markets ON CONFLICT DO NOTHING")


# ---------------------------------------------------------------- lookups
def _lock_before(c):
    r = c.execute("SELECT value FROM plan_settings WHERE key='lock_before'").fetchone()
    return int(r["value"]) if r and r["value"] else None


def _scenarios(c):
    return {r["scenario"]: r for r in c.execute("SELECT scenario,status,kind,description,sort FROM plan_scenarios ORDER BY sort, scenario")}


def _user(c, user_id):
    if user_id in (None, "", 0):
        return None
    return c.execute("SELECT user_id,name,role,markets,active FROM plan_users WHERE user_id=%s", (int(user_id),)).fetchone()


def _covers(u, market):
    return u and (u["markets"] == "ALL" or market in u["markets"].split(","))


def _status(c, scenario, market):
    r = c.execute("SELECT status FROM plan_status WHERE scenario=%s AND market=%s", (scenario, market)).fetchone()
    return r["status"] if r else None


def can_edit(c, u, scenario, market, sc=None):
    """Returns None when allowed, else the reason."""
    sc = sc or _scenarios(c)
    s = sc.get(scenario)
    if not s or s["status"] != "Open":
        return f"{scenario} is closed for editing"
    if u is None:
        return "Choose who you are before editing"
    if u["role"] == "Admin":
        return None
    if not _covers(u, market):
        return f"{market} is not one of your markets"
    st = _status(c, scenario, market)
    if st is None:
        return None
    if u["role"] == "Planner" and st != 10:
        return f"{market} is {STATUS.get(st, st)}. Ask FP&A to send it back to edit"
    if u["role"] == "FP&A" and st > 20:
        return f"{market} is {STATUS.get(st, st)}"
    if u["role"] == "CFO":
        return "The CFO approves or sends back, and does not edit numbers"
    return None


# ---------------------------------------------------------------- read
def list_options():
    with conn() as c:
        sc = _scenarios(c)
        mk = c.execute("SELECT * FROM plan_markets ORDER BY market").fetchall()
        if not mk:
            mk = [{"market": r["market"]} for r in c.execute("SELECT DISTINCT market FROM plan_cells ORDER BY 1")]
        users = c.execute("SELECT user_id,name,role,markets FROM plan_users WHERE active ORDER BY role, name").fetchall()
        lb = _lock_before(c)
    return {"scenarios": list(sc.values()), "markets": mk, "users": users, "statuses": STATUS, "roles": ROLES,
            "lockBefore": lb, "planYear": PLAN_YEAR, "workflowScenario": WORKFLOW_SCENARIO, "lastYear": LY,
            "lockedScenarios": [k for k, v in sc.items() if v["status"] != "Open"]}


def get_board(scenario=WORKFLOW_SCENARIO, compare="Agent 2+10"):
    y0, y1 = PLAN_YEAR * 10000, (PLAN_YEAR + 1) * 10000
    with conn() as c:
        tot = c.execute(
            "SELECT market, sum(value) FILTER (WHERE scenario=%s AND period>=%s AND period<%s) AS fc, "
            "sum(value) FILTER (WHERE scenario=%s AND period>=%s AND period<%s) AS cmp, "
            "sum(value) FILTER (WHERE scenario='Actuals' AND period>=%s AND period<%s) AS ly, "
            "sum(value) FILTER (WHERE scenario='Actuals' AND period>=%s AND period<%s) AS ytd "
            "FROM plan_cells GROUP BY market", (scenario, y0, y1, compare, y0, y1, y0 - 10000, y0, y0, y1)).fetchall()
        st = {r["market"]: r for r in c.execute("SELECT market,status,updated_by,updated_at FROM plan_status WHERE scenario=%s", (scenario,))}
        mk = {r["market"]: r for r in c.execute("SELECT * FROM plan_markets")}
        cm = {r["market"]: r for r in c.execute(
            "SELECT DISTINCT ON (market) market, author, kind, text, created_at FROM plan_comments WHERE scenario=%s AND kind<>'agent' "
            "ORDER BY market, created_at DESC", (scenario,))}
        ncm = {r["market"]: r["n"] for r in c.execute("SELECT market, count(*) AS n FROM plan_comments WHERE scenario=%s GROUP BY market", (scenario,))}
    rows = []
    for t in tot:
        m = t["market"]
        s = st.get(m, {})
        rows.append({**(mk.get(m) or {"market": m}), "forecast": t["fc"] or 0, "compare": t["cmp"] or 0, "lastYear": t["ly"] or 0, "ytdActual": t["ytd"] or 0,
                     "status": s.get("status"), "statusName": STATUS.get(s.get("status")), "statusBy": s.get("updated_by"),
                     "statusAt": s["updated_at"].isoformat() if s.get("updated_at") else None,
                     "lastNote": _iso(dict(cm[m]), "created_at") if m in cm else None, "comments": ncm.get(m, 0)})
    rows.sort(key=lambda r: r["market"])
    return {"scenario": scenario, "compare": compare, "markets": rows}


def get_grid(scenario, market, compare=None, measure=DEFAULT_MEASURE, year=PLAN_YEAR, user_id=None):
    y0, y1 = int(year) * 10000, (int(year) + 1) * 10000
    with conn() as c:
        rows = c.execute("SELECT item,period,value FROM plan_cells WHERE market=%s AND measure=%s AND scenario=%s AND period>=%s AND period<%s",
                         (market, measure, scenario, y0, y1)).fetchall()
        if compare == LY:
            crows = c.execute("SELECT item,period+10000 AS period,value FROM plan_cells WHERE market=%s AND measure=%s AND scenario='Actuals' AND period>=%s AND period<%s",
                              (market, measure, y0 - 10000, y1 - 10000)).fetchall()
        elif compare and compare != scenario:
            crows = c.execute("SELECT item,period,value FROM plan_cells WHERE market=%s AND measure=%s AND scenario=%s AND period>=%s AND period<%s",
                              (market, measure, compare, y0, y1)).fetchall()
        else:
            crows = []
        actual = {f'{r["item"]}|{r["period"]}': r["value"] for r in c.execute(
            "SELECT item,period,value FROM plan_cells WHERE market=%s AND measure=%s AND scenario='Actuals' AND period>=%s AND period<%s",
            (market, measure, y0, y1))}
        u = _user(c, user_id)
        why = can_edit(c, u, scenario, market)
        lb = _lock_before(c)
        st = _status(c, scenario, market)
        now = c.execute("SELECT now() AS t").fetchone()["t"]
    cells = {f'{r["item"]}|{r["period"]}': r["value"] for r in rows}
    comp = {f'{r["item"]}|{r["period"]}': r["value"] for r in crows}
    periods = sorted({r["period"] for r in rows} | {r["period"] for r in crows} | {y0 + m * 100 + 1 for m in range(1, 13)})
    items = sorted({r["item"] for r in rows} | {r["item"] for r in crows})
    return {"scenario": scenario, "compare": compare, "market": market, "measure": measure, "year": int(year),
            "periods": periods, "items": items, "cells": cells, "compareCells": comp, "actuals": actual,
            "readOnly": why is not None, "readOnlyReason": why, "lockBefore": lb,
            "status": st, "statusName": STATUS.get(st), "readAt": now.isoformat()}


def get_market(scenario, market):
    with conn() as c:
        comments = [_iso(r, "created_at") for r in c.execute(
            "SELECT comment_id,author,role,kind,text,created_at FROM plan_comments WHERE scenario=%s AND market=%s ORDER BY created_at", (scenario, market))]
        log = [_iso(r, "updated_at") for r in c.execute(
            "SELECT old_status,new_status,action,note,updated_by,updated_at FROM plan_status_log WHERE scenario=%s AND market=%s ORDER BY log_id DESC LIMIT 30",
            (scenario, market))]
        st = _status(c, scenario, market)
        mk = c.execute("SELECT * FROM plan_markets WHERE market=%s", (market,)).fetchone()
    return {"market": mk or {"market": market}, "status": st, "statusName": STATUS.get(st), "comments": comments, "statusLog": log}


def get_history(scenario=None, market=None, limit=20):
    q, a = ("SELECT commit_id AS \"CommitId\", scenario AS \"ScenarioKey\", markets AS \"Markets\", cell_count AS \"CellCount\", "
            "net_change AS \"NetChange\", comment AS \"Comment\", updated_by AS \"UpdatedBy\", updated_at AS \"UpdatedAt\" FROM plan_commits WHERE true"), []
    if scenario:
        q += " AND (scenario=%s OR scenario LIKE %s)"; a += [scenario, f"%{scenario}%"]
    if market:
        q += " AND (markets='ALL' OR %s = ANY(string_to_array(markets, ',')))"; a.append(market)
    q += " ORDER BY commit_id DESC LIMIT %s"; a.append(max(1, min(int(limit), 200)))
    with conn() as c:
        rows = c.execute(q, a).fetchall()
    return {"commits": [_iso(r, "UpdatedAt") for r in rows]}


# ---------------------------------------------------------------- write
def _same(a, b):
    if a is None or b is None:
        return (a or 0) == 0 and (b or 0) == 0
    return abs(float(a) - float(b)) < 1e-6


def save_cells(scenario, changes, comment="", user_id=None, measure=DEFAULT_MEASURE, user=None, _admin_name=None):
    if not changes:
        return {"saved": 0, "conflicts": [], "locked": []}
    with conn() as c:
        u = _user(c, user_id)
        name = _admin_name or (u["name"] if u else None)
        if not name:
            return {"saved": 0, "conflicts": [], "locked": [{"reason": "Choose who you are before saving"}]}
        lb = _lock_before(c)
        sc = _scenarios(c)
        locked = []
        for ch in changes:
            reason = None
            if lb and int(ch["period"]) < lb:
                reason = f"Months before {str(lb)[:4]}-{str(lb)[4:6]} are closed"
            elif _admin_name is None:
                reason = can_edit(c, u, scenario, ch["market"], sc)
            elif scenario not in sc or sc[scenario]["status"] != "Open":
                reason = f"{scenario} is closed for editing"
            if reason:
                locked.append({**ch, "reason": reason})
        if locked:
            return {"saved": 0, "conflicts": [], "locked": locked}
        keys = [(ch["market"], ch["item"], int(ch["period"])) for ch in changes]
        cur = {(r["market"], r["item"], r["period"]): r["value"] for r in c.execute(
            "SELECT market,item,period,value FROM plan_cells WHERE scenario=%s AND measure=%s "
            "AND (market,item,period) IN (SELECT * FROM unnest(%s::text[],%s::text[],%s::int[])) FOR UPDATE",
            (scenario, measure, [k[0] for k in keys], [k[1] for k in keys], [k[2] for k in keys]))}
        conflicts = [{**ch, "current": cur.get(k)} for ch, k in zip(changes, keys) if not _same(cur.get(k), ch.get("expected"))]
        if conflicts:
            c.rollback()
            return {"saved": 0, "conflicts": conflicts, "locked": []}
        net = sum((float(ch["value"]) if ch["value"] is not None else 0) - (cur.get(k) or 0) for ch, k in zip(changes, keys))
        head = c.execute("INSERT INTO plan_commits(scenario,markets,cell_count,net_change,comment,updated_by) VALUES (%s,%s,%s,%s,%s,%s) "
                         "RETURNING commit_id,updated_at",
                         (scenario, ",".join(sorted({k[0] for k in keys})), len(changes), net, (comment or "")[:1000], name)).fetchone()
        with c.cursor() as cu:
            cu.executemany(
                "INSERT INTO plan_cells(scenario,measure,market,item,period,value,commit_id,updated_by,updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,now()) "
                "ON CONFLICT (scenario,measure,market,item,period) DO UPDATE SET value=EXCLUDED.value, commit_id=EXCLUDED.commit_id, "
                "updated_by=EXCLUDED.updated_by, updated_at=now()",
                [(scenario, measure, k[0], k[1], k[2], ch["value"], head["commit_id"], name) for ch, k in zip(changes, keys)])
    return {"saved": len(changes), "commitId": head["commit_id"], "netChange": net, "updatedAt": head["updated_at"].isoformat(),
            "updatedBy": name, "conflicts": [], "locked": []}


ACTIONS = {
    # action: (allowed roles, from statuses, to status)
    "submit": (("Planner", "Admin"), (10,), 20),
    "review": (("FP&A", "Admin"), (20,), 30),
    "approve": (("CFO", "Admin"), (30,), 40),
    "send_back": (("FP&A", "CFO", "Admin"), (20, 30, 40), 10),
    "reopen_review": (("CFO", "Admin"), (40,), 30),
}


def set_status(scenario, market, action, user_id, note=""):
    if action not in ACTIONS:
        return {"ok": False, "error": f"Unknown action {action}"}
    roles, froms, to = ACTIONS[action]
    with conn() as c:
        u = _user(c, user_id)
        if not u:
            return {"ok": False, "error": "Choose who you are first"}
        if u["role"] not in roles:
            return {"ok": False, "error": f"{u['role']} cannot {action.replace('_', ' ')}"}
        if u["role"] == "Planner" and not _covers(u, market):
            return {"ok": False, "error": f"{market} is not one of your markets"}
        if action == "send_back" and not (note or "").strip():
            return {"ok": False, "error": "Say what needs to change when sending a market back"}
        cur = _status(c, scenario, market) or 10
        if cur not in froms:
            verb = {"submit": "submitted", "review": "reviewed", "approve": "approved", "send_back": "sent back", "reopen_review": "reopened"}[action]
            return {"ok": False, "error": f"{market} is {STATUS.get(cur, cur)}, so it cannot be {verb}"}
        c.execute("INSERT INTO plan_status(scenario,market,status,updated_by,updated_at) VALUES (%s,%s,%s,%s,now()) ON CONFLICT (scenario,market) "
                  "DO UPDATE SET status=EXCLUDED.status, updated_by=EXCLUDED.updated_by, updated_at=now()", (scenario, market, to, u["name"]))
        c.execute("INSERT INTO plan_status_log(scenario,market,old_status,new_status,action,note,updated_by) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                  (scenario, market, cur, to, action, (note or "")[:2000], u["name"]))
        label = {"submit": "Submitted", "review": "Reviewed", "approve": "Approved", "send_back": "Sent back", "reopen_review": "Reopened for review"}[action]
        c.execute("INSERT INTO plan_comments(scenario,market,author,role,kind,text) VALUES (%s,%s,%s,%s,'status',%s)",
                  (scenario, market, u["name"], u["role"], label + (f": {note.strip()}" if (note or "").strip() else "")))
    return {"ok": True, "market": market, "status": to, "statusName": STATUS[to]}


def add_comment(scenario, market, text, user_id):
    text = (text or "").strip()
    if not text:
        return {"ok": False, "error": "Empty comment"}
    with conn() as c:
        u = _user(c, user_id)
        if not u:
            return {"ok": False, "error": "Choose who you are first"}
        r = c.execute("INSERT INTO plan_comments(scenario,market,author,role,kind,text) VALUES (%s,%s,%s,%s,'note',%s) RETURNING comment_id, created_at",
                      (scenario, market, u["name"], u["role"], text[:4000])).fetchone()
    return {"ok": True, "commentId": r["comment_id"], "createdAt": r["created_at"].isoformat()}


# ---------------------------------------------------------------- admin
def _admin(c, user_id):
    u = _user(c, user_id)
    if not u or u["role"] != "Admin":
        return None
    return u


def _admin_log(c, action, detail, name):
    c.execute("INSERT INTO plan_admin_log(action,detail,updated_by) VALUES (%s,%s,%s)", (action, detail[:2000], name))


def admin_overview():
    y0 = PLAN_YEAR * 10000
    with conn() as c:
        sc = _scenarios(c)
        stats = {r["scenario"]: r for r in c.execute(
            "SELECT scenario, count(*) AS cells, coalesce(sum(value) FILTER (WHERE period>=%s AND period<%s),0) AS total, count(DISTINCT market) AS markets, "
            "min(period) AS first_period, max(period) AS last_period, max(updated_at) AS last_change FROM plan_cells GROUP BY scenario", (y0, y0 + 10000))}
        counts = c.execute("SELECT (SELECT count(*) FROM plan_commits) AS saves, (SELECT count(*) FROM plan_log) AS cell_changes, "
                           "(SELECT count(*) FROM plan_status_log) AS status_changes").fetchone()
        alog = [_iso(r, "updated_at") for r in c.execute("SELECT action,detail,updated_by,updated_at FROM plan_admin_log ORDER BY log_id DESC LIMIT 30")]
        slog = [_iso(r, "updated_at") for r in c.execute(
            "SELECT scenario,market,old_status,new_status,action,note,updated_by,updated_at FROM plan_status_log ORDER BY log_id DESC LIMIT 30")]
        users = [_iso(r, "updated_at") for r in c.execute("SELECT user_id,name,role,markets,active,updated_at FROM plan_users ORDER BY role,name")]
        board = {r["status"]: r["n"] for r in c.execute("SELECT status, count(*) AS n FROM plan_status WHERE scenario=%s GROUP BY status", (WORKFLOW_SCENARIO,))}
        lb = _lock_before(c)
        markets = [r["market"] for r in c.execute("SELECT market FROM plan_markets ORDER BY market")]
    out = []
    for k, v in sc.items():
        s = stats.get(k, {})
        out.append({**v, "cells": s.get("cells", 0), "total": s.get("total", 0), "markets": s.get("markets", 0),
                    "firstPeriod": s.get("first_period"), "lastPeriod": s.get("last_period"),
                    "lastChange": s["last_change"].isoformat() if s.get("last_change") else None})
    return {"scenarios": out, "lockBefore": lb, "planYear": PLAN_YEAR, **counts, "adminLog": alog, "statusLog": slog, "users": users,
            "workflow": [{"status": k, "name": n, "markets": board.get(k, 0)} for k, n in STATUS.items()], "statuses": STATUS,
            "roles": ROLES, "allMarkets": markets}


def set_scenario(scenario, status=None, description=None, user_id=None):
    with conn() as c:
        u = _admin(c, user_id)
        if not u:
            return {"ok": False, "error": "Only an Admin can change versions"}
        if not c.execute("SELECT 1 FROM plan_scenarios WHERE scenario=%s", (scenario,)).fetchone():
            return {"ok": False, "error": f"No version called {scenario}"}
        if status and status not in ("Open", "Closed"):
            return {"ok": False, "error": "Status must be Open or Closed"}
        c.execute("UPDATE plan_scenarios SET status=coalesce(%s,status), description=coalesce(%s,description), updated_by=%s, updated_at=now() WHERE scenario=%s",
                  (status, description, u["name"], scenario))
        _admin_log(c, "Version changed", f"{scenario}: " + ", ".join(x for x in [f"status {status}" if status else "", "description" if description is not None else ""] if x), u["name"])
    return {"ok": True}


def set_lock_before(lock_before, user_id=None):
    with conn() as c:
        u = _admin(c, user_id)
        if not u:
            return {"ok": False, "error": "Only an Admin can close months"}
        v = str(int(lock_before)) if lock_before else ""
        c.execute("INSERT INTO plan_settings(key,value,updated_by) VALUES ('lock_before',%s,%s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, "
                  "updated_by=EXCLUDED.updated_by, updated_at=now()", (v, u["name"]))
        _admin_log(c, "Closed months changed", f"Months before {v or 'none'} are closed", u["name"])
    return {"ok": True, "lockBefore": int(v) if v else None}


def copy_scenario(source, target, comment="", user_id=None, status="Closed"):
    target = (target or "").strip()
    with conn() as c:
        u = _admin(c, user_id)
        if not u:
            return {"copied": 0, "error": "Only an Admin can copy versions"}
        if not target or target == source or target == LY:
            return {"copied": 0, "error": "Give the new version a different name"}
        if c.execute("SELECT 1 FROM plan_scenarios WHERE scenario=%s", (target,)).fetchone() or c.execute("SELECT 1 FROM plan_cells WHERE scenario=%s LIMIT 1", (target,)).fetchone():
            return {"copied": 0, "error": f"A version called {target} already exists"}
        n = c.execute("SELECT count(*) AS n FROM plan_cells WHERE scenario=%s", (source,)).fetchone()["n"]
        if not n:
            return {"copied": 0, "error": f"No version called {source}"}
        cid = c.execute("INSERT INTO plan_commits(scenario,markets,cell_count,net_change,comment,updated_by) "
                        "SELECT %s,'ALL',count(*),coalesce(sum(value),0),%s,%s FROM plan_cells WHERE scenario=%s RETURNING commit_id",
                        (target, (comment or f"Copied from {source}")[:1000], u["name"], source)).fetchone()["commit_id"]
        c.execute("ALTER TABLE plan_cells DISABLE TRIGGER plan_cells_audit")
        c.execute("INSERT INTO plan_cells(scenario,measure,market,item,period,value,commit_id,updated_by) "
                  "SELECT %s,measure,market,item,period,value,%s,%s FROM plan_cells WHERE scenario=%s", (target, cid, u["name"], source))
        c.execute("ALTER TABLE plan_cells ENABLE TRIGGER plan_cells_audit")
        c.execute("INSERT INTO plan_scenarios(scenario,status,kind,description,sort,updated_by) VALUES (%s,%s,'Snapshot',%s,50,%s)",
                  (target, status if status in ("Open", "Closed") else "Closed", comment or f"Copy of {source}", u["name"]))
        _admin_log(c, "Version copied", f"{source} to {target}, {n} cells", u["name"])
    return {"copied": n, "commitId": cid, "target": target}


def upsert_user(name, role, markets, active=True, user_id=None, admin_user_id=None):
    if role not in ROLES:
        return {"ok": False, "error": f"Role must be one of {', '.join(ROLES)}"}
    name = (name or "").strip()
    if not name:
        return {"ok": False, "error": "Name is required"}
    mk = "ALL" if markets in ("ALL", ["ALL"]) else ",".join(sorted({m.strip() for m in (markets if isinstance(markets, list) else str(markets).split(",")) if m.strip()}))
    with conn() as c:
        a = _admin(c, admin_user_id)
        if not a:
            return {"ok": False, "error": "Only an Admin can change users"}
        if user_id:
            c.execute("UPDATE plan_users SET name=%s, role=%s, markets=%s, active=%s, updated_by=%s, updated_at=now() WHERE user_id=%s",
                      (name, role, mk, bool(active), a["name"], int(user_id)))
            uid = int(user_id)
        else:
            uid = c.execute("INSERT INTO plan_users(name,role,markets,active,updated_by) VALUES (%s,%s,%s,%s,%s) RETURNING user_id",
                            (name, role, mk, bool(active), a["name"])).fetchone()["user_id"]
        _admin_log(c, "User saved", f"{name}: {role}, markets {mk or 'none'}, {'active' if active else 'inactive'}", a["name"])
    return {"ok": True, "userId": uid}


def get_commit(commit_id, limit=500):
    with conn() as c:
        head = c.execute("SELECT commit_id,scenario,markets,cell_count,net_change,comment,updated_by,updated_at FROM plan_commits WHERE commit_id=%s",
                         (commit_id,)).fetchone()
        if not head:
            return {"error": "No such save"}
        rows = c.execute("SELECT market,item,period,old_value,new_value,updated_by,updated_at FROM plan_log WHERE commit_id=%s ORDER BY market,item,period LIMIT %s",
                         (commit_id, max(1, min(int(limit), 5000)))).fetchall()
    return {"commit": _iso(head, "updated_at"), "changes": [_iso(r, "updated_at") for r in rows], "truncated": len(rows) >= limit}


def revert_commit(commit_id, user_id=None):
    with conn() as c:
        u = _admin(c, user_id)
        if not u:
            return {"saved": 0, "error": "Only an Admin can reverse a save"}
        head = c.execute("SELECT scenario FROM plan_commits WHERE commit_id=%s", (commit_id,)).fetchone()
        rows = c.execute("SELECT market,item,period,measure,old_value,new_value FROM plan_log WHERE commit_id=%s", (commit_id,)).fetchall()
    if not head or not rows:
        return {"saved": 0, "error": "This save has no cell-level history (loads and copies cannot be reversed)"}
    changes = [{"market": r["market"], "item": r["item"], "period": r["period"], "value": r["old_value"], "expected": r["new_value"]} for r in rows]
    res = save_cells(head["scenario"], changes, f"Reversal of save {commit_id}", measure=rows[0]["measure"], _admin_name=u["name"])
    if res.get("saved"):
        with conn() as c:
            _admin_log(c, "Save reversed", f"Save {commit_id}, {res['saved']} cells, new save {res['commitId']}", u["name"])
    return res


# ---------------------------------------------------------------- MCP
def build_server(**kw):
    from mcp.server.fastmcp import FastMCP
    from mcp.types import ToolAnnotations
    mcp = FastMCP("plangrid", **kw)
    ro, rw = ToolAnnotations(readOnlyHint=True), ToolAnnotations(readOnlyHint=False, destructiveHint=False)

    @mcp.tool(name="list_options", annotations=ro, description="Versions (scenarios) with status, markets with region and managers, users and roles, workflow statuses, closed months.")
    def t1() -> dict: return list_options()

    @mcp.tool(name="get_board", annotations=ro, description="Planning cycle board: every market's plan-year total for a version, the comparison version, last year's actuals, year-to-date actuals, workflow status and latest note.")
    def t2(scenario: str = WORKFLOW_SCENARIO, compare: str = "Agent 2+10") -> dict: return get_board(scenario, compare)

    @mcp.tool(name="get_grid", annotations=ro, description="One market's product x month values for a version and plan year, with a comparison version ('Last year actuals' shifts actuals forward a year). Says whether the user may edit and why not.")
    def t3(scenario: str, market: str, compare: str | None = None, year: int = PLAN_YEAR, user_id: int | None = None, measure: str = DEFAULT_MEASURE) -> dict:
        return get_grid(scenario, market, compare, measure, year, user_id)

    @mcp.tool(name="get_market", annotations=ro, description="One market's workflow status, comment thread (agent explanation, planner notes, review notes) and status history.")
    def t4(scenario: str, market: str) -> dict: return get_market(scenario, market)

    @mcp.tool(name="get_history", annotations=ro, description="Recent saves, newest first: who, when, note, cells, net change.")
    def t5(scenario: str | None = None, market: str | None = None, limit: int = 20) -> dict: return get_history(scenario, market, limit)

    @mcp.tool(name="save_cells", annotations=rw, description="Save changed cells as one transaction for a user. Each change: {market, item, period, value, expected}. Nothing is saved if a version or month is closed, the workflow status does not allow the user to edit, or a stored value differs from 'expected'.")
    def t6(scenario: str, changes: list[dict], comment: str = "", user_id: int | None = None, measure: str = DEFAULT_MEASURE) -> dict:
        return save_cells(scenario, changes, comment, user_id, measure)

    @mcp.tool(name="set_status", annotations=rw, description="Move a market through the workflow: submit (planner), review (FP&A), approve (CFO), send_back (FP&A or CFO, note required), reopen_review (CFO).")
    def t7(scenario: str, market: str, action: str, user_id: int, note: str = "") -> dict: return set_status(scenario, market, action, user_id, note)

    @mcp.tool(name="add_comment", annotations=rw, description="Add a note to a market's comment thread.")
    def t8(scenario: str, market: str, text: str, user_id: int) -> dict: return add_comment(scenario, market, text, user_id)

    @mcp.tool(name="admin_overview", annotations=ro, description="Admin view: versions with status and totals, users and roles, workflow counts, closed months, recent admin actions and status changes.")
    def t9() -> dict: return admin_overview()

    @mcp.tool(name="set_scenario", annotations=rw, description="Admin: open or close a version, or change its description.")
    def t10(scenario: str, status: str | None = None, description: str | None = None, user_id: int | None = None) -> dict:
        return set_scenario(scenario, status, description, user_id)

    @mcp.tool(name="set_lock_before", annotations=rw, description="Admin: close every month before this date key (e.g. 20260301).")
    def t11(lock_before: int | None, user_id: int | None = None) -> dict: return set_lock_before(lock_before, user_id)

    @mcp.tool(name="copy_scenario", annotations=rw, description="Admin: copy every cell of one version into a new version (snapshot). Refuses if the name exists.")
    def t12(source: str, target: str, comment: str = "", user_id: int | None = None, status: str = "Closed") -> dict:
        return copy_scenario(source, target, comment, user_id, status)

    @mcp.tool(name="upsert_user", annotations=rw, description="Admin: add or change a user: name, role (Planner, FP&A, CFO, Admin), markets (list or 'ALL'), active.")
    def t13(name: str, role: str, markets: list[str] | str, active: bool = True, user_id: int | None = None, admin_user_id: int | None = None) -> dict:
        return upsert_user(name, role, markets, active, user_id, admin_user_id)

    @mcp.tool(name="get_commit", annotations=ro, description="One save with every cell change: market, product, month, old value, new value.")
    def t14(commit_id: int, limit: int = 500) -> dict: return get_commit(commit_id, limit)

    @mcp.tool(name="revert_commit", annotations=rw, description="Admin: reverse one save by writing back its old values as a new save. Refuses if any of those cells changed since.")
    def t15(commit_id: int, user_id: int | None = None) -> dict: return revert_commit(commit_id, user_id)
    return mcp
