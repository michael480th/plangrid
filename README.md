# PlanGrid

Save service and database for Claude planning pages. Pages (Claude artifacts) call this service through a Claude connector. The service checks closed versions, closed months, the planning workflow and clashing edits, then writes to Postgres. A database trigger logs every cell change (old value, new value, who, when) in `plan_log`. Each save is one row in `plan_commits` with a note.

Workflow per market: Agent Draft, Planner Submitted, FP&A Reviewed, CFO Approved. Planners edit their own markets until they submit. FP&A reviews or sends back with a note. The CFO approves or sends back.

- `planning.py`: tools (list_options, get_board, get_grid, get_market, get_history, save_cells, set_status, add_comment, and admin tools)
- `schema.sql`: tables and the audit trigger
- `app.py`: HTTP entry point. The connector address is `https://<host>/<PLANGRID_SECRET>/mcp`
- `render.yaml`: Render Blueprint (Postgres + web service)
- `seed.csv.gz`, `seed_extra.json`: depletion demo data (2025 and 2026), markets, agent explanations

Demo build: access control is the secret in the address, and users pick who they are. Real sign-in comes next.

## Pages

- `pages/depletion.html`: Depletion Forecasting, the planning tool (Claude artifact https://claude.ai/artifact/72gSMESXh7nQqfcujyP24z)
- `pages/admin.html`: PlanGrid Admin (Claude artifact https://claude.ai/artifact/5KPHJ6nyMV6mToyuwUvKb9)

Both call the connector named `PlanGrid` in Claude.
