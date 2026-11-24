# PlanGrid

Save service and database for Claude planning pages. A planning page (Claude artifact) calls this service through a Claude connector. The service checks locks and clashing edits, then writes to Postgres. A database trigger logs every cell change (old value, new value, who, when) in `plan_log`. Each save is one row in `plan_commits` with a comment.

- `mailroom.py`: the four tools (list_options, get_grid, save_cells, get_history)
- `schema.sql`: tables and the audit trigger
- `app.py`: HTTP entry point. The connector address is `https://<host>/<PLANGRID_SECRET>/mcp`
- `render.yaml`: Render Blueprint (Postgres + web service)
- `seed.csv.gz`: depletion demo data, loaded once when the database is empty

Demo build: access control is the secret in the address. Real sign-in comes next.
