-- PlanGrid filing cabinet. Every change to plan_cells is copied to plan_log by a trigger,
-- so the history is kept even if something writes to the table directly.
CREATE TABLE IF NOT EXISTS plan_commits (
  commit_id   BIGSERIAL PRIMARY KEY,
  scenario    TEXT NOT NULL,
  markets     TEXT NOT NULL,
  cell_count  INT NOT NULL,
  net_change  DOUBLE PRECISION NOT NULL,
  comment     TEXT NOT NULL DEFAULT '',
  updated_by  TEXT NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS plan_cells (
  scenario    TEXT NOT NULL,
  measure     TEXT NOT NULL DEFAULT 'M99',
  market      TEXT NOT NULL,
  item        TEXT NOT NULL,
  period      INT  NOT NULL,
  value       DOUBLE PRECISION,
  commit_id   BIGINT REFERENCES plan_commits(commit_id),
  updated_by  TEXT NOT NULL DEFAULT 'Setup',
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (scenario, measure, market, item, period)
);
CREATE TABLE IF NOT EXISTS plan_log (
  log_id      BIGSERIAL PRIMARY KEY,
  commit_id   BIGINT,
  scenario    TEXT, measure TEXT, market TEXT, item TEXT, period INT,
  old_value   DOUBLE PRECISION,
  new_value   DOUBLE PRECISION,
  updated_by  TEXT,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS plan_log_commit ON plan_log(commit_id);
CREATE INDEX IF NOT EXISTS plan_cells_mkt ON plan_cells(scenario, market);
CREATE OR REPLACE FUNCTION plan_cells_audit() RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    INSERT INTO plan_log(commit_id,scenario,measure,market,item,period,old_value,new_value,updated_by)
    VALUES (OLD.commit_id,OLD.scenario,OLD.measure,OLD.market,OLD.item,OLD.period,OLD.value,NULL,
            coalesce(current_setting('plangrid.user', true), session_user));
    RETURN OLD;
  END IF;
  IF TG_OP = 'UPDATE' AND NEW.value IS NOT DISTINCT FROM OLD.value THEN RETURN NEW; END IF;
  INSERT INTO plan_log(commit_id,scenario,measure,market,item,period,old_value,new_value,updated_by)
  VALUES (NEW.commit_id,NEW.scenario,NEW.measure,NEW.market,NEW.item,NEW.period,
          CASE WHEN TG_OP='UPDATE' THEN OLD.value END, NEW.value, NEW.updated_by);
  RETURN NEW;
END $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS plan_cells_audit ON plan_cells;
CREATE TRIGGER plan_cells_audit AFTER INSERT OR UPDATE OR DELETE ON plan_cells
  FOR EACH ROW EXECUTE FUNCTION plan_cells_audit();
CREATE TABLE IF NOT EXISTS plan_settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_by TEXT NOT NULL DEFAULT 'Setup',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS plan_admin_log (
  log_id BIGSERIAL PRIMARY KEY,
  action TEXT NOT NULL,
  detail TEXT NOT NULL,
  updated_by TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
