ALTER TABLE earnings ADD COLUMN created_at_day DATE generated always AS (DATE(created_at))
