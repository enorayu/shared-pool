-- Run this in Supabase SQL Editor (https://supabase.com/dashboard → your project → SQL Editor)
-- Then the Shared Pool v2 will store operation logs in this table instead of the
-- overloaded config JSON field (which silently stopped working at ~105KB).

CREATE TABLE IF NOT EXISTS operation_log (
    log_id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    op_time             TIMESTAMPTZ DEFAULT NOW(),
    type                TEXT,
    username            TEXT,
    pool                TEXT,
    count               INT DEFAULT 0,
    detail              TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_op_log_time ON operation_log(op_time DESC);
CREATE INDEX IF NOT EXISTS idx_op_log_type ON operation_log(type);

ALTER TABLE operation_log ENABLE ROW LEVEL SECURITY;

CREATE POLICY "anon_can_read_op_log" ON operation_log
    FOR SELECT USING (true);

CREATE POLICY "anon_can_insert_op_log" ON operation_log
    FOR INSERT WITH CHECK (true);

CREATE POLICY "anon_can_delete_op_log" ON operation_log
    FOR DELETE USING (true);
