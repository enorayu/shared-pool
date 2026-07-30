DROP TABLE IF EXISTS quote_pool CASCADE;

CREATE TABLE quote_pool (
    quote_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    email               TEXT NOT NULL,
    domain              TEXT NOT NULL,
    supplier            TEXT,
    contact_email       TEXT,

    niche               TEXT,
    country             TEXT,
    traffic             TEXT,
    site_category       TEXT,

    cooperation_type    TEXT,
    price               TEXT,
    link_rules          TEXT,
    permanence          TEXT,
    content             TEXT,
    tat                 TEXT,
    payment             TEXT,
    discount            TEXT,

    additional_services TEXT,
    requirements        TEXT,

    reply_id            BIGINT,
    reply_content       TEXT,

    status              TEXT DEFAULT 'New',
    priority            INT DEFAULT 0,
    notes               TEXT,
    discovered_by       TEXT DEFAULT 'system',
    discovered_at       TIMESTAMPTZ DEFAULT NOW(),

    reviewed_by         TEXT,
    reviewed_at         TIMESTAMPTZ,
    contacted_by        TEXT,
    contacted_at        TIMESTAMPTZ,

    UNIQUE(email)
);

CREATE INDEX idx_quote_domain  ON quote_pool(domain);
CREATE INDEX idx_quote_status  ON quote_pool(status);
CREATE INDEX idx_quote_niche   ON quote_pool(niche);
CREATE INDEX idx_quote_country ON quote_pool(country);
CREATE INDEX idx_quote_reply_id ON quote_pool(reply_id);

ALTER TABLE quote_pool ENABLE ROW LEVEL SECURITY;

CREATE POLICY "anon_can_read_quotes" ON quote_pool FOR SELECT USING (true);
CREATE POLICY "anon_can_insert_quotes" ON quote_pool FOR INSERT WITH CHECK (true);
CREATE POLICY "anon_can_update_quotes" ON quote_pool FOR UPDATE USING (true);
CREATE POLICY "anon_can_delete_quotes" ON quote_pool FOR DELETE USING (true);
