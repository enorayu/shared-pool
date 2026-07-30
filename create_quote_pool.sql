-- ============================================================
-- quote_pool: 报价详情池（替代旧 price_pool）
-- 从 reply_pool A类导入，记录完整报价信息
-- ============================================================

-- 如果旧表存在，先备份再删除
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename='price_pool') THEN
        DROP TABLE IF EXISTS price_pool CASCADE;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS quote_pool (
    quote_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- 域名 & 联系人
    email               TEXT NOT NULL,                    -- 回复人邮箱
    domain              TEXT NOT NULL,                    -- 域名
    supplier            TEXT,                             -- 供应商名称/发件人
    contact_email       TEXT,                             -- 联系人邮箱

    -- 站点画像
    niche               TEXT,                             -- 领域/niche (tech, travel, health...)
    country             TEXT,                             -- 国家/地区
    traffic             TEXT,                             -- 流量数据 (monthly visits, DA, DR...)
    site_category       TEXT,                             -- 站点分类 (blog, news, magazine...)

    -- 合作条款
    cooperation_type    TEXT,                             -- 合作类型 (guest post, link insertion, review, listicle...)
    price               TEXT,                             -- 价格（含货币和 tiers，如 "$80/post, $120/review"）
    link_rules          TEXT,                             -- 链接规则 (dofollow, no casino, max 2 links...)
    permanence          TEXT,                             -- 永久性 (permanent, 1 year, monthly fee...)
    content             TEXT,                             -- 内容要求 (word count, topics, images...)
    tat                 TEXT,                             -- TAT 交付周期 (2-3 days, 1 week...)
    payment             TEXT,                             -- 付款方式 (PayPal, bank transfer, crypto...)
    discount            TEXT,                             -- 折扣 (bulk discount 10%, first order free...)

    -- 附加信息
    additional_services TEXT,                             -- 附加服务 (social share, index guarantee, writing service...)
    requirements        TEXT,                             -- 特殊要求/限制 (no CBD, gambling, adult...)

    -- 原始信息 & 追踪
    reply_id            BIGINT,                           -- 关联 reply_pool.reply_id
    reply_content       TEXT,                             -- 原始回复全文（用于参考）

    -- 状态 & 操作
    status              TEXT DEFAULT 'New',               -- New / Reviewed / Contacted / Negotiating / Accepted / Rejected
    priority            INT DEFAULT 0,                    -- 优先级
    notes               TEXT,                             -- 人工备注
    discovered_by       TEXT DEFAULT 'system',            -- 发现者
    discovered_at       TIMESTAMPTZ DEFAULT NOW(),        -- 录入时间

    -- 操作人
    reviewed_by         TEXT,                             -- 审核人
    reviewed_at         TIMESTAMPTZ,                      -- 审核时间
    contacted_by        TEXT,                             -- 沟通人
    contacted_at        TIMESTAMPTZ,                      -- 沟通时间

    UNIQUE(email)
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_quote_domain  ON quote_pool(domain);
CREATE INDEX IF NOT EXISTS idx_quote_status  ON quote_pool(status);
CREATE INDEX IF NOT EXISTS idx_quote_niche   ON quote_pool(niche);
CREATE INDEX IF NOT EXISTS idx_quote_country ON quote_pool(country);
CREATE INDEX IF NOT EXISTS idx_quote_reply_id ON quote_pool(reply_id);

-- RLS
ALTER TABLE quote_pool ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname='anon_can_read_quotes' AND tablename='quote_pool') THEN
        CREATE POLICY "anon_can_read_quotes" ON quote_pool FOR SELECT USING (true);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname='anon_can_insert_quotes' AND tablename='quote_pool') THEN
        CREATE POLICY "anon_can_insert_quotes" ON quote_pool FOR INSERT WITH CHECK (true);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname='anon_can_update_quotes' AND tablename='quote_pool') THEN
        CREATE POLICY "anon_can_update_quotes" ON quote_pool FOR UPDATE USING (true);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE policyname='anon_can_delete_quotes' AND tablename='quote_pool') THEN
        CREATE POLICY "anon_can_delete_quotes" ON quote_pool FOR DELETE USING (true);
    END IF;
END $$;
