-- Quote Pool 清洗 & 标准化 — 新增 6 个标准化字段
-- 执行：Supabase 控制台 → SQL Editor → 粘贴 → Run
-- （PostgREST 不支持 DDL，必须在此手动执行；service_role key 亦无法绕过）
ALTER TABLE quote_pool
  ADD COLUMN IF NOT EXISTS original_price      numeric,
  ADD COLUMN IF NOT EXISTS original_currency   text,
  ADD COLUMN IF NOT EXISTS normalized_price    numeric,
  ADD COLUMN IF NOT EXISTS normalized_currency text    DEFAULT 'USD',
  ADD COLUMN IF NOT EXISTS price_type          text,
  ADD COLUMN IF NOT EXISTS data_status         text    DEFAULT 'NEED_REVIEW';
