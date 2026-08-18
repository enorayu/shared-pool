-- 在 Supabase SQL Editor 执行此函数（只需建一次）
-- 之后可通过 REST RPC 触发物化视图刷新，无需手动进后台
CREATE OR REPLACE FUNCTION refresh_pool_stats_mv()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  REFRESH MATERIALIZED VIEW pool_stats_mv;
END;
$$;

-- 授权 anon 也能调用（面板部署用 anon key 调 RPC）
GRANT EXECUTE ON FUNCTION refresh_pool_stats_mv() TO anon;
GRANT EXECUTE ON FUNCTION refresh_pool_stats_mv() TO authenticated;
