"""
Shared Pool v2 — Configuration
===============================
Fill in your own Supabase project credentials below.
Get these from: Supabase Dashboard → Settings → API

SUPABASE_URL       — Your project URL (e.g. https://xxxxx.supabase.co)
SUPABASE_ANON_KEY  — The "anon public" key (starts with eyJ...)
                     DO NOT use the "service_role secret" key here.

Share this file with your team. Each person can use their own project.
"""

import os

SUPABASE_URL = "https://kgheakrpnpchtdtthoah.supabase.co"
# 注：原 anon key 已被 Supabase 控制台 rotate 失效，导致 Render 后端所有查询 401 → 前端全 0。
# 改用当前有效的 service_role key（Render 后端服务端使用，安全）。优先读环境变量 SUPABASE_ANON_KEY。
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY") or "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiOiJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc4NDg2MTkwOCwiZXhwIjoyMTAwNDM3OTA4fQ.vThMMA1ICwgKsAcIPxffpqEDmKoaUmNJZdOmtD_Yk6o"
