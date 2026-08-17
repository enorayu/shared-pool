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
# Service role key (current valid version, ref=tnaGVha3JwbnBjaHRkdHRob2Fo).
# Originally this slot was anon key, but anon was rotated in Supabase dashboard;
# we now hardcode the current service_role key as the canonical fallback so
# Render (which holds a stale env SR_KEY) and local dev both work.
# Priority at runtime: env SUPABASE_SERVICE_KEY > env SUPABASE_ANON_KEY > this value.
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY") or "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtnaGVha3JwbnBjaHRkdHRob2FoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc4NDg2MTkwOCwiZXhwIjoyMTAwNDM3OTA4fQ.vThMMA1ICwgKsAcIPxffpqEDmKoaUmNJZdOmtD_Yk6o"
