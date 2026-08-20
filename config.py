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
# Service role key is NOT hardcoded here for security.
# Set it via environment variable SUPABASE_SERVICE_ROLE_KEY (Render Config Vars).
# Local dev fallback: set the env var, or add SUPABASE_SERVICE_ROLE_KEY to this file.
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY") or "__USE_SERVICE_ROLE_KEY_INSTEAD__"
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or None
