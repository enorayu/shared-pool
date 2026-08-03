"""Migrate legacy operation_logs JSON blob (in config table) into the new
independent operation_log table.

Run:  python migrate_operation_log.py
Requires config.py with SUPABASE_URL / SUPABASE_ANON_KEY in same dir.
"""
import json
import sys
import urllib.request
import urllib.parse

try:
    from config import SUPABASE_URL, SUPABASE_ANON_KEY
except Exception as e:
    print("ERROR: cannot import config.py:", e)
    sys.exit(1)

REST = SUPABASE_URL.rstrip("/") + "/rest/v1"
HEADERS = {
    "apikey": SUPABASE_ANON_KEY,
    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}


def cfg_get(key):
    url = f"{REST}/config?select=value&key=eq.{urllib.parse.quote(key)}"
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read())
        return data[0]["value"] if data else None
    except Exception as e:
        print("cfg_get error:", e)
        return None


def ensure_table():
    """Try to create operation_log table via REST (works only if anon has DDL)."""
    sql = """
    CREATE TABLE IF NOT EXISTS operation_log (
        log_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        op_time TIMESTAMPTZ DEFAULT NOW(),
        type TEXT, username TEXT, pool TEXT,
        count INT DEFAULT 0, detail TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    """
    url = f"{REST}/rpc/exec?query={urllib.parse.quote(sql)}"
    req = urllib.request.Request(url, headers=HEADERS, method="POST")
    try:
        urllib.request.urlopen(req, timeout=30).read()
        print("Table created via rpc.exec")
        return True
    except Exception as e:
        print("Cannot create table via RPC (expected on anon):", str(e)[:120])
        return False


def insert_rows(rows):
    url = f"{REST}/operation_log"
    req = urllib.request.Request(
        url, data=json.dumps(rows).encode("utf-8"), headers=HEADERS, method="POST"
    )
    try:
        urllib.request.urlopen(req, timeout=30).read()
        return True
    except Exception as e:
        print("insert error:", str(e)[:200])
        return False


def main():
    raw = cfg_get("operation_logs")
    if not raw:
        print("No legacy logs to migrate.")
        return
    logs = json.loads(raw) if isinstance(raw, str) else raw
    if not logs:
        print("Legacy log list empty.")
        return
    print(f"Found {len(logs)} legacy log entries.")

    if not ensure_table():
        print("\n>>> operation_log table does not exist.")
        print(">>> Please run the SQL from /setup (reply_pool + operation_log section)")
        print(">>> in your Supabase SQL Editor, then re-run this script.\n")
        return

    payload = []
    for l in logs:
        payload.append({
            "type": l.get("type"),
            "username": l.get("user") or "unknown",
            "pool": l.get("table"),
            "count": int(l.get("count") or 0),
            "detail": l.get("detail") or "",
            # preserve original time if present
            "op_time": l.get("time") if l.get("time") else None,
        })

    ok = insert_rows(payload)
    if ok:
        print(f"Migrated {len(payload)} entries into operation_log.")
        print("Legacy config blob left intact (harmless); new logs go to the table.")
    else:
        print("Migration insert failed — check table columns / anon perms.")


if __name__ == "__main__":
    main()
