"""
Shared Pool v2 — Supplier Intelligence (Supabase Edition)
==========================================================
A generic shared pool for domain/link supplier intelligence.
Connects to Supabase PostgreSQL as the data layer.
Anyone can use it with their own Supabase project — just edit config.py.

Four Pools:
  Domain Pool  — domain_pool table      (161K+ domains)
  Email Pool   — email_pool table        (send queue, bounce tracking)
  Reply Pool   — reply_pool table        (inbound replies, A/B/C classification)
  Quote Pool  — quote_pool table        (detailed quotes from A-class replies)

Quick Start:
  1. Edit config.py with your SUPABASE_URL + SUPABASE_ANON_KEY
  2. pip install flask
  3. python shared_pool_v2.py --port 8765
  4. Open http://localhost:8765

Required Supabase Tables (already exist in default project):
  domain_pool, supplier_pool, quote_pool, config

To create email_pool and reply_pool:
  Run the CREATE TABLE SQL from the /setup page after starting the server.
"""

import os
import sys
import json
import argparse
import csv
import io
import time as _time
import urllib.request
import urllib.error
import urllib.parse
import re as _re
import requests
import openpyxl
from collections import defaultdict
from datetime import datetime, timezone, timedelta

# ── Config ─────────────────────────────────────────────────
try:
    from config import SUPABASE_URL, SUPABASE_ANON_KEY
except ImportError:
    print("ERROR: config.py not found. Create config.py with SUPABASE_URL and SUPABASE_ANON_KEY.")
    sys.exit(1)

if not SUPABASE_URL or not SUPABASE_ANON_KEY or "YOUR_PROJECT" in SUPABASE_URL:
    print("ERROR: Please edit config.py with your actual Supabase credentials.")
    sys.exit(1)

# ── Flask ───────────────────────────────────────────────────
try:
    from flask import Flask, request, jsonify, Response
except ImportError:
    print("ERROR: Flask not installed. Run: pip install flask")
    sys.exit(1)

app = Flask(__name__)

# ── CORS 跨域支持 ──
@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    return response

REST_URL = f"{SUPABASE_URL}/rest/v1"
AUTH_HEADERS = {
    "apikey": SUPABASE_ANON_KEY,
    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
}

# Simple in-memory cache to reduce Supabase API calls
_cache = {}
def _cached(key, ttl_sec=30, fn=None):
    now = _time.time()
    if key in _cache and _cache[key][0] > now:
        return _cache[key][1]
    if fn:
        val = fn()
        _cache[key] = (now + ttl_sec, val)
        return val
    return None


def _count_unique_domains(status=None):
    """Count unique domain names (deduplicated) by paginated fetch.
    Cached 600s since scanning 161K rows takes ~160 API calls (Supabase caps at 1000/request).
    When status=None, returns a dict of all status counts in one pass."""
    def _do_count():
        seen = {}  # status -> set of unique domains
        if status:
            seen[status] = set()
        offset = 0
        batch_size = 1000  # match Supabase default max-rows cap
        while True:
            batch = db.select("domain_pool", select="domain,collection_status",
                              limit=batch_size, offset=offset,
                              order="domain_id")
            if not batch:
                break
            for row in batch:
                dom = (row.get("domain") or "").strip().lower()
                st = row.get("collection_status", "")
                if not dom:
                    continue
                if status:
                    if st == status:
                        seen[status].add(dom)
                else:
                    if st not in seen:
                        seen[st] = set()
                    seen[st].add(dom)
            if len(batch) < batch_size:
                break
            offset += batch_size

        if status:
            return len(seen.get(status, set()))
        else:
            return {k: len(v) for k, v in seen.items()}
    cache_key = f"unique_domains:{status or 'all'}"
    return _cached(cache_key, ttl_sec=600, fn=_do_count)

# ── Supabase Client ─────────────────────────────────────────

class SB:
    """Minimal Supabase REST client (stdlib only, no extra deps)."""

    def __init__(self):
        self.base = REST_URL
        self._headers = {
            "apikey": SUPABASE_ANON_KEY,
            "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
        }

    def _req(self, method, path, data=None, params=None, extra_headers=None):
        url = f"{self.base}/{path}"
        if params:
            qs = []
            for k, v in params.items():
                if v is not None:
                    qs.append(f"{urllib.parse.quote(k)}={urllib.parse.quote(str(v))}")
            if qs:
                connector = "&" if "?" in url else "?"
                url += connector + "&".join(qs)

        headers = dict(self._headers)
        headers["Connection"] = "close"  # prevent keep-alive issues with threaded Flask
        if extra_headers:
            headers.update(extra_headers)

        body = None
        if data is not None:
            body = json.dumps(data).encode("utf-8")
            headers.setdefault("Content-Type", "application/json")

        # Retry on SSL/connection errors (common with Supabase free tier)
        last_err = None
        for attempt in range(3):
            try:
                req = urllib.request.Request(url, data=body, headers=headers, method=method)
                with urllib.request.urlopen(req, timeout=20) as resp:
                    raw = resp.read()
                    return (resp, json.loads(raw) if raw else None)
            except urllib.error.HTTPError as e:
                err_body = e.read().decode("utf-8", errors="replace")[:500]
                return (e, {"error": str(e.code), "detail": err_body})
            except (urllib.error.URLError, OSError) as e:
                last_err = e
                if attempt < 2:
                    _time.sleep(0.5 * (attempt + 1))
        raise last_err

    # ── Read helpers ───────────────────────────────────────

    @staticmethod
    def _filter_part(k, v):
        """Build a Supabase filter string.
        If v already starts with an operator (eq., neq., gt., gte., lt., lte.,
        like., ilike., is., in., not.is.), use it directly. Otherwise prefix eq."""
        if v is None:
            return None
        v_str = str(v)
        ops = ("eq.", "neq.", "gt.", "gte.", "lt.", "lte.",
               "like.", "ilike.", "is.", "in.", "not.is.")
        if any(v_str.startswith(op) for op in ops):
            return f"{k}={urllib.parse.quote(v_str)}"
        return f"{k}=eq.{urllib.parse.quote(v_str)}"

    def select(self, table, select="*", filters=None, limit=None, offset=None,
               order=None, ascending=True):
        params = {"select": select}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset
        if order:
            direction = "asc" if ascending else "desc"
            params["order"] = f"{order}.{direction}"

        path = table
        if filters:
            parts = []
            for k, v in filters.items():
                part = self._filter_part(k, v)
                if part:
                    parts.append(part)
            if parts:
                path += "?" + "&".join(parts)

        resp, data = self._req("GET", path, params=params)
        if isinstance(data, list):
            return data
        return []

    def count(self, table, filters=None):
        """Get exact count via content-range header."""
        params = {"select": "*", "limit": 1}
        extra = {"Prefer": "count=exact"}
        path = table
        if filters:
            parts = []
            for k, v in filters.items():
                part = self._filter_part(k, v)
                if part:
                    parts.append(part)
            if parts:
                path += "?" + "&".join(parts)

        resp, _ = self._req("GET", path, params=params, extra_headers=extra)
        if hasattr(resp, "headers"):
            cr = resp.headers.get("content-range", "")
            if "/" in cr:
                return int(cr.split("/")[-1])
        return 0

    # ── Write helpers ──────────────────────────────────────

    def insert(self, table, rows, upsert=False):
        """Insert rows. Returns (resp, data)."""
        extra = {}
        if upsert:
            extra["Prefer"] = "resolution=merge-duplicates, return=representation"
        else:
            extra["Prefer"] = "return=representation"
        resp, data = self._req("POST", table, data=rows, extra_headers=extra)
        return resp, data

    def update(self, table, data, filters):
        """Update rows matching filters. Returns response object."""
        path = table
        parts = []
        for k, v in (filters or {}).items():
            if v is not None:
                parts.append(f"{k}=eq.{urllib.parse.quote(str(v))}")
        if parts:
            path += "?" + "&".join(parts)

        resp, result = self._req("PATCH", path, data=data,
                                  extra_headers={"Prefer": "return=representation"})
        return resp, result

    def patch_by_ids(self, table, data, ids, id_column="domain_id"):
        """Update multiple rows by IDs."""
        if not ids:
            return None, []
        all_results = []
        for batch in chunks(ids, 100):
            in_clause = ",".join(f"{urllib.parse.quote(str(i))}" for i in batch)
            path = f"{table}?{id_column}=in.({in_clause})"
            resp, result = self._req("PATCH", path, data=data,
                                      extra_headers={"Prefer": "return=representation"})
            all_results.append(result)
        return resp, all_results


db = SB()


# ── Helpers ─────────────────────────────────────────────────

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def now_iso():
    return datetime.now(timezone(timedelta(hours=8))).isoformat()

def safe_str(v, default=""):
    if v is None:
        return default
    return str(v)


def _utc_to_bj(iso_str):
    """Convert existing UTC ISO timestamps to Beijing time for display."""
    if not iso_str or not isinstance(iso_str, str):
        return iso_str
    try:
        if iso_str.endswith("+00:00"):
            dt = datetime.fromisoformat(iso_str)
            bj = dt.astimezone(timezone(timedelta(hours=8)))
            return bj.isoformat()
        return iso_str
    except Exception:
        return iso_str


# ── Operation Log helpers ───────────────────────────────────

def _log_operation(op_type, user, table_name, count, detail=""):
    """Log an operation to config table (key='operation_logs')."""
    try:
        raw = _get_config("operation_logs", "[]")
        logs = json.loads(raw) if raw else []
    except Exception:
        logs = []

    log_entry = {
        "time": now_iso(),
        "type": op_type,
        "user": user or "unknown",
        "table": table_name,
        "count": count,
        "detail": detail,
    }
    logs.insert(0, log_entry)  # newest first

    # Keep only last 1000 entries
    logs = logs[:1000]
    _set_config("operation_logs", json.dumps(logs, ensure_ascii=False), "Operation logs")


# ── Health ──────────────────────────────────────────────────

@app.route("/health")
def health():
    try:
        rows = db.select("domain_pool", select="domain_id", limit=1)
        connected = True
    except Exception:
        connected = False
    return jsonify({
        "status": "ok" if connected else "error",
        "db": SUPABASE_URL,
        "version": "v2-supabase",
        "time": now_iso(),
        "connected": connected
    })


@app.route("/api/version")
def api_version():
    return jsonify({
        "version": "v2-supabase",
        "backend": "Supabase PostgreSQL",
        "project": SUPABASE_URL.split("//")[1].split(".")[0] if "//" in SUPABASE_URL else "",
        "tables": {
            "domain_pool": db.count("domain_pool"),
            "supplier_pool": db.count("supplier_pool"),
            "quote_pool": db.count("quote_pool"),
            "config": db.count("config"),
        }
    })


# ════════════════════════════════════════════════════════════
# Domain Pool API (→ domain_pool table)
# ════════════════════════════════════════════════════════════

DOMAIN_STATUSES = ["New", "Claimed", "Contacted", "Replied", "Imported", "Collecting", "Completed", "Exported", "Failed"]


@app.route("/api/domain/register", methods=["POST"])
def domain_register():
    """Batch register domains. Dedup against existing DB first.
    Body: {"domains": ["a.com","b.com"], "priority": 0,
           "collection_status": "New", "notes": "", "imported_by": "emma"}
    If collection_status is omitted, defaults to "New".
    All imported domains are marked with source="未提取" for extraction tracking."""
    data = request.get_json(force=True)
    raw_domains = data.get("domains", [])
    imported_by = data.get("imported_by", "")
    priority = data.get("priority", 0)
    notes = data.get("notes", "")
    default_status = data.get("collection_status", "New")
    if default_status not in DOMAIN_STATUSES:
        default_status = "New"

    if imported_by:
        if notes:
            notes = f"[imported by {imported_by}] {notes}"
        else:
            notes = f"[imported by {imported_by}]"

    # 1. Normalize & dedup within batch
    seen = {}
    for raw in raw_domains:
        d = raw.lower().strip().lstrip("www.")
        if not d:
            continue
        if d not in seen:
            seen[d] = {
                "domain": d,
                "source": "未提取",
                "priority": priority,
                "notes": notes,
                "collection_status": default_status,
            }
    if not seen:
        return jsonify({"new": 0, "duplicates": 0, "domains": []})

    # 2. Batch check against existing domains (100 per query)
    domain_list = list(seen.keys())
    existing = set()
    for i in range(0, len(domain_list), 100):
        batch = domain_list[i:i+100]
        d_filter = ",".join(batch)
        try:
            results = db.select("domain_pool", select="domain",
                              filters={"domain": f"in.({d_filter})"}, limit=100)
            for r in (results or []):
                existing.add((r.get("domain") or "").strip().lower())
        except Exception:
            pass  # if check fails, treat as new (best-effort dedup)

    # 3. Filter out existing
    new_domains = {d: v for d, v in seen.items() if d not in existing}
    dup_count = len(seen) - len(new_domains)

    if not new_domains:
        _log_operation("domain_import", imported_by, "domain_pool", 0,
                       f"All {dup_count} domains already exist, Source: 未提取")
        return jsonify({"new": 0, "duplicates": dup_count})

    # 4. Insert only new domains
    rows = list(new_domains.values())
    resp, result = db.insert("domain_pool", rows, upsert=False)
    new_count = 0
    if hasattr(resp, "status") and resp.status in (200, 201):
        new_count = len(result) if isinstance(result, list) else len(rows)

    # 5. Clear cache so stats refresh immediately
    global _cache
    _cache.clear()

    # 6. Log operation
    _log_operation("domain_import", imported_by, "domain_pool", new_count,
                   f"Source: 未提取, Duplicates: {dup_count}")

    return jsonify({"new": new_count, "duplicates": dup_count})


@app.route("/api/domain/list", methods=["GET"])
def domain_list():
    """List domains with filtering. Returns unique domain list (one per domain)."""
    status = request.args.get("status", "")
    user = request.args.get("user", "")
    limit = min(int(request.args.get("limit", 100)), 5000)
    offset = int(request.args.get("offset", 0))

    filters = {}
    if status:
        filters["collection_status"] = status
    if user:
        filters["claimed_by"] = user

    domains = db.select(
        "domain_pool",
        select="domain_id,domain,source,collection_status,claimed_by,claim_batch_id,priority,notes,created_at",
        filters=filters,
        limit=limit,
        offset=offset,
        order="priority",
        ascending=False,
    )

    # Deduplicate by domain name (keep first occurrence, highest priority)
    seen = set()
    unique_domains = []
    for d in domains:
        dom = d.get("domain", "")
        if dom and dom not in seen:
            seen.add(dom)
            unique_domains.append(d)

    total_filters = {k: v for k, v in {"collection_status": status, "claimed_by": user}.items() if v}
    total = db.count("domain_pool", filters=total_filters if total_filters else None)
    unique_total = _count_unique_domains(status if status else None)

    return jsonify({"domains": unique_domains, "total": total, "unique_total": unique_total})


@app.route("/api/domain/export", methods=["POST"])
def domain_export():
    """
    Export unclaimed domains for a user and lock them.
    Deduplicates by domain name. Generates CSV with domain fields only (no email).
    Only exports domains with collection_status=New, marks them as extracted after export.
    Exported in ascending domain_id order.
    """
    data = request.get_json(force=True)
    user = data.get("user", "").strip()
    if not user:
        return jsonify({"error": "user is required", "exported": 0}), 400
    count = min(int(data.get("count", 200)), 5000)

    # 1. Fetch unclaimed domains (New status, source!=已提取, ordered by domain_id ascending)
    domains = db.select(
        "domain_pool",
        select="domain_id,domain,source,priority,created_at",
        filters={"collection_status": "New", "source": "neq.已提取"},
        limit=count * 3,  # fetch extra for dedup
        order="domain_id",
        ascending=True,
    )

    # Deduplicate by domain name (keep first occurrence, lowest ID first)
    seen = set()
    unique_domains = []
    for d in domains:
        dom = d.get("domain", "").strip().lower()
        if dom and dom not in seen:
            seen.add(dom)
            unique_domains.append(d)
    unique_domains = unique_domains[:count]

    if not unique_domains:
        return jsonify({"exported": 0, "filename": "", "batch_id": ""})

    batch_id = f"domain_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{user}"
    filename = f"{batch_id}.csv"
    now = now_iso()
    ids = [d["domain_id"] for d in unique_domains]

    # 2. Lock domains and mark as extracted
    resp, results = db.patch_by_ids("domain_pool", {
        "claimed_by": user,
        "collection_status": "Claimed",
        "source": "已提取",
        "claim_batch_id": batch_id,
        "claim_time": now,
    }, ids)

    # Verify PATCH actually worked before generating CSV
    if hasattr(resp, "status") and resp.status not in (200, 201, 204):
        err_detail = ""
        if isinstance(results, list) and results:
            err_detail = str(results[0])[:200]
        elif isinstance(results, dict):
            err_detail = results.get("detail", str(results))[:200]
        print(f"[domain_export] PATCH failed HTTP {resp.status}: {err_detail}", file=sys.stderr)
        return jsonify({"error": f"Failed to lock domains (HTTP {resp.status})", "exported": 0}), 500

    # 3. Generate CSV — domain fields only (no contact_email)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["domain_id", "domain", "source", "priority", "created_at"])
    for r in unique_domains:
        writer.writerow([r["domain_id"], r["domain"], safe_str(r.get("source")),
                         r.get("priority", 0),
                         safe_str(r.get("created_at"))[:19]])

    csv_content = output.getvalue()

    # 4. Save CSV
    export_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "shared_pool_exports")
    os.makedirs(export_dir, exist_ok=True)
    with open(os.path.join(export_dir, filename), "w", newline="", encoding="utf-8") as f:
        f.write(csv_content)

    # 5. Clear cache & log operation
    global _cache
    _cache.clear()
    _log_operation("domain_export", user, "domain_pool", len(unique_domains),
                   f"Batch: {batch_id}, Status: Claimed, Source: 已提取")

    return jsonify({
        "exported": len(unique_domains),
        "filename": filename,
        "batch_id": batch_id,
        "csv_content": csv_content,
        "csv_preview": csv_content[:500]
    })


@app.route("/api/domain/distribute", methods=["POST"])
def domain_distribute():
    """
    Round-robin distribute unclaimed domains among team members.
    Body: {"count": 1000}  (users come from config or default)
    Uses config key "team_members" for user list. Falls back to ["leo","emma","jack"].
    Domains are ordered by domain_id ascending and marked as extracted after distribution.
    """
    data = request.get_json(force=True)

    # Read team members from config
    raw = _get_config("team_members", '["leo","emma","jack"]')
    try:
        users = json.loads(raw) if raw else ["leo", "emma", "jack"]
    except (json.JSONDecodeError, TypeError):
        users = [m.strip() for m in str(raw).split(",") if m.strip()]
    if not users:
        users = ["leo", "emma", "jack"]

    count = int(data.get("count", 0))

    total = db.count("domain_pool", filters={"collection_status": "New"})
    if count <= 0 or count > total:
        count = total

    if count <= 0:
        return jsonify({"assigned": 0, "distribution": {}})

    domains = db.select(
        "domain_pool",
        select="domain_id",
        filters={"collection_status": "New"},
        limit=count,
        order="domain_id",
        ascending=True,
    )

    now = now_iso()
    distribution = {u: 0 for u in users}
    for i, d in enumerate(domains):
        user = users[i % len(users)]
        db.update("domain_pool", {
            "claimed_by": user,
            "collection_status": "Claimed",
            "source": "已提取",
            "claim_time": now,
            "claim_batch_id": f"distribute_{now[:10]}_{user}",
        }, {"domain_id": d["domain_id"]})
        distribution[user] += 1

    # Clear cache & log operation
    global _cache
    _cache.clear()
    _log_operation("domain_distribute", "system", "domain_pool", len(domains),
                   f"Distribution: {json.dumps(distribution)}, Source: 已提取")

    return jsonify({"assigned": len(domains), "distribution": distribution})


@app.route("/api/domain/update_status", methods=["POST"])
def domain_update_status():
    """Update domain collection status."""
    data = request.get_json(force=True)
    domain_ids = data.get("ids", [])
    new_status = data.get("status", "")
    maisui_id = data.get("maisui_task_id", "")
    notes = data.get("notes", "")

    if new_status not in DOMAIN_STATUSES:
        return jsonify({"error": f"Invalid status: {new_status}"}), 400

    payload = {"collection_status": new_status, "updated_at": now_iso()}
    if maisui_id:
        payload["maisui_task_id"] = maisui_id
    if notes:
        payload["notes"] = notes

    resp, result = db.patch_by_ids("domain_pool", payload, domain_ids)
    updated = len(result) if result else 0
    return jsonify({"updated": updated})


@app.route("/api/domain/stats", methods=["GET"])
def domain_stats():
    """Statistics for domain pool."""
    stats = {"total": db.count("domain_pool")}
    for s in DOMAIN_STATUSES:
        stats[s.lower()] = db.count("domain_pool", filters={"collection_status": s})

    # Today's new
    today = datetime.now().strftime("%Y-%m-%d")
    stats["today_new"] = db.count("domain_pool",
                                   filters={"created_at": f"gte.{today}"})

    # By user
    users = defaultdict(int)
    domains = db.select("domain_pool", select="claimed_by",
                         filters={"claimed_by": "not.is.null"}, limit=5000)
    for d in domains:
        users[d.get("claimed_by") or "unknown"] += 1
    stats["by_user"] = dict(users)

    return jsonify(stats)


# ════════════════════════════════════════════════════════════
# Email Pool API (derived from domain_pool contact_email)
# ════════════════════════════════════════════════════════════

EMAIL_STATUSES = ["New", "Unsent", "Assigned", "Sent", "Bounce"]


@app.route("/api/email/queue", methods=["GET"])
def email_queue():
    """
    Get UNSENT emails ready for sending from email_pool table.
    Filter: send_status='UNSENT', ordered by email_id (FIFO).
    """
    user = request.args.get("user", "")
    count = min(int(request.args.get("count", 100)), 2000)

    filters = {"send_status": "UNSENT"}

    emails = db.select(
        "email_pool",
        select="email_id,email,domain,send_status,collection_status,claimed_by,source,notes,created_at",
        filters=filters,
        limit=count,
        order="email_id",
        ascending=True,
    )

    result = []
    for e in (emails or []):
        e["contact_email"] = e.get("email")  # for frontend compatibility
        result.append(e)

    return jsonify({"emails": result, "count": len(result)})


@app.route("/api/email/export", methods=["POST"])
def email_export():
    """
    Export UNSENT emails from email_pool for sending.
      - Exports emails with send_status='UNSENT'.
      - Different users get different emails (claimed_by locking).
      - Marks send_status='SENT', source='已提取' after export.
    """
    data = request.get_json(force=True)
    user = data.get("user", "").strip()
    if not user:
        return jsonify({"error": "user is required", "exported": 0}), 400
    count = min(int(data.get("count", 500)), 5000)

    # Only unsent and unclaimed (prevent duplicate export race condition)
    filters = {"send_status": "UNSENT", "claimed_by": "is.null"}

    emails = db.select(
        "email_pool",
        select="email_id,email,domain,send_status,collection_status,source,claimed_by",
        filters=filters,
        limit=count,
        order="email_id",
        ascending=True,
    )

    if not emails:
        return jsonify({"exported": 0, "filename": ""})

    batch_id = f"email_send_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{user}"
    filename = f"{batch_id}.csv"
    now = now_iso()

    # Generate CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["email_id", "email", "domain", "send_status", "source"])
    for e in emails:
        writer.writerow([
            e["email_id"],
            safe_str(e.get("email")),
            e["domain"],
            e.get("send_status", "UNSENT"),
            safe_str(e.get("source")),
        ])

    csv_content = output.getvalue()
    export_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "shared_pool_exports")
    os.makedirs(export_dir, exist_ok=True)
    with open(os.path.join(export_dir, filename), "w", newline="", encoding="utf-8") as f:
        f.write(csv_content)

    # Lock: mark as SENT + extracted, assign to user
    ids = [e["email_id"] for e in emails]
    resp, result = db.patch_by_ids("email_pool", {
        "send_status": "SENT",
        "source": "已提取",
        "claimed_by": user,
        "claim_time": now,
        "updated_at": now,
    }, ids, id_column="email_id")
    
    # Verify update succeeded
    if resp and hasattr(resp, "status") and resp.status in (200, 201, 204):
        updated_count = len(result) if isinstance(result, list) else len(emails)
    else:
        updated_count = 0
        print(f"[WARN] email_export PATCH failed: resp={resp}, result={result}")

    global _cache
    _cache.clear()
    _log_operation("email_export", user, "email_pool", len(emails),
                   f"Batch: {batch_id}, Source: 已提取")

    return jsonify({"exported": len(emails), "filename": filename, "batch_id": batch_id, "csv_content": csv_content})


@app.route("/api/email/stats", methods=["GET"])
def email_stats():
    """Email pool statistics (from email_pool table)."""
    total = db.count("email_pool")
    unsent = db.count("email_pool", filters={"send_status": "UNSENT"})
    sent = db.count("email_pool", filters={"send_status": "SENT"})
    bounce = db.count("email_pool", filters={"send_status": "Bounce"})

    return jsonify({
        "total": total,
        "unsent": unsent,
        "sent": sent,
        "bounce": bounce,
    })


@app.route("/api/email/import", methods=["POST"])
def email_pool_import():
    """
    Batch import emails to independent email_pool table.
    Body: {"emails": [{"email":"a@b.com","domain":"b.com"},...], "imported_by": "leo"}
    Batch dedup via in.() filter (performance: 1 API call per 100 emails).
    All new entries marked send_status='UNSENT', source='未提取', collection_status='New'.
    """
    global _cache
    data = request.get_json(force=True)
    records = data.get("emails", [])
    imported_by = data.get("imported_by", "").strip()
    if not imported_by:
        return jsonify({"error": "imported_by is required"}), 400

    if not records:
        return jsonify({"imported": 0, "new": 0, "skipped": 0})

    import_batch = f"import_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{imported_by}"
    notes = f"[email imported by {imported_by}]"

    # 1. Normalize & dedup within batch
    seen = {}
    for rec in records:
        email = rec.get("email", "").strip().lower()
        domain = rec.get("domain", "").strip().lower().lstrip("www.")
        if not email or not domain or "@" not in email:
            continue
        if email not in seen:
            seen[email] = {
                "email": email,
                "domain": domain,
                "send_status": "UNSENT",
                "source": "未提取",
                "collection_status": "New",
                "notes": notes,
                "import_batch": import_batch,
                "priority": 0,
            }

    if not seen:
        return jsonify({"imported": 0, "new": 0, "skipped": 0})

    # 2. Batch check against existing emails (100 per query)
    email_list = list(seen.keys())
    existing_emails = set()
    dedup_errors = 0
    for i in range(0, len(email_list), 100):
        batch = email_list[i:i+100]
        e_filter = ",".join(batch)
        try:
            results = db.select("email_pool", select="email",
                              filters={"email": f"in.({e_filter})"}, limit=100)
            for r in (results or []):
                existing_emails.add((r.get("email") or "").strip().lower())
        except Exception as e:
            dedup_errors += 1
            print(f"[email_import] dedup query failed (batch {i}): {e}", file=sys.stderr)

    # 3. Filter out existing (only trust dedup if no errors)
    new_emails = {e: v for e, v in seen.items() if e not in existing_emails}
    skipped = len(seen) - len(new_emails)

    if dedup_errors:
        print(f"[email_import] WARNING: {dedup_errors} dedup queries failed. "
              f"Fallback: using server-side upsert. Skipped(counted): {skipped}", file=sys.stderr)
        # Can't trust skipped count if dedup failed — actual skipped determined by server
        skipped = 0

    if not new_emails:
        _cache.clear()
        _log_operation("email_import", imported_by, "email_pool", 0,
                       f"All {skipped} emails already exist, Source: 未提取")
        return jsonify({"imported": 0, "skipped": skipped})

    # 4. Insert new emails (upsert as safety net for dedup failures)
    rows = list(new_emails.values()) if new_emails else list(seen.values())
    resp, result = db.insert("email_pool", rows, upsert=True)
    imported = 0
    if hasattr(resp, "status") and resp.status in (200, 201):
        # With return=representation, result contains actually inserted rows
        imported = len(result) if isinstance(result, list) else 0
    elif hasattr(resp, "code"):
        print(f"[email_import] insert failed HTTP {resp.code}: {result}", file=sys.stderr)
    else:
        # Should not happen with return=representation, but keep fallback
        imported = 0
        skipped = len(seen)

    # 5. Clear cache & log
    _cache.clear()
    _log_operation("email_import", imported_by, "email_pool", imported,
                   f"Source: 未提取, Skipped(dup): {skipped}, Batch: {import_batch}")

    return jsonify({"imported": imported, "skipped": skipped})


# ════════════════════════════════════════════════════════════
# Reply Pool API (→ reply_pool table, fallback to supplier_pool)
# ════════════════════════════════════════════════════════════

def _parse_reply_category(notes):
    """Parse A/B/C category from supplier_pool notes field."""
    if not notes:
        return "C"
    notes = str(notes)
    if "分类:A" in notes or "有合作意向" in notes:
        return "A"
    if "分类:B" in notes:
        return "B"
    if "分类:C" in notes or "一般回复" in notes or "历史回复" in notes:
        return "C"
    return "C"


def _supplier_to_reply(s):
    """Convert a supplier_pool row to reply_pool format."""
    email = s.get("contact_email") or s.get("supplier_name") or ""
    domain = email.split("@")[-1] if "@" in email else ""
    return {
        "reply_id": s.get("supplier_id"),
        "email": email,
        "domain": domain,
        "reply_content": s.get("notes", ""),
        "reply_time": s.get("created_at"),
        "category": _parse_reply_category(s.get("notes")),
        "status": "New",
        "supplier": s.get("supplier_name", ""),
        "contact_email": email,
        "discovered_by": s.get("source", "system"),
        "discovered_at": s.get("created_at"),
        "replied_by": None,
        "replied_at": None,
        "notes": s.get("notes", ""),
    }


@app.route("/api/reply/import", methods=["POST"])
def reply_import():
    """
    Import replies from Excel/CSV upload.
    Template columns: 序号,邮箱,域名,账号,发件人,主题,正文摘要,日期
    Deduplicates by email (skip existing).
    """
    user = request.form.get("user", "unknown").strip()
    file = request.files.get("file")
    if not file:
        return jsonify({"error": "No file uploaded"}), 400

    try:
        filename = file.filename.lower()
        if filename.endswith('.csv'):
            content = file.read().decode('utf-8-sig')
            reader = csv.DictReader(io.StringIO(content))
            rows = list(reader)
        else:
            # Excel: use openpyxl
            wb = openpyxl.load_workbook(io.BytesIO(file.read()))
            ws = wb.active
            headers = [str(c.value or '').strip() for c in ws[1]]
            rows = []
            for row in ws.iter_rows(min_row=2, values_only=True):
                rows.append(dict(zip(headers, [str(v or '') for v in row])))
    except Exception as e:
        return jsonify({"error": f"File parse error: {str(e)}"}), 400

    if not rows:
        return jsonify({"error": "Empty file"}), 400

    # Normalize column names
    col_map = {}
    for col in rows[0].keys():
        cl = col.lower().strip()
        if any(k in cl for k in ['邮箱', 'email', 'mail']): col_map[col] = 'email'
        elif any(k in cl for k in ['域名', 'domain']): col_map[col] = 'domain'
        elif any(k in cl for k in ['账号', 'account', 'inbox', 'discovered']): col_map[col] = 'account'
        elif any(k in cl for k in ['发件人', 'from', 'supplier', 'name']): col_map[col] = 'supplier'
        elif any(k in cl for k in ['主题', 'subject', 'title']): col_map[col] = 'subject'
        elif any(k in cl for k in ['正文', 'body', 'content', 'preview', '摘要']): col_map[col] = 'body'
        elif any(k in cl for k in ['日期', 'date', 'time', 'reply_time']): col_map[col] = 'date'

    # Get existing emails for dedup (paginated fetch)
    existing = set()
    try:
        page, page_size = 0, 1000
        while True:
            resp = requests.get(
                f"{SUPABASE_URL}/rest/v1/reply_pool?select=email&limit={page_size}&offset={page*page_size}",
                headers=AUTH_HEADERS,
                timeout=30
            )
            if resp.status_code != 200:
                break
            data = resp.json()
            if not data:
                break
            for r in data:
                if r.get('email'):
                    existing.add(r['email'].lower())
            if len(data) < page_size:
                break
            page += 1
    except Exception:
        pass  # if can't check, rely on UNIQUE(email) constraint

    imported = 0
    skipped = 0
    batch = []
    BATCH_SIZE = 50

    def flush_batch():
        nonlocal imported
        if not batch:
            return
        for attempt in range(3):
            try:
                resp = requests.post(
                    f"{SUPABASE_URL}/rest/v1/reply_pool",
                    headers={**AUTH_HEADERS, 'Prefer': 'return=representation'},
                    json=batch,
                    timeout=30
                )
                if resp.status_code in (200, 201):
                    imported += len(batch)
                    break
            except Exception:
                if attempt < 2:
                    _time.sleep(1)
        batch.clear()

    for row in rows:
        email = _extract_email(row, col_map)
        if not email:
            continue
        email = email.lower()
        if email in existing:
            skipped += 1
            continue
        existing.add(email)  # dedup within this batch too

        supplier = row.get(col_map.get('supplier', ''), '') or ''
        domain = row.get(col_map.get('domain', ''), '') or email.split('@')[-1]
        discovered = row.get(col_map.get('account', ''), '') or user
        subject = row.get(col_map.get('subject', ''), '') or ''
        body = row.get(col_map.get('body', ''), '') or ''
        date_str = row.get(col_map.get('date', ''), '') or ''

        reply_time = now_iso()
        if date_str:
            try:
                for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y/%m/%d', '%a, %d %b %Y %H:%M:%S %z']:
                    try:
                        reply_time = datetime.strptime(date_str.strip(), fmt).isoformat()
                        break
                    except ValueError:
                        continue
            except Exception:
                pass

        batch.append({
            "email": email,
            "domain": domain.lower(),
            "reply_content": f"{subject}\n\n{body}"[:5000],
            "reply_time": reply_time,
            "category": "C",
            "status": "New",
            "supplier": supplier[:200],
            "contact_email": email,
            "discovered_by": discovered[:100],
            "discovered_at": now_iso(),
            "notes": f"Imported by {user}",
        })

        if len(batch) >= BATCH_SIZE:
            flush_batch()

    flush_batch()

    # Log operation
    _log_operation("reply_import", user, "reply_pool", imported,
                   f"Imported {imported} replies" + (f", skipped {skipped} duplicates" if skipped else ""))

    return jsonify({"imported": imported, "skipped": skipped, "total": len(rows)})


def _extract_email(row, col_map):
    """Extract email from various field formats."""
    # Try email column first
    email_col = col_map.get('email')
    if email_col:
        val = str(row.get(email_col, '') or '').strip()
        if '@' in val:
            return val.split()[0].strip() if ' ' in val else val

    # Try from/supplier column (may have format "Name <email>")
    supplier_col = col_map.get('supplier')
    if supplier_col:
        val = str(row.get(supplier_col, '') or '').strip()
        m = _re.search(r'<([^>]+@[^>]+)>', val)
        if m:
            return m.group(1)

    return None


@app.route("/api/reply/add", methods=["POST"])
def reply_add():
    """
    Record a new reply from a supplier.
    Body: {"email":"a@x.com","domain":"x.com","content":"...","category":"A",
           "supplier":"John","supplier_email":"a@x.com","discovered_by":"emma"}
    """
    data = request.get_json(force=True)

    payload = {
        "email": data["email"].lower().strip(),
        "domain": data.get("domain", ""),
        "reply_content": (data.get("content") or data.get("reply_content") or "")[:2000],
        "reply_time": data.get("reply_time", now_iso()),
        "category": data.get("category", "C").upper(),
        "status": "New",
        "supplier": data.get("supplier", ""),
        "contact_email": data.get("supplier_email", data["email"]),
        "discovered_by": data.get("discovered_by", "system"),
        "discovered_at": now_iso(),
    }

    resp, result = db.insert("reply_pool", payload)
    if hasattr(resp, "status") and resp.status in (200, 201):
        reply_id = result[0].get("reply_id") if isinstance(result, list) and result else None
        return jsonify({"result": "created", "reply_id": reply_id})
    else:
        return jsonify({"result": "error", "detail": str(result)}), 500


@app.route("/api/reply/list", methods=["GET"])
def reply_list():
    """List replies with filtering.
    Falls back to supplier_pool if reply_pool is empty (legacy data)."""
    category = request.args.get("category", "")
    status = request.args.get("status", "")
    limit = min(int(request.args.get("limit", 100)), 5000)

    # Try reply_pool first
    filters = {}
    if category:
        filters["category"] = category.upper()
    if status:
        filters["status"] = status

    replies = db.select(
        "reply_pool",
        select="*",
        filters=filters,
        limit=limit,
        order="discovered_at",
        ascending=False,
    )

    # Fallback to supplier_pool (legacy data from shared-pool-tools.exe)
    if not replies:
        sf = {"status": "Replied"}
        suppliers = db.select(
            "supplier_pool",
            select="supplier_id,supplier_name,contact_email,source,notes,created_at",
            filters=sf,
            limit=limit,
            order="created_at",
            ascending=False,
        )
        replies = [_supplier_to_reply(s) for s in (suppliers or [])]
        if category:
            replies = [r for r in replies if r["category"] == category.upper()]

    return jsonify({"replies": replies or []})


@app.route("/api/reply/stats", methods=["GET"])
def reply_stats():
    """Reply statistics. Falls back to supplier_pool if reply_pool is empty."""
    try:
        total = db.count("reply_pool")
        if total > 0:
            a_count = db.count("reply_pool", filters={"category": "A"})
            b_count = db.count("reply_pool", filters={"category": "B"})
            c_count = db.count("reply_pool", filters={"category": "C"})
            new_count = db.count("reply_pool", filters={"status": "New"})
        else:
            # Fallback: count from supplier_pool (legacy data)
            total = db.count("supplier_pool", filters={"status": "Replied"})
            # Sample up to 1000 for classification (Supabase REST limit)
            suppliers = db.select(
                "supplier_pool",
                select="notes",
                filters={"status": "Replied"},
                limit=1000,
            )
            a_count = sum(1 for s in suppliers if _parse_reply_category(s.get("notes")) == "A")
            b_count = sum(1 for s in suppliers if _parse_reply_category(s.get("notes")) == "B")
            c_count = sum(1 for s in suppliers if _parse_reply_category(s.get("notes")) == "C")
            # Scale up proportionally if we hit the 1000 limit
            if total > 1000 and suppliers:
                scale = total / len(suppliers)
                a_count = int(a_count * scale)
                b_count = int(b_count * scale)
                c_count = int(c_count * scale)
            new_count = total  # All legacy replies are treated as unread
    except Exception:
        return jsonify({
            "total": 0, "a_class": 0, "b_class": 0, "c_class": 0,
            "unread": 0, "replied": 0, "today": 0, "a_today": 0,
            "note": "reply_pool table may not exist. Run setup to create it."
        })

    return jsonify({
        "total": total,
        "unread": new_count,
        "replied": total - new_count,
        "a_class": a_count,
        "b_class": b_count,
        "c_class": c_count,
        "today": 0,
        "a_today": 0,
    })


@app.route("/api/reply/update_status", methods=["POST"])
def reply_update_status():
    """Update reply status."""
    data = request.get_json(force=True)
    reply_id = data.get("id")

    resp, _ = db.update("reply_pool", {
        "status": data.get("status", "Resolved"),
        "replied_at": now_iso(),
    }, {"reply_id": reply_id})
    return jsonify({"updated": 1})


# ════════════════════════════════════════════════════════════
# Quote Pool API (→ quote_pool table)
# ════════════════════════════════════════════════════════════

@app.route("/api/price/add", methods=["POST"])
@app.route("/api/quote/add", methods=["POST"])
def quote_add():
    """Add a quote record."""
    data = request.get_json(force=True)
    payload = {
        "email": data.get("email", ""),
        "domain": data.get("domain", ""),
        "supplier": data.get("supplier", ""),
        "contact_email": data.get("contact_email", ""),
        "niche": data.get("niche", ""),
        "country": data.get("country", ""),
        "traffic": data.get("traffic", ""),
        "site_category": data.get("site_category", ""),
        "cooperation_type": data.get("cooperation_type", ""),
        "price": data.get("price", ""),
        "link_rules": data.get("link_rules", ""),
        "permanence": data.get("permanence", ""),
        "content": data.get("content", ""),
        "tat": data.get("tat", ""),
        "payment": data.get("payment", ""),
        "discount": data.get("discount", ""),
        "additional_services": data.get("additional_services", ""),
        "requirements": data.get("requirements", ""),
        "reply_id": data.get("reply_id"),
        "reply_content": data.get("reply_content", ""),
        "status": data.get("status", "New"),
        "priority": data.get("priority", 0),
        "notes": data.get("notes", ""),
        "discovered_by": data.get("discovered_by", "manual"),
    }
    resp, result = db.insert("quote_pool", payload)
    return jsonify({"result": "created"})


@app.route("/api/price/list", methods=["GET"])
@app.route("/api/quote/list", methods=["GET"])
def quote_list():
    """List quotes with optional filters."""
    domain = request.args.get("domain", "")
    status = request.args.get("status", "")
    niche = request.args.get("niche", "")
    limit = min(int(request.args.get("limit", 100)), 1000)

    filters = {}
    if domain:
        filters["domain"] = domain
    if status:
        filters["status"] = status
    if niche:
        filters["niche"] = niche

    quotes = db.select(
        "quote_pool",
        select="*",
        filters=filters,
        limit=limit,
        order="discovered_at",
        ascending=False,
    )
    return jsonify({"quotes": quotes or []})


@app.route("/api/price/stats", methods=["GET"])
@app.route("/api/quote/stats", methods=["GET"])
def quote_stats():
    """Quote pool statistics."""
    total = db.count("quote_pool")
    suppliers = 0
    if total > 0:
        quotes = db.select("quote_pool", select="supplier", limit=5000)
        suppliers = len(set(q.get("supplier") for q in quotes if q.get("supplier")))

    # Count by status
    status_counts = {}
    if total > 0:
        quotes = db.select("quote_pool", select="status", limit=5000)
        from collections import Counter
        status_counts = dict(Counter(q.get("status") for q in quotes if q.get("status")))

    return jsonify({
        "total": total,
        "suppliers": suppliers,
        "today_new": 0,
        "by_status": status_counts,
    })


@app.route("/api/price/export", methods=["GET"])
@app.route("/api/quote/export", methods=["GET"])
def quote_export():
    """Export quotes as CSV."""
    quotes = db.select("quote_pool", select="*", limit=10000,
                       order="discovered_at", ascending=False)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["domain", "email", "supplier", "contact_email", "niche", "country",
                     "traffic", "cooperation_type", "price", "link_rules", "permanence",
                     "content", "tat", "payment", "discount", "additional_services",
                     "requirements", "status", "notes", "discovered_at"])
    for q in (quotes or []):
        writer.writerow([
            q.get("domain", ""), q.get("email", ""), q.get("supplier", ""),
            q.get("contact_email", ""), q.get("niche", ""), q.get("country", ""),
            q.get("traffic", ""), q.get("cooperation_type", ""), q.get("price", ""),
            q.get("link_rules", ""), q.get("permanence", ""), q.get("content", ""),
            q.get("tat", ""), q.get("payment", ""), q.get("discount", ""),
            q.get("additional_services", ""), q.get("requirements", ""),
            q.get("status", ""), q.get("notes", ""),
            safe_str(q.get("discovered_at", ""))[:19],
        ])

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=quote_pool_export.csv"}
    )


@app.route("/api/quote/import-a-replies", methods=["POST"])
def quote_import_a_replies():
    """
    从 reply_pool A类回复导入到 quote_pool。
    每条回复作为 quote 记录，reply_content 完整保留供人工解析。
    已导入的(email)不会重复导入。
    """
    user = request.form.get("user", "unknown").strip()
    force = request.form.get("force", "") == "1"  # force re-import all

    # Get all A类 replies
    page, page_size = 0, 1000
    all_replies = []
    while True:
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/reply_pool?category=eq.A&select=*&limit={page_size}&offset={page*page_size}",
            headers=AUTH_HEADERS,
            timeout=30
        )
        if resp.status_code != 200:
            break
        data = resp.json()
        if not data:
            break
        all_replies.extend(data)
        if len(data) < page_size:
            break
        page += 1

    # Get existing quote emails
    existing_emails = set()
    page = 0
    while True:
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/quote_pool?select=email&limit={page_size}&offset={page*page_size}",
            headers=AUTH_HEADERS,
            timeout=30
        )
        if resp.status_code != 200:
            break
        data = resp.json()
        if not data:
            break
        for r in data:
            if r.get("email"):
                existing_emails.add(r["email"].lower())
        if len(data) < page_size:
            break
        page += 1

    imported = 0
    skipped = 0
    batch = []
    BATCH_SIZE = 30

    def flush():
        nonlocal imported
        if not batch:
            return
        for attempt in range(3):
            try:
                resp = requests.post(
                    f"{SUPABASE_URL}/rest/v1/quote_pool",
                    headers={**AUTH_HEADERS, "Prefer": "return=representation"},
                    json=batch,
                    timeout=30
                )
                if resp.status_code in (200, 201):
                    imported += len(batch)
                    break
            except Exception:
                if attempt < 2:
                    _time.sleep(1)
        batch.clear()

    for reply in all_replies:
        email = (reply.get("email") or "").lower()
        if not email:
            continue
        if not force and email in existing_emails:
            skipped += 1
            continue
        existing_emails.add(email)

        batch.append({
            "email": email,
            "domain": (reply.get("domain") or email.split("@")[-1]).lower(),
            "supplier": (reply.get("supplier") or "")[:200],
            "contact_email": reply.get("contact_email") or email,
            "niche": "",
            "country": "",
            "traffic": "",
            "site_category": "",
            "cooperation_type": "",
            "price": "",
            "link_rules": "",
            "permanence": "",
            "content": "",
            "tat": "",
            "payment": "",
            "discount": "",
            "additional_services": "",
            "requirements": "",
            "reply_id": reply.get("reply_id"),
            "reply_content": (reply.get("reply_content") or "")[:8000],
            "status": "New",
            "priority": 0,
            "notes": f"Imported from A-class reply by {user}",
            "discovered_by": user,
            "discovered_at": now_iso(),
        })

        if len(batch) >= BATCH_SIZE:
            flush()

    flush()

    _log_operation("quote_import", user, "quote_pool", imported,
                   f"Imported {imported} A-class replies" + (f", skipped {skipped} duplicates" if skipped else ""))

    return jsonify({
        "imported": imported,
        "skipped": skipped,
        "total_a_replies": len(all_replies),
        "quote_pool_total": db.count("quote_pool"),
    })


# ════════════════════════════════════════════════════════════
# Comprehensive Stats
# ════════════════════════════════════════════════════════════

@app.route("/api/admin/clear-cache", methods=["POST"])
def admin_clear_cache():
    """Clear all in-memory caches (stats, unique domains, config)."""
    global _cache, CONFIG_CACHE
    _cache.clear()
    CONFIG_CACHE.clear()
    return jsonify({"status": "ok", "message": "All caches cleared"})

@app.route("/api/stats", methods=["GET"])
def get_stats():
    """Aggregated statistics (cached 60s). Optimized to avoid slow full-table scans."""

    def _fetch():
        # Domain counts by status — each count() hits Supabase content-range (fast)
        domain_total = db.count("domain_pool")
        domain_new = db.count("domain_pool", filters={"collection_status": "New"})
        domain_claimed = db.count("domain_pool", filters={"collection_status": "Claimed"})
        domain_contacted = db.count("domain_pool", filters={"collection_status": "Contacted"})
        domain_replied = db.count("domain_pool", filters={"collection_status": "Replied"})

        # Skip _count_unique_domains() — it paginates 167K rows (~30-60s).
        # Use total count as approximate unique count for the dashboard.
        domain_unique_total = domain_total
        domain_unique_new = domain_new
        domain_unique_claimed = domain_claimed
        domain_unique_contacted = domain_contacted
        domain_unique_replied = domain_replied

        # Email pool stats — from email_pool table (independent table)
        # send_status: UNSENT = available, SENT = exported, Bounce = bounced
        try:
            email_total = db.count("email_pool")
            email_unsent = db.count("email_pool", filters={"send_status": "UNSENT"})
            email_sent = db.count("email_pool", filters={"send_status": "SENT"})
            email_exported = db.count("email_pool", filters={"send_status": "EXPORTED"})
            email_bounce = db.count("email_pool", filters={"send_status": "Bounce"})
            # claimed_by may not exist on email_pool; wrap safely
            try:
                email_assigned = db.count("email_pool", filters={"claimed_by": "not.is.null"})
            except Exception:
                email_assigned = 0
            # SENT + EXPORTED both represent "already used / sent"
            email_sent_total = email_sent + email_exported
        except Exception:
            # Fallback to domain_pool mapping if email_pool table doesn't exist
            email_total = domain_total
            email_unsent = domain_new + domain_claimed
            email_sent_total = domain_contacted
            email_assigned = domain_claimed
            email_bounce = 0

        reply_total = reply_a = reply_b = reply_c = reply_unread = 0
        try:
            reply_total = db.count("reply_pool")
            if reply_total > 0:
                reply_a = db.count("reply_pool", filters={"category": "A"})
                reply_b = db.count("reply_pool", filters={"category": "B"})
                reply_c = db.count("reply_pool", filters={"category": "C"})
                reply_unread = db.count("reply_pool", filters={"status": "New"})
            else:
                # Fallback to supplier_pool legacy data
                reply_total = db.count("supplier_pool", filters={"status": "Replied"})
                if reply_total > 0:
                    suppliers = db.select(
                        "supplier_pool",
                        select="notes",
                        filters={"status": "Replied"},
                        limit=500,
                    )
                    reply_a = sum(1 for s in suppliers if _parse_reply_category(s.get("notes")) == "A")
                    reply_b = sum(1 for s in suppliers if _parse_reply_category(s.get("notes")) == "B")
                    reply_c = sum(1 for s in suppliers if _parse_reply_category(s.get("notes")) == "C")
                    if reply_total > 500 and suppliers:
                        scale = reply_total / len(suppliers)
                        reply_a = int(reply_a * scale)
                        reply_b = int(reply_b * scale)
                        reply_c = int(reply_c * scale)
                    reply_unread = reply_total
        except Exception:
            pass

        quote_total = db.count("quote_pool")

        return {
            "domain_total": domain_total,
            "domain_new": domain_new,
            "domain_claimed": domain_claimed,
            "domain_imported": 0,
            "domain_completed": domain_contacted,
            "domain_exported": domain_replied,
            "domain_unique_total": domain_unique_total,
            "domain_unique_new": domain_unique_new,
            "domain_unique_claimed": domain_unique_claimed,
            "domain_unique_contacted": domain_unique_contacted,
            "domain_unique_replied": domain_unique_replied,
            "domain_today_new": 0,
            "email_total": email_total,
            "email_unsent": email_unsent,
            "email_assigned": email_assigned,
            "email_sent": email_sent_total,
            "email_bounce": email_bounce,
            "reply_total": reply_total,
            "reply_unread": reply_unread,
            "reply_a": reply_a,
            "reply_b": reply_b,
            "reply_c": reply_c,
            "reply_today": 0,
            "reply_a_today": 0,
            "quote_total": quote_total,
            "quote_today_new": 0,
            "quote_suppliers": 0,
        }

    return jsonify(_cached("stats", ttl_sec=60, fn=_fetch))


@app.route("/api/members", methods=["GET"])
def api_members():
    """Team member stats."""
    domains = db.select("domain_pool",
                        select="claimed_by",
                        filters={"claimed_by": "not.is.null"},
                        limit=10000)
    users = defaultdict(lambda: {"domains": 0, "emails_assigned": 0, "emails_sent": 0})
    for d in (domains or []):
        u = d.get("claimed_by") or "unknown"
        users[u]["domains"] += 1

    return jsonify([
        {"username": u, **stats}
        for u, stats in sorted(users.items(), key=lambda x: -x[1]["domains"])
    ])


# ════════════════════════════════════════════════════════════
# Config API (→ config table as key-value store)
# ════════════════════════════════════════════════════════════

CONFIG_CACHE = {}  # simple in-memory cache for config

def _get_config(key, default=None):
    """Read a config value from Supabase, with local cache."""
    cache_key = f"config:{key}"
    now = _time.time()
    if cache_key in CONFIG_CACHE and CONFIG_CACHE[cache_key][0] > now:
        return CONFIG_CACHE[cache_key][1]
    try:
        rows = db.select("config", select="value", filters={"key": key}, limit=1)
        val = rows[0]["value"] if rows else default
    except Exception:
        val = default
    CONFIG_CACHE[cache_key] = (now + 30, val)
    return val


def _set_config(key, value, description=""):
    """Upsert a config value via POST (update if exists, insert if new)."""
    existing = db.select("config", select="key", filters={"key": key}, limit=1)
    if existing:
        db.update("config", {"value": value, "updated_at": now_iso()}, {"key": key})
    else:
        db.insert("config", {"key": key, "value": value, "description": description})
    CONFIG_CACHE[f"config:{key}"] = (_time.time() + 30, value)
    return True


@app.route("/api/config/team", methods=["GET", "POST"])
def config_team():
    """Get or set team members list (comma-separated or JSON array).
    GET → {"members": ["leo","emma","jack"], "raw": "leo,emma,jack"}
    POST {"members": ["leo","emma","jack"]} → save to config"""
    if request.method == "GET":
        raw = _get_config("team_members", "leo,emma,jack")
        # Parse: try JSON array, fallback to comma-sep
        try:
            members = json.loads(raw) if raw else []
        except (json.JSONDecodeError, TypeError):
            members = [m.strip() for m in str(raw).split(",") if m.strip()]
        return jsonify({"members": members, "raw": raw})

    # POST: update team members
    data = request.get_json(force=True)
    members = data.get("members", [])
    if isinstance(members, str):
        members = [m.strip() for m in members.split(",") if m.strip()]
    if not members:
        return jsonify({"error": "members is required"}), 400
    raw = json.dumps(members, ensure_ascii=False)
    _set_config("team_members", raw, "Team member usernames for domain distribution")
    # Clear distribute cache
    CONFIG_CACHE.pop("team_members_list", None)
    return jsonify({"members": members, "saved": True})


@app.route("/api/domain/import_log", methods=["GET"])
def domain_import_log():
    """Recent import records (domains with notes containing 'imported by')."""
    limit = min(int(request.args.get("limit", 50)), 500)
    domains = db.select(
        "domain_pool",
        select="domain_id,domain,source,notes,created_at",
        filters={"notes": "like.[imported by%]"},
        limit=limit,
        order="created_at",
        ascending=False,
    )
    # Parse imported_by from notes field
    result = []
    for d in (domains or []):
        notes = d.get("notes", "")
        imported_by = ""
        if "[imported by " in notes:
            imported_by = notes.split("[imported by ")[1].split("]")[0]
        result.append({
            "domain_id": d.get("domain_id"),
            "domain": d.get("domain"),
            "imported_by": imported_by,
            "notes": notes,
            "imported_at": d.get("created_at", ""),
        })
    return jsonify({"imports": result, "count": len(result)})


@app.route("/api/log/list", methods=["GET"])
def log_list():
    """List operation logs (import/export/distribute)."""
    limit = min(int(request.args.get("limit", 100)), 1000)
    op_type = request.args.get("type", "")
    user = request.args.get("user", "")

    try:
        raw = _get_config("operation_logs", "[]")
        logs = json.loads(raw) if raw else []
    except Exception:
        logs = []

    # Filter
    if op_type:
        logs = [l for l in logs if l.get("type") == op_type]
    if user:
        logs = [l for l in logs if l.get("user") == user]

    logs = logs[:limit]

    # Convert legacy UTC timestamps to Beijing time for display
    for l in logs:
        if "time" in l:
            l["time"] = _utc_to_bj(l["time"])

    return jsonify({"logs": logs, "count": len(logs)})


# ════════════════════════════════════════════════════════════

EMAIL_POOL_SQL = """-- Run this in Supabase SQL Editor to create the email_pool table:
-- (This is the independent email pool — separate from domain_pool)

CREATE TABLE IF NOT EXISTS email_pool (
    email_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email               TEXT NOT NULL,
    domain              TEXT NOT NULL,
    send_status         TEXT DEFAULT 'UNSENT',
    source              TEXT DEFAULT '未提取',
    collection_status   TEXT DEFAULT 'New',
    claimed_by          TEXT,
    claim_time          TIMESTAMPTZ,
    import_batch        TEXT,
    notes               TEXT,
    priority            INT DEFAULT 0,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Unique index on email (dedup key)
CREATE UNIQUE INDEX IF NOT EXISTS idx_email_pool_email ON email_pool(email);
CREATE INDEX IF NOT EXISTS idx_email_pool_domain ON email_pool(domain);
CREATE INDEX IF NOT EXISTS idx_email_pool_status ON email_pool(send_status);
CREATE INDEX IF NOT EXISTS idx_email_pool_collection ON email_pool(collection_status);

-- Enable RLS for email_pool
ALTER TABLE email_pool ENABLE ROW LEVEL SECURITY;

-- Allow anon reads
CREATE POLICY "anon_can_read_emails" ON email_pool
    FOR SELECT USING (true);

-- Allow anon inserts
CREATE POLICY "anon_can_insert_emails" ON email_pool
    FOR INSERT WITH CHECK (true);

-- Allow anon updates
CREATE POLICY "anon_can_update_emails" ON email_pool
    FOR UPDATE USING (true);


-- Optional: migrate existing email data from domain_pool to email_pool
-- Uncomment and run if you have existing contact_email data in domain_pool:

/*
INSERT INTO email_pool (email, domain, source, collection_status, claimed_by, claim_time, notes, priority, created_at)
SELECT DISTINCT ON (LOWER(TRIM(contact_email)))
  LOWER(TRIM(contact_email)) as email,
  LOWER(TRIM(domain)) as domain,
  COALESCE(source, '未提取') as source,
  COALESCE(collection_status, 'New') as collection_status,
  claimed_by,
  claim_time,
  notes,
  COALESCE(priority, 0) as priority,
  created_at
FROM domain_pool
WHERE contact_email IS NOT NULL AND TRIM(contact_email) != ''
ON CONFLICT (email) DO NOTHING;
*/
"""

SETUP_SQL = """-- Run this in Supabase SQL Editor to create the reply_pool table:

CREATE TABLE IF NOT EXISTS reply_pool (
    reply_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email               TEXT NOT NULL,
    domain              TEXT,
    reply_content       TEXT,
    reply_time          TIMESTAMPTZ DEFAULT NOW(),
    category            TEXT DEFAULT 'C',
    status              TEXT DEFAULT 'New',
    supplier            TEXT,
    contact_email       TEXT,
    discovered_by       TEXT DEFAULT 'system',
    discovered_at       TIMESTAMPTZ DEFAULT NOW(),
    replied_by          TEXT,
    replied_at          TIMESTAMPTZ,
    notes               TEXT
);

CREATE INDEX IF NOT EXISTS idx_reply_category ON reply_pool(category);
CREATE INDEX IF NOT EXISTS idx_reply_status ON reply_pool(status);
CREATE INDEX IF NOT EXISTS idx_reply_domain ON reply_pool(domain);

-- Enable RLS for reply_pool
ALTER TABLE reply_pool ENABLE ROW LEVEL SECURITY;

-- Allow anon reads
CREATE POLICY "anon_can_read_replies" ON reply_pool
    FOR SELECT USING (true);

-- Allow anon inserts
CREATE POLICY "anon_can_insert_replies" ON reply_pool
    FOR INSERT WITH CHECK (true);

-- Allow anon updates
CREATE POLICY "anon_can_update_replies" ON reply_pool
    FOR UPDATE USING (true);
"""


@app.route("/setup")
def setup_page():
    """Show setup instructions."""
    tables_found = {}
    for tbl in ["domain_pool", "email_pool", "supplier_pool", "quote_pool", "reply_pool", "config"]:
        try:
            cnt = db.count(tbl)
            tables_found[tbl] = f"OK ({cnt} rows)"
        except Exception:
            tables_found[tbl] = "MISSING"

    rows = "".join(
        f"<tr><td>{t}</td><td style='color:{'green' if 'OK' in v else 'red'}'>{v}</td></tr>"
        for t, v in tables_found.items()
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>Shared Pool Setup</title>
<style>
body{{font:14px/1.6 sans-serif;max-width:800px;margin:40px auto;padding:20px;color:#1a1a2e}}
table{{border-collapse:collapse;width:100%;margin:16px 0}}
td,th{{padding:8px 12px;border:1px solid #e0e0e0;text-align:left}}
pre{{background:#1a1a2e;color:#a5d6ff;padding:16px;border-radius:8px;overflow-x:auto;font-size:12px}}
h2{{border-top:1px solid #e0e0e0;padding-top:20px;margin-top:30px}}
.green{{color:#1d9e75}} .red{{color:#d85a30}}
</style></head>
<body>
<h1>Shared Pool v2 — Setup</h1>
<p>Supabase: <code>{SUPABASE_URL}</code></p>
<table>{rows}</table>
<p>If <b>email_pool</b> or <b>reply_pool</b> is MISSING, copy the SQL below to your Supabase SQL Editor:</p>
<h2>email_pool</h2>
<pre>{EMAIL_POOL_SQL}</pre>
<h2>reply_pool</h2>
<pre>{SETUP_SQL}</pre>
<p><a href="/">← Back to Dashboard</a></p>
</body></html>"""


# ════════════════════════════════════════════════════════════
# Dashboard (4-tab UI)
# ════════════════════════════════════════════════════════════

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Shared Pool v2 — Supplier Intelligence</title>
<style>
:root{--bg:#f8f9fa;--card:#fff;--border:#e0e0e0;--text:#1a1a2e;--muted:#6b7280;--blue:#378add;--green:#1d9e75;--amber:#ba7517;--coral:#d85a30;--purple:#7f77dd;--teal:#0f6e56;}
*{margin:0;padding:0;box-sizing:border-box}
body{font:13px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:var(--bg);color:var(--text);padding:20px 28px;max-width:1200px;margin:0 auto}
h1{font-size:18px;font-weight:500;margin-bottom:2px}
.sub{font-size:12px;color:var(--muted);margin-bottom:20px}
.tabs{display:flex;gap:4px;margin-bottom:20px;border-bottom:1px solid var(--border)}
.tab{padding:8px 18px;font-size:13px;font-weight:500;color:var(--muted);cursor:pointer;border-bottom:2px solid transparent;transition:all .2s}
.tab:hover{color:var(--text)}
.tab.active{color:var(--blue);border-bottom-color:var(--blue)}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px;margin-bottom:20px}
.card{background:var(--card);border:0.5px solid var(--border);border-radius:10px;padding:14px}
.card .label{font-size:11px;color:var(--muted);margin-bottom:3px}
.card .value{font-size:22px;font-weight:500}
.card .value.blue{color:var(--blue)}.card .value.green{color:var(--green)}.card .value.amber{color:var(--amber)}.card .value.coral{color:var(--coral)}.card .value.purple{color:var(--purple)}.card .value.teal{color:var(--teal)}
table{width:100%;border-collapse:collapse;background:var(--card);border:0.5px solid var(--border);border-radius:10px;overflow:hidden}
th,td{text-align:left;padding:8px 12px;font-size:12px}
th{font-weight:500;background:#f8f9fa;border-bottom:0.5px solid var(--border);color:var(--muted)}
td{border-bottom:0.5px solid var(--border)}
tr:last-child td{border-bottom:none}
.btn{display:inline-block;padding:6px 14px;font-size:12px;font-weight:500;border-radius:6px;border:none;cursor:pointer;color:#fff;background:var(--blue);transition:opacity .2s;margin-right:4px}
.btn:hover{opacity:.85}
.btn.green{background:var(--green)}.btn.amber{background:var(--amber)}.btn.coral{background:var(--coral)}.btn.purple{background:var(--purple)}.btn.teal{background:var(--teal)}
.actions{display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap}
.status-New{background:#e6f1fb;color:#185fa5;padding:2px 7px;border-radius:4px;font-size:11px}
.status-Claimed{background:#faeeda;color:#854f0b;padding:2px 7px;border-radius:4px;font-size:11px}
.status-Contacted{background:#e1f5ee;color:#0f6e56;padding:2px 7px;border-radius:4px;font-size:11px}
.status-Replied{background:#eaf3de;color:#3b6d11;padding:2px 7px;border-radius:4px;font-size:11px}
.cat-A{background:#eaf3de;color:#3b6d11;font-weight:500;padding:2px 7px;border-radius:4px;font-size:11px}
.cat-B{background:#faeeda;color:#854f0b;font-weight:500;padding:2px 7px;border-radius:4px;font-size:11px}
.cat-C{background:#f1efe8;color:#5f5e5a;font-weight:500;padding:2px 7px;border-radius:4px;font-size:11px}
.refresh{font-size:11px;color:var(--muted);text-align:right;margin-bottom:12px}
.page{display:none}.page.active{display:block}
.btn.active{opacity:.6;box-shadow:inset 0 2px 4px rgba(0,0,0,.2)}
.log-filter-group{display:flex;gap:6px;margin-bottom:8px;flex-wrap:wrap;align-items:center}
#log-pagination .btn{padding:4px 10px;font-size:11px}
.form-row{display:flex;gap:8px;align-items:center;margin-bottom:12px;flex-wrap:wrap}
.form-row input,.form-row select{padding:5px 10px;font-size:12px;border:0.5px solid var(--border);border-radius:6px}
.form-row label{font-size:12px;color:var(--muted);min-width:60px}
</style>
</head>
<body>
<h1>Shared Pool v2 — Supplier Intelligence</h1>
<p class="sub">Supabase Edition · Domain / Email / Reply / Price · <a href="/setup" style="color:var(--blue)">Setup</a></p>
<p class="refresh" id="refresh-msg">Loading...</p>

<div class="tabs">
  <div class="tab active" onclick="switchTab('domain')">Domain Pool</div>
  <div class="tab" onclick="switchTab('email')">Email Pool</div>
  <div class="tab" onclick="switchTab('reply')">Reply Pool</div>
  <div class="tab" onclick="switchTab('quote')">Quote Pool</div>
  <div class="tab" onclick="switchTab('log')">Operation Log</div>
</div>

<!-- Domain Pool -->
<div class="page active" id="page-domain">
  <div class="cards" id="domain-cards"></div>
  <div class="actions">
    <button class="btn" onclick="exportDomains()">Export NEW domains</button>
    <button class="btn green" onclick="distributeDomains()">Distribute to team</button>
    <button class="btn amber" onclick="toggleImport()">Import domains</button>
  </div>
  <div class="form-row">
    <label>My Name</label><input id="d-user" placeholder="your name" style="width:100px" onchange="saveUserName()">
    <label>Count</label><input id="d-count" value="5000" type="number" style="width:80px">
    <label>Status</label><select id="d-status" onchange="loadDomainTable()"><option value="">All</option><option value="New">New</option><option value="Claimed">Claimed</option><option value="Contacted">Contacted</option><option value="Replied">Replied</option></select>
  </div>
  <!-- Import panel (hidden by default) -->
  <div id="import-panel" style="display:none;margin-bottom:16px;padding:14px;background:var(--card);border:0.5px solid var(--border);border-radius:10px">
    <div style="font-size:13px;font-weight:500;margin-bottom:8px">Import Domains</div>
    <textarea id="import-text" rows="5" placeholder="Paste domains, one per line&#10;example.com&#10;site.org&#10;..." style="width:100%;padding:8px;font-size:12px;border:0.5px solid var(--border);border-radius:6px;resize:vertical"></textarea>
    <div style="margin-top:8px;display:flex;align-items:center;gap:8px;flex-wrap:wrap">
      <select id="import-status" style="padding:5px 10px;font-size:12px;border:0.5px solid var(--border);border-radius:6px">
        <option value="New">Status: New</option>
      </select>
      <button class="btn green" onclick="importDomains()">Submit Import</button>
      <span style="color:var(--muted);font-size:12px">or</span>
      <label style="cursor:pointer">
        <input type="file" id="import-csv-file" accept=".csv" style="display:none" onchange="handleCsvUpload(this)">
        <span class="btn" style="display:inline-block">Upload CSV</span>
      </label>
      <button class="btn" style="font-size:11px" onclick="downloadCsvTemplate()">Download Template</button>
      <span id="import-result" style="font-size:12px;color:var(--muted)"></span>
    </div>
  </div>
  <div id="team-panel" style="display:none;margin-bottom:16px;padding:14px;background:var(--card);border:0.5px solid var(--border);border-radius:10px">
    <div style="font-size:13px;font-weight:500;margin-bottom:8px">Team Members</div>
    <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
      <input id="team-input" style="width:300px;padding:5px 10px;font-size:12px;border:0.5px solid var(--border);border-radius:6px" placeholder="leo,emma,jack">
      <button class="btn purple" onclick="saveTeam()">Save Team</button>
      <span id="team-result" style="font-size:12px;color:var(--muted)"></span>
    </div>
    <div style="font-size:11px;color:var(--muted);margin-top:4px">Comma-separated usernames. Distribute assigns domains round-robin to these members.</div>
  </div>
  <table id="domain-table"><tr><th>Domain</th><th>Source</th><th>Status</th><th>Claimed By</th><th>Priority</th><th>Created</th></tr></table>
</div>

<!-- Email Pool -->
<div class="page" id="page-email">
  <div class="cards" id="email-cards"></div>
  <div class="actions">
    <button class="btn" onclick="exportEmails()">Export send queue</button>
    <label style="font-size:12px;color:var(--muted)">My Name</label><input id="e-user" placeholder="your name" style="width:100px" onchange="saveUserName()">
    <label style="font-size:12px;color:var(--muted)">Count</label><input id="e-count" value="1000" type="number" style="width:80px">
    <button class="btn green" onclick="toggleEmailImport()">Import emails</button>
  </div>
  <!-- Email Import panel (hidden by default) -->
  <div id="email-import-panel" style="display:none;margin-bottom:16px;padding:14px;background:var(--card);border:0.5px solid var(--border);border-radius:10px">
    <div style="font-size:13px;font-weight:500;margin-bottom:8px">Import Emails (independent email pool, batch dedup, large batches OK)</div>
    <input id="email-import-user" placeholder="Your name" style="width:120px;padding:4px 8px;margin-right:8px" value="">
    <textarea id="email-import-text" rows="5" placeholder="Paste email-domain pairs, one per line&#10;email@example.com,example.com&#10;contact@site.org,site.org&#10;..." style="width:100%;padding:8px;font-size:12px;border:0.5px solid var(--border);border-radius:6px;resize:vertical;margin-top:8px"></textarea>
    <div style="margin-top:8px;display:flex;align-items:center;gap:8px;flex-wrap:wrap">
      <button class="btn green" onclick="importEmails()">Submit Import</button>
      <span style="color:var(--muted);font-size:12px">or</span>
      <label style="cursor:pointer">
        <input type="file" id="email-import-csv-file" accept=".csv" style="display:none" onchange="handleEmailCsvUpload(this)">
        <span class="btn" style="display:inline-block">Upload CSV</span>
      </label>
      <button class="btn" style="font-size:11px" onclick="downloadEmailCsvTemplate()">Download Template</button>
      <span id="email-import-result" style="font-size:12px;color:var(--muted)"></span>
    </div>
  </div>
  <table id="email-table"><tr><th>Email</th><th>Domain</th><th>Send Status</th><th>Claimed By</th><th>Source</th><th>Created</th></tr></table>
</div>

<!-- Reply Pool -->
<div class="page" id="page-reply">
  <div class="cards" id="reply-cards"></div>
  <div class="actions">
    <button class="btn" onclick="loadReplyTable('')">All</button>
    <button class="btn green" onclick="loadReplyTable('A')">A class</button>
    <button class="btn amber" onclick="loadReplyTable('B')">B class</button>
    <button class="btn purple" onclick="loadReplyTable('C')">C class</button>
    <label style="font-size:12px;color:var(--muted)">My Name</label><input id="r-user" placeholder="your name" style="width:100px" onchange="saveUserName()">
    <button class="btn green" onclick="toggleReplyImport()">Import replies</button>
  </div>
  <!-- Reply Import panel (hidden by default) -->
  <div id="reply-import-panel" style="display:none;margin-bottom:16px;padding:14px;background:var(--card);border:0.5px solid var(--border);border-radius:10px">
    <div style="font-size:13px;font-weight:500;margin-bottom:8px">Import Replies (Excel/CSV, template: 序号,邮箱,域名,账号,发件人,主题,正文摘要,日期)</div>
    <label style="cursor:pointer">
      <input type="file" id="reply-import-file" accept=".xlsx,.csv" style="display:none" onchange="handleReplyUpload(this)">
      <span class="btn" style="display:inline-block">Upload File</span>
    </label>
    <button class="btn" style="font-size:11px" onclick="downloadReplyTemplate()">Download Template</button>
    <span id="reply-import-result" style="font-size:12px;color:var(--muted)"></span>
  </div>
  <table id="reply-table"><tr><th>Email</th><th>Domain</th><th>Category</th><th>Status</th><th>Supplier</th><th>Reply Time</th><th>Content</th></tr></table>
</div>

<!-- Quote Pool -->
<div class="page" id="page-quote">
  <div class="cards" id="quote-cards"></div>
  <div class="actions">
    <a class="btn" href="/api/quote/export" target="_blank">Export CSV</a>
    <label style="font-size:12px;color:var(--muted)">My Name</label><input id="q-user" placeholder="your name" style="width:100px" onchange="saveUserName()">
    <button class="btn green" onclick="importARepliesToQuotes()">Import A-class replies</button>
    <span id="quote-import-result" style="font-size:12px;color:var(--muted)"></span>
  </div>
  <div style="overflow-x:auto">
  <table id="quote-table"><tr>
    <th>Domain</th><th>Supplier</th><th>Country</th><th>Type</th><th>Price</th>
    <th>Link Rules</th><th>Permanence</th><th>TAT</th><th>Status</th>
  </tr></table>
  </div>
</div>

<!-- Operation Log -->
<div class="page" id="page-log">
  <div class="cards" id="log-cards"></div>

  <!-- 一级分类: Pool -->
  <div class="log-filter-group" id="log-pool-filters">
    <button class="btn active" id="log-pool-all" onclick="setLogPoolFilter('')">All</button>
    <button class="btn green" id="log-pool-domain" onclick="setLogPoolFilter('domain')">Domain Pool</button>
    <button class="btn amber" id="log-pool-email" onclick="setLogPoolFilter('email')">Email Pool</button>
    <button class="btn teal" id="log-pool-reply" onclick="setLogPoolFilter('reply')">Reply Pool</button>
    <button class="btn purple" id="log-pool-quote" onclick="setLogPoolFilter('quote')">Quote Pool</button>
  </div>

  <!-- 二级分类: Action (动态显示) -->
  <div class="log-filter-group" id="log-action-filters" style="display:none">
    <span style="font-size:12px;color:var(--muted)">Action:</span>
    <button class="btn active" id="log-action-all" onclick="setLogActionFilter('')">All</button>
    <button class="btn" id="log-action-import" onclick="setLogActionFilter('import')">Import</button>
    <button class="btn" id="log-action-export" onclick="setLogActionFilter('export')">Export</button>
    <button class="btn" id="log-action-distribute" onclick="setLogActionFilter('distribute')">Distribute</button>
  </div>

  <!-- 用户筛选 -->
  <div class="log-filter-group">
    <label style="font-size:12px;color:var(--muted)">User:</label>
    <select id="log-user-filter" onchange="renderLogPage()" style="padding:4px 10px;font-size:12px;border:0.5px solid var(--border);border-radius:6px;background:var(--bg);color:var(--text)">
      <option value="">All Users</option>
    </select>
    <button class="btn" onclick="refreshLogs()" style="margin-left:auto">Refresh</button>
  </div>

  <table id="log-table"><tr><th>Time</th><th>User</th><th>Action</th><th>Table</th><th>Count</th><th>Detail</th></tr></table>

  <!-- 分页控件 -->
  <div class="log-filter-group" id="log-pagination" style="justify-content:center;margin-top:8px">
    <button class="btn" onclick="changeLogPage(-1)" id="log-prev">Prev</button>
    <span id="log-page-info" style="font-size:12px;color:var(--muted);margin:0 12px">Page 1 / 1 (0 records)</span>
    <button class="btn" onclick="changeLogPage(1)" id="log-next">Next</button>
    <label style="font-size:12px;color:var(--muted);margin-left:16px">Per page:</label>
    <select id="log-page-size" onchange="onLogPageSizeChange()" style="padding:4px 8px;font-size:12px;border:0.5px solid var(--border);border-radius:6px;background:var(--bg);color:var(--text)">
      <option value="10">10</option>
      <option value="20" selected>20</option>
      <option value="50">50</option>
      <option value="100">100</option>
    </select>
  </div>

  <!-- 当前页统计 -->
  <div id="log-page-stats" style="font-size:12px;color:var(--muted);text-align:center;margin-top:4px;padding-bottom:12px"></div>
</div>

<script>
const API=location.origin;
function fmt(n){return n!=null?Number(n).toLocaleString():'0'}
function esc(s){return String(s||'').replace(/</g,'&lt;').slice(0,80)}

// ── User name via localStorage ──
function getUserName(){
  const el=document.getElementById('d-user');
  let name=el.value.trim();
  if(!name) name=localStorage.getItem('shared_pool_user')||'';
  if(name){
    el.value=name;
    const eu=document.getElementById('e-user'); if(eu) eu.value=name;
    const ru=document.getElementById('r-user'); if(ru) ru.value=name;
    const qu=document.getElementById('q-user'); if(qu) qu.value=name;
  }
  return name;
}
function saveUserName(){
  const name=document.getElementById('d-user').value.trim();
  if(name){
    localStorage.setItem('shared_pool_user',name);
    const eu=document.getElementById('e-user'); if(eu) eu.value=name;
    const ru=document.getElementById('r-user'); if(ru) ru.value=name;
    const qu=document.getElementById('q-user'); if(qu) qu.value=name;
  }
}
// Load saved user name on page load
(function(){
  const saved=localStorage.getItem('shared_pool_user');
  if(saved){
    const du=document.getElementById('d-user'); if(du) du.value=saved;
    const eu=document.getElementById('e-user'); if(eu) eu.value=saved;
    const ru=document.getElementById('r-user'); if(ru) ru.value=saved;
    const qu=document.getElementById('q-user'); if(qu) qu.value=saved;
  }
})();

// ── Import panel ──
function toggleImport(){
  const p=document.getElementById('import-panel');
  p.style.display=p.style.display==='none'?'block':'none';
  loadTeam();  // also show team panel together
  document.getElementById('team-panel').style.display=p.style.display;
}
async function importDomains(){
  const user=getUserName();
  if(!user){alert('Please enter your name first');return;}
  const raw=document.getElementById('import-text').value;
  const status=document.getElementById('import-status').value;
  const domains=raw.split(/[\n,;]+/).map(d=>d.trim()).filter(d=>d&&d.includes('.'));
  if(!domains.length){alert('No valid domains found');return;}
  document.getElementById('import-result').textContent='Importing '+domains.length+' domains...';
  try{
    const r=await fetch(API+'/api/domain/register',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({domains,collection_status:status,imported_by:user})
    }).then(r=>r.json());
    document.getElementById('import-result').textContent='Done: '+r.new+' new, '+r.duplicates+' duplicates';
    document.getElementById('import-text').value='';
    loadStats();loadDomainTable();
  }catch(e){
    document.getElementById('import-result').textContent='Error: '+e.message;
  }
}

// ── CSV Upload & Template ──
function handleCsvUpload(input){
  const file=input.files[0];
  if(!file)return;
  const reader=new FileReader();
  reader.onload=function(e){
    const text=e.target.result;
    const lines=text.split(/\r?\n/);
    const domains=[];
    for(const line of lines){
      const cells=line.split(',');
      for(const cell of cells){
        const d=cell.trim().toLowerCase();
        if(d && d.includes('.')) domains.push(d);
      }
    }
    if(!domains.length){alert('No valid domains found in CSV');input.value='';return;}
    document.getElementById('import-text').value=domains.join('\n');
    document.getElementById('import-result').textContent='CSV loaded: '+domains.length+' domains ready. Click Submit Import.';
    input.value='';
  };
  reader.readAsText(file);
}
function downloadCsvTemplate(){
  const csv='domain\nexample.com\nsite.org\ncompany.net\n';
  const blob=new Blob([csv],{type:'text/csv'});
  const url=URL.createObjectURL(blob);
  const a=document.createElement('a');
  a.href=url;
  a.download='domain_import_template.csv';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// ── Team management ──
async function loadTeam(){
  try{
    const r=await fetch(API+'/api/config/team').then(r=>r.json());
    document.getElementById('team-input').value=(r.members||[]).join(',');
  }catch(e){}
}
async function saveTeam(){
  const raw=document.getElementById('team-input').value;
  const members=raw.split(',').map(m=>m.trim()).filter(m=>m);
  if(!members.length){alert('Enter at least one team member');return;}
  try{
    await fetch(API+'/api/config/team',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({members})
    });
    document.getElementById('team-result').textContent='Saved: '+members.join(', ');
  }catch(e){
    document.getElementById('team-result').textContent='Error: '+e.message;
  }
}

function switchTab(name){
  const tabs=['domain','email','reply','quote','log'];
  document.querySelectorAll('.tab').forEach((t,i)=>t.classList.toggle('active',tabs[i]===name));
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  document.getElementById('page-'+name).classList.add('active');
  if(name==='email')loadEmailTable();
  if(name==='reply')loadReplyTable('');
  if(name==='quote')loadQuoteTable();
  if(name==='log')loadLogTable('');
}

async function loadStats(){
  try{
    const s=await fetch(API+'/api/stats').then(r=>r.json());
    document.getElementById('domain-cards').innerHTML=[
      {l:'Unique Domains',v:s.domain_unique_total,c:'blue'},{l:'New',v:s.domain_unique_new,c:'amber'},
      {l:'Claimed',v:s.domain_unique_claimed,c:'purple'},{l:'Contacted',v:s.domain_unique_contacted,c:'teal'},
      {l:'Replied',v:s.domain_unique_replied,c:'green'},
    ].map(c=>`<div class="card"><div class="label">${c.l}</div><div class="value ${c.c}">${fmt(c.v)}</div></div>`).join('');
    document.getElementById('email-cards').innerHTML=[
      {l:'Total Emails',v:s.email_total,c:'blue'},{l:'Unsent',v:s.email_unsent,c:'amber'},
      {l:'Sent',v:s.email_sent,c:'green'},{l:'Bounce',v:s.email_bounce,c:'red'},
    ].map(c=>`<div class="card"><div class="label">${c.l}</div><div class="value ${c.c}">${fmt(c.v)}</div></div>`).join('');
    document.getElementById('reply-cards').innerHTML=[
      {l:'Total',v:s.reply_total,c:'blue'},{l:'Unread',v:s.reply_unread,c:'amber'},
      {l:'A class',v:s.reply_a,c:'green'},{l:'B class',v:s.reply_b,c:'amber'},
      {l:'C class',v:s.reply_c,c:'purple'},
    ].map(c=>`<div class="card"><div class="label">${c.l}</div><div class="value ${c.c}">${fmt(c.v)}</div></div>`).join('');
    document.getElementById('quote-cards').innerHTML=[
      {l:'Total quotes',v:s.quote_total,c:'blue'},{l:'Today new',v:s.quote_today_new,c:'amber'},
    ].map(c=>`<div class="card"><div class="label">${c.l}</div><div class="value ${c.c}">${fmt(c.v)}</div></div>`).join('');
    document.getElementById('refresh-msg').textContent='Updated: '+new Date().toLocaleTimeString('zh-CN');
  }catch(e){document.getElementById('refresh-msg').textContent='Error: '+e.message}
}

async function loadDomainTable(){
  const status=document.getElementById('d-status').value;
  const r=await fetch(API+'/api/domain/list?limit=100'+(status?'&status='+status:'')).then(r=>r.json());
  document.getElementById('domain-table').innerHTML='<tr><th>Domain</th><th>Status</th><th>Claimed By</th><th>Priority</th><th>Created</th></tr>'+
    (r.domains||[]).map(d=>`<tr>
      <td>${esc(d.domain)}</td>
      <td><span class="status-${d.collection_status||'New'}">${d.collection_status||'New'}</span></td>
      <td>${esc(d.claimed_by)}</td>
      <td>${d.priority||0}</td>
      <td>${(d.created_at||'').slice(0,16)}</td>
    </tr>`).join('');
}

async function loadEmailTable(){
  const u=getUserName()||'';
  const r=await fetch(API+'/api/email/queue?user='+encodeURIComponent(u)+'&count=50').then(r=>r.json());
  document.getElementById('email-table').innerHTML='<tr><th>Email</th><th>Domain</th><th>Send Status</th><th>Claimed By</th><th>Created</th></tr>'+
    (r.emails||[]).map(e=>`<tr>
      <td>${esc(e.contact_email)}</td>
      <td>${esc(e.domain)}</td>
      <td><span class="status-${e.send_status||'UNSENT'}">${e.send_status||'UNSENT'}</span></td>
      <td>${esc(e.claimed_by)}</td>
      <td>${(e.created_at||'').slice(0,16)}</td>
    </tr>`).join('');
}

function toggleEmailImport(){
  const p=document.getElementById('email-import-panel');
  p.style.display=p.style.display==='none'?'block':'none';
}
async function importEmails(){
  const user=getUserName();
  if(!user){alert('Please enter your name first');return;}
  const raw=document.getElementById('email-import-text').value;
  const lines=raw.split(/[\n]+/).map(l=>l.trim()).filter(l=>l);
  const records=[];
  for(const line of lines){
    const parts=line.split(/[,;\t]+/);
    if(parts.length>=2){
      const email=parts[0].trim().toLowerCase();
      const domain=parts[1].trim().toLowerCase().replace(/^www\./,'');
      if(email.includes('@')&&domain.includes('.')){
        records.push({email,domain});
      }
    }
  }
  if(!records.length){alert('No valid email-domain pairs found. Format: email,domain');return;}
  document.getElementById('email-import-result').textContent='Importing '+records.length+' emails...';
  try{
    const r=await fetch(API+'/api/email/import',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({emails:records,imported_by:user})
    }).then(r=>r.json());
    document.getElementById('email-import-result').textContent='Done: '+r.imported+' imported, '+r.skipped+' skipped (duplicates)';
    document.getElementById('email-import-text').value='';
    loadStats();loadEmailTable();
  }catch(e){
    document.getElementById('email-import-result').textContent='Error: '+e.message;
  }
}
function handleEmailCsvUpload(input){
  const file=input.files[0];
  if(!file)return;
  const reader=new FileReader();
  reader.onload=function(e){
    const text=e.target.result;
    const lines=text.split(/\r?\n/);
    const pairs=[];
    for(const line of lines){
      const cells=line.split(/,|;|\t/);
      if(cells.length>=2){
        const email=cells[0].trim().toLowerCase();
        const domain=cells[1].trim().toLowerCase().replace(/^www\./,'');
        if(email.includes('@')&&domain.includes('.')){
          pairs.push(email+','+domain);
        }
      }
    }
    if(!pairs.length){alert('No valid email-domain pairs found in CSV');input.value='';return;}
    document.getElementById('email-import-text').value=pairs.join('\n');
    document.getElementById('email-import-result').textContent='CSV loaded: '+pairs.length+' email-domain pairs ready. Click Submit Import.';
    input.value='';
  };
  reader.readAsText(file);
}
function downloadEmailCsvTemplate(){
  const csv='email,domain\na@example.com,example.com\nb@site.org,site.org\ncontact@company.net,company.net\n';
  const blob=new Blob([csv],{type:'text/csv'});
  const url=URL.createObjectURL(blob);
  const a=document.createElement('a');
  a.href=url;
  a.download='email_import_template.csv';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// ── Operation Log (new: two-level filter + user select + pagination + per-page stats) ──
const LOG = { allLogs: [], poolFilter: '', actionFilter: '', page: 1, pageSize: 20 };

function _logActionShort(type) {
  if (!type) return '-';
  const map = { domain_import: 'Import', domain_export: 'Export', domain_distribute: 'Distribute',
                email_import: 'Import', email_export: 'Export',
                price_import: 'Import', price_export: 'Export',
                reply_import: 'Import', reply_export: 'Export' };
  return map[type] || type;
}

function _logPoolShort(type) {
  if (!type) return '-';
  const p = (type.split('_')[0] || '');
  return p ? p.charAt(0).toUpperCase() + p.slice(1) + ' Pool' : '-';
}

async function refreshLogs() {
  const url = API + '/api/log/list?limit=2000';
  const r = await fetch(url).then(r => r.json());
  LOG.allLogs = r.logs || [];
  LOG.allLogs.sort((a, b) => {
    const ta = (a.time || a.timestamp || '').replace('T', ' ');
    const tb = (b.time || b.timestamp || '').replace('T', ' ');
    return tb.localeCompare(ta);
  });

  // Render pool-level cards from ALL logs
  const domainCount = LOG.allLogs.filter(l => l.type && l.type.startsWith('domain_')).length;
  const emailCount = LOG.allLogs.filter(l => l.type && l.type.startsWith('email_')).length;
  const replyCount = LOG.allLogs.filter(l => l.type && l.type.startsWith('reply_')).length;
  const quoteCount = LOG.allLogs.filter(l => l.type && (l.type.startsWith('quote_') || l.type.startsWith('price_'))).length;
  document.getElementById('log-cards').innerHTML = [
    { l: 'Domain Pool', v: domainCount, c: 'blue' },
    { l: 'Email Pool', v: emailCount, c: 'amber' },
    { l: 'Reply Pool', v: replyCount, c: 'green' },
    { l: 'Quote Pool', v: quoteCount, c: 'purple' },
  ].map(c => `<div class="card"><div class="label">${c.l}</div><div class="value ${c.c}">${fmt(c.v)}</div></div>`).join('');

  renderLogPage();
}

function setLogPoolFilter(pool) {
  LOG.poolFilter = pool;
  LOG.actionFilter = '';
  LOG.page = 1;

  // Update button active states
  document.querySelectorAll('#log-pool-filters .btn').forEach(b => b.classList.remove('active'));
  document.getElementById('log-pool-' + (pool || 'all')).classList.add('active');

  // Show/hide action filter row
  const actionRow = document.getElementById('log-action-filters');
  if (pool && pool !== 'reply') {
    actionRow.style.display = 'flex';
    document.querySelectorAll('#log-action-filters .btn').forEach(b => b.classList.remove('active'));
    document.getElementById('log-action-all').classList.add('active');
  } else {
    actionRow.style.display = 'none';
    LOG.actionFilter = '';
  }

  renderLogPage();
}

function setLogActionFilter(action) {
  LOG.actionFilter = action;
  LOG.page = 1;
  document.querySelectorAll('#log-action-filters .btn').forEach(b => b.classList.remove('active'));
  document.getElementById('log-action-' + (action || 'all')).classList.add('active');
  renderLogPage();
}

function changeLogPage(delta) {
  LOG.page += delta;
  if (LOG.page < 1) LOG.page = 1;
  renderLogPage();
}

function onLogPageSizeChange() {
  LOG.pageSize = parseInt(document.getElementById('log-page-size').value) || 20;
  LOG.page = 1;
  renderLogPage();
}

function renderLogPage() {
  let logs = [...LOG.allLogs];

  // Apply pool filter (first-level)
  if (LOG.poolFilter) {
    if (LOG.poolFilter === 'quote') {
      logs = logs.filter(l => l.type && (l.type.startsWith('quote_') || l.type.startsWith('price_')));
    } else {
      logs = logs.filter(l => l.type && l.type.startsWith(LOG.poolFilter + '_'));
    }
  }

  // Apply action filter (second-level: import/export/distribute)
  if (LOG.actionFilter) {
    logs = logs.filter(l => {
      const parts = (l.type || '').split('_');
      return parts[1] === LOG.actionFilter;
    });
  }

  // Build user dropdown from filtered logs, exclude test/system entries
  const TEST_RE = /test|system|demo|unknown|admin@|autolink/i;
  const filteredUsers = [...new Set(logs.map(l => l.user).filter(u => u && !TEST_RE.test(u)))].sort();
  const sel = document.getElementById('log-user-filter');
  const oldVal = sel.value;
  sel.innerHTML = '<option value="">All Users</option>' +
    filteredUsers.map(u => `<option value="${esc(u)}">${esc(u)}</option>`).join('');
  if (filteredUsers.includes(oldVal)) sel.value = oldVal;

  // Apply user filter
  const userFilter = sel.value;
  if (userFilter) {
    logs = logs.filter(l => l.user === userFilter);
  }

  const total = logs.length;
  const totalPages = Math.max(1, Math.ceil(total / LOG.pageSize));
  if (LOG.page > totalPages) LOG.page = totalPages;
  if (LOG.page < 1) LOG.page = 1;

  const start = (LOG.page - 1) * LOG.pageSize;
  const pageLogs = logs.slice(start, start + LOG.pageSize);

  // Render table
  const table = document.getElementById('log-table');
  table.innerHTML = '<tr><th>Time</th><th>User</th><th>Action</th><th>Pool</th><th>Count</th><th>Detail</th></tr>' +
    pageLogs.map(l => `<tr>
      <td>${esc((l.time || '').slice(0, 16))}</td>
      <td>${esc(l.user)}</td>
      <td><span class="status-${l.type || 'New'}">${_logActionShort(l.type)}</span></td>
      <td>${_logPoolShort(l.type)}</td>
      <td>${l.count || 0}</td>
      <td style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(l.detail)}</td>
    </tr>`).join('');

  // Update pagination info
  document.getElementById('log-page-info').textContent =
    `Page ${LOG.page} / ${totalPages} (${total} records)`;
  document.getElementById('log-prev').disabled = LOG.page <= 1;
  document.getElementById('log-next').disabled = LOG.page >= totalPages;

  // Per-page statistics
  const pageTotalCount = pageLogs.reduce((s, l) => s + (l.count || 0), 0);
  const typeStats = {};
  pageLogs.forEach(l => {
    const t = l.type || 'unknown';
    typeStats[t] = (typeStats[t] || 0) + (l.count || 0);
  });
  const statParts = Object.entries(typeStats)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([t, c]) => `${_logActionShort(t)}: ${fmt(c)}`);
  const statsText = `This page: ${pageLogs.length} logs, total count ${fmt(pageTotalCount)}` +
    (statParts.length ? ' | ' + statParts.join(' / ') : '');
  document.getElementById('log-page-stats').textContent = statsText;
}

// Back-compat wrapper
async function loadLogTable(filterType) {
  if (!LOG.allLogs.length) await refreshLogs();
  if (!filterType) {
    setLogPoolFilter('');
  } else if (['domain', 'email', 'reply', 'price'].includes(filterType)) {
    setLogPoolFilter(filterType);
  } else {
    // Exact type like domain_import
    const pool = filterType.split('_')[0];
    const action = filterType.split('_')[1];
    setLogPoolFilter(pool);
    if (action) setLogActionFilter(action);
  }
}

async function loadReplyTable(cat){
  const r=await fetch(API+'/api/reply/list?limit=100'+(cat?'&category='+cat:'')).then(r=>r.json());
  document.getElementById('reply-table').innerHTML='<tr><th>Email</th><th>Domain</th><th>Category</th><th>Status</th><th>Supplier</th><th>Reply Time</th><th>Content</th></tr>'+
    (r.replies||[]).map(rp=>`<tr>
      <td>${esc(rp.email)}</td>
      <td>${esc(rp.domain)}</td>
      <td><span class="cat-${rp.category}">${rp.category||'C'}</span></td>
      <td>${esc(rp.status)}</td>
      <td>${esc(rp.supplier)}</td>
      <td>${(rp.reply_time||'').slice(0,16)}</td>
      <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(rp.reply_content)}</td>
    </tr>`).join('');
}

async function loadQuoteTable(){
  const r=await fetch(API+'/api/quote/list?limit=200').then(r=>r.json());
  document.getElementById('quote-table').innerHTML='<tr><th>Domain</th><th>Supplier</th><th>Country</th><th>Type</th><th>Price</th><th>Link Rules</th><th>Permanence</th><th>TAT</th><th>Status</th></tr>'+
    (r.quotes||[]).map(q=>`<tr>
      <td><a href="http://${esc(q.domain)}" target="_blank">${esc(q.domain)}</a></td>
      <td>${esc(q.supplier)}</td>
      <td>${esc(q.country)}</td>
      <td>${esc(q.cooperation_type)}</td>
      <td>${esc(q.price)}</td>
      <td>${esc(q.link_rules)}</td>
      <td>${esc(q.permanence)}</td>
      <td>${esc(q.tat)}</td>
      <td><span class="status-${q.status||'New'}">${q.status||'New'}</span></td>
    </tr>`).join('');
}

// ── Import A-class replies to Quote Pool ──
async function importARepliesToQuotes(){
  const user=getUserName();
  if(!user){alert('Please enter your name in "My Name" field first');return;}
  if(!confirm('Import all A-class replies (671 records) to Quote Pool?\\nExisting quotes (by email) will be skipped.'))return;
  const result=document.getElementById('quote-import-result');
  result.textContent='Importing...';
  try{
    const form=new FormData();
    form.append('user',user);
    const r=await fetch(API+'/api/quote/import-a-replies',{method:'POST',body:form});
    const data=await r.json();
    if(data.error){result.textContent='Error: '+data.error;alert(data.error);}
    else{
      result.textContent='OK: '+data.imported+' imported, '+data.skipped+' skipped (quote pool total: '+data.quote_pool_total+')';
      loadQuoteTable();loadStats();
    }
  }catch(e){result.textContent='Network error';alert('Import failed: '+e.message);}
}

// ── Reply Pool import ──
function toggleReplyImport(){
  const p=document.getElementById('reply-import-panel');
  p.style.display=p.style.display==='none'?'block':'none';
}
function downloadReplyTemplate(){
  const csv='\uFEFF序号,邮箱,域名,账号,发件人,主题,正文摘要,日期\n1,example@domain.com,domain.com,your-account,Supplier Name,Reply Subject,Reply body preview...,2026-07-30';
  const blob=new Blob([csv],{type:'text/csv;charset=utf-8'});
  const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download='reply_import_template.csv';a.click();
}
async function handleReplyUpload(input){
  const user=getUserName();
  if(!user){alert('Please enter your name in "My Name" field first');return;}
  if(!input.files||!input.files[0])return;
  const file=input.files[0];
  const result=document.getElementById('reply-import-result');
  result.textContent='Uploading...';
  const form=new FormData();
  form.append('file',file);
  form.append('user',user);
  try{
    const r=await fetch(API+'/api/reply/import',{method:'POST',body:form});
    const data=await r.json();
    if(data.error){result.textContent='Error: '+data.error;alert(data.error);}
    else{
      result.textContent='OK: imported '+data.imported+', skipped '+data.skipped+' duplicates';
      alert('Imported '+data.imported+' replies'+(data.skipped?' (skipped '+data.skipped+' duplicates)':''));
      loadReplyTable('');loadStats();
    }
  }catch(e){result.textContent='Network error';alert('Upload failed: '+e.message);}
  input.value='';
}

async function exportDomains(){
  const user=getUserName();
  if(!user){alert('Please enter your name in "My Name" field first');return;}
  const count=document.getElementById('d-count').value;
  const r=await fetch(API+'/api/domain/export',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({user,count:parseInt(count)})}).then(r=>r.json());
  if(r.error){alert(r.error);return;}
  if(r.csv_content){
    const blob=new Blob(['\uFEFF'+r.csv_content],{type:'text/csv;charset=utf-8'});
    const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download=r.filename;a.click();
  }
  alert('Exported '+r.exported+' domains\nFile: '+r.filename+'\nBatch: '+r.batch_id);
  loadStats();loadDomainTable();
}
async function distributeDomains(){
  if(!confirm('Distribute ALL New domains among team members?'))return;
  const r=await fetch(API+'/api/domain/distribute',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({})}).then(r=>r.json());
  alert('Distributed '+r.assigned+' domains\n'+JSON.stringify(r.distribution,null,2));
  loadStats();loadDomainTable();
}
async function exportEmails(){
  const user=getUserName();
  if(!user){alert('Please enter your name in "My Name" field first');return;}
  const count=document.getElementById('e-count').value;
  const r=await fetch(API+'/api/email/export',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({user,count:parseInt(count)})}).then(r=>r.json());
  if(r.error){alert(r.error);return;}
  if(r.csv_content){
    const blob=new Blob(['\uFEFF'+r.csv_content],{type:'text/csv;charset=utf-8'});
    const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download=r.filename;a.click();
  }
  alert('Exported '+r.exported+' emails\nFile: '+r.filename);
  loadStats();
}

function loadAll(){loadStats();loadDomainTable();}
loadAll();
setInterval(loadStats,15000);
</script>
</body>
</html>"""


@app.route("/")
def dashboard():
    return DASHBOARD_HTML


# ════════════════════════════════════════════════════════════
# Main
# ════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Shared Pool v2 — Supplier Intelligence (Supabase)")
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--host", type=str, default="0.0.0.0")
    parser.add_argument("--dbg", action="store_true", help="Debug mode")
    args = parser.parse_args()

    # PORT priority: env var (Render) > --port arg > default 8765
    port = int(os.environ.get("PORT", 0)) or args.port or 8765

    # Verify connection
    print(f"\n  Shared Pool v2 — Supabase Edition")
    print(f"  Project:   {SUPABASE_URL}")
    try:
        health_check = db.select("domain_pool", select="domain_id", limit=1)
        print(f"  Connection: OK (domain_pool: {db.count('domain_pool'):,} rows)")
    except Exception as e:
        print(f"  Connection: FAILED — {e}")
        print(f"  Check your config.py settings.")
        sys.exit(1)

    print(f"  Listening:  http://{args.host}:{port}")
    print(f"  Dashboard:  http://localhost:{port}")
    print(f"  Setup:      http://localhost:{port}/setup")
    print(f"  API:        /api/domain/*  /api/email/*  /api/reply/*  /api/price/*")
    print()

    app.run(host=args.host, port=port, debug=args.dbg, threaded=True)
