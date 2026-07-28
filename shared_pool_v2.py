"""
Shared Pool v2 — Supplier Intelligence (Supabase Edition)
==========================================================
A generic shared pool for domain/link supplier intelligence.
Connects to Supabase PostgreSQL as the data layer.
Anyone can use it with their own Supabase project — just edit config.py.

Four Pools:
  Domain Pool  — domain_pool table      (161K+ domains, with contact emails)
  Email Pool   — derived from domain_pool (send queue, bounce tracking)
  Reply Pool   — reply_pool table        (inbound replies, A/B/C classification)
  Price Pool   — quote_pool table        (multi-supplier quotes, negotiation)

Quick Start:
  1. Edit config.py with your SUPABASE_URL + SUPABASE_ANON_KEY
  2. pip install flask
  3. python shared_pool_v2.py --port 8765
  4. Open http://localhost:8765

Required Supabase Tables (already exist in default project):
  domain_pool, supplier_pool, quote_pool, config

To create reply_pool:
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
from collections import defaultdict
from datetime import datetime, timezone

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
REST_URL = f"{SUPABASE_URL}/rest/v1"

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
            extra["Prefer"] = "resolution=merge-duplicates"
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
    return datetime.now(timezone.utc).isoformat()

def safe_str(v, default=""):
    if v is None:
        return default
    return str(v)


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
    """Batch register domains. Auto-dedup on domain name.
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

    rows = []
    new_count, dup_count = 0, 0
    for raw in raw_domains:
        d = raw.lower().strip().lstrip("www.")
        if not d:
            continue
        rows.append({
            "domain": d,
            "source": "未提取",
            "priority": priority,
            "notes": notes,
            "collection_status": default_status,
        })

    if not rows:
        return jsonify({"new": 0, "duplicates": 0, "domains": []})

    resp, result = db.insert("domain_pool", rows, upsert=False)
    if hasattr(resp, "status") and resp.status in (200, 201):
        new_count = len(result) if isinstance(result, list) else len(rows)
    else:
        # Some might be duplicates; count successes
        new_count = len(result) if isinstance(result, list) else 0
        dup_count = len(rows) - new_count

    # 4. Log operation
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

    # 1. Fetch unclaimed domains (New status, ordered by domain_id ascending)
    domains = db.select(
        "domain_pool",
        select="domain_id,domain,source,priority,created_at",
        filters={"collection_status": "New"},
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
    db.patch_by_ids("domain_pool", {
        "claimed_by": user,
        "collection_status": "Claimed",
        "source": "已提取",
        "claim_batch_id": batch_id,
        "claim_time": now,
    }, ids)

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

    # 5. Log operation
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

    # Log operation
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
    Get UNSENT emails ready for sending.
    Only collection_status=New/Claimed, deduplicated by normalized contact_email.
    Excludes contacted (sent), replied, bounced, and blacklisted.
    """
    user = request.args.get("user", "")
    count = min(int(request.args.get("count", 100)), 2000)

    # Only unsent: New or Claimed
    filters = {"collection_status": "in.(New,Claimed)"}

    domains = db.select(
        "domain_pool",
        select="domain_id,domain,contact_email,collection_status,claimed_by,source,created_at",
        filters=filters,
        limit=count * 3,  # fetch extra to account for dedup
        order="created_at",
        ascending=True,
    )

    # Dedup by normalized email, must have contact_email
    seen = set()
    result = []
    for d in domains:
        email = (d.get("contact_email") or "").strip().lower()
        if email and email not in seen:
            seen.add(email)
            d["send_status"] = "UNSENT"
            result.append(d)
    result = result[:count]

    return jsonify({"emails": result, "count": len(result)})


@app.route("/api/email/export", methods=["POST"])
def email_export():
    """
    Export UNSENT emails for sending. Same logic as domain pool:
      - Only exports emails with source="未提取" (or collection_status=New/Claimed for legacy).
      - Deduplicates by contact_email.
      - Different users get different emails (claimed_by locking).
      - Marks source="已提取" after export.
    """
    data = request.get_json(force=True)
    user = data.get("user", "").strip()
    if not user:
        return jsonify({"error": "user is required", "exported": 0}), 400
    count = min(int(data.get("count", 500)), 5000)

    # Only unsent and unextracted: New or Claimed
    filters = {"collection_status": "in.(New,Claimed)"}

    domains = db.select(
        "domain_pool",
        select="domain_id,domain,contact_email,collection_status,source",
        filters=filters,
        limit=count * 3,  # fetch extra for dedup
        order="domain_id",
        ascending=True,
    )

    # Filter: must have contact_email, dedup by normalized email
    seen = set()
    emails = []
    for d in domains:
        email = (d.get("contact_email") or "").strip().lower()
        if email and email not in seen:
            seen.add(email)
            emails.append(d)
    emails = emails[:count]

    if not emails:
        return jsonify({"exported": 0, "filename": ""})

    batch_id = f"email_send_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{user}"
    filename = f"{batch_id}.csv"
    now = now_iso()

    # Generate CSV — same format Maisui sender expects
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["email_id", "email", "domain", "send_status", "source"])
    for e in emails:
        writer.writerow([
            e["domain_id"],
            safe_str(e.get("contact_email")),
            e["domain"],
            "UNSENT",
            safe_str(e.get("source")),
        ])

    csv_content = output.getvalue()
    export_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "shared_pool_exports")
    os.makedirs(export_dir, exist_ok=True)
    with open(os.path.join(export_dir, filename), "w", newline="", encoding="utf-8") as f:
        f.write(csv_content)

    # Lock: mark as Contacted (sent) + extracted, assign to user
    ids = [e["domain_id"] for e in emails]
    db.patch_by_ids("domain_pool", {
        "collection_status": "Contacted",
        "source": "已提取",
        "claimed_by": user,
        "claim_time": now,
        "updated_at": now,
    }, ids)

    # Log operation
    _log_operation("email_export", user, "email_pool", len(emails),
                   f"Batch: {batch_id}, Source: 已提取")

    return jsonify({"exported": len(emails), "filename": filename, "batch_id": batch_id, "csv_content": csv_content})


@app.route("/api/email/stats", methods=["GET"])
def email_stats():
    """Email pool statistics (derived from domain_pool)."""
    total = db.count("domain_pool")
    contacted = db.count("domain_pool", filters={"collection_status": "Contacted"})
    replied = db.count("domain_pool", filters={"collection_status": "Replied"})
    new = db.count("domain_pool", filters={"collection_status": "New"})
    claimed = db.count("domain_pool", filters={"collection_status": "Claimed"})

    return jsonify({
        "total": total,
        "unsent": new + claimed,
        "sent": contacted,
        "replied": replied,
        "bounce": 0,
    })


@app.route("/api/email/import", methods=["POST"])
def email_pool_import():
    """
    Batch import emails for sending.
    Body: {"emails": [{"email":"a@b.com","domain":"b.com"},...], "imported_by": "leo"}
    Deduplicates by contact_email: same email → skip (prevents overwriting existing emails).
    All new entries marked source="未提取", collection_status="New".
    """
    data = request.get_json(force=True)
    records = data.get("emails", [])
    imported_by = data.get("imported_by", "").strip()
    if not imported_by:
        return jsonify({"error": "imported_by is required"}), 400

    if not records:
        return jsonify({"imported": 0, "new": 0, "skipped": 0})

    imported = 0
    skipped = 0

    for rec in records:
        email = rec.get("email", "").strip().lower()
        domain = rec.get("domain", "").strip().lower().lstrip("www.")
        if not email or not domain:
            skipped += 1
            continue

        # Dedup by contact_email — same email already in pool → skip
        existing = db.select("domain_pool", select="domain_id",
                             filters={"contact_email": email}, limit=1)
        if existing:
            skipped += 1
            continue

        # New record
        db.insert("domain_pool", {
            "domain": domain,
            "contact_email": email,
            "source": "未提取",
            "collection_status": "New",
            "notes": f"[email imported by {imported_by}]",
            "priority": 0,
        })
        imported += 1

    _log_operation("email_import", imported_by, "email_pool", imported,
                   f"Imported: {imported}, Skipped(dup): {skipped}")

    return jsonify({"imported": imported, "new": imported, "skipped": skipped})


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
# Price Pool API (→ quote_pool table)
# ════════════════════════════════════════════════════════════

@app.route("/api/price/add", methods=["POST"])
def price_add():
    """Add a quote."""
    data = request.get_json(force=True)
    payload = {
        "domain": data.get("domain", ""),
        "supplier": data.get("supplier", ""),
        "contact_email": data.get("contact_email", ""),
        "price": data.get("price"),
        "currency": data.get("currency", "USD"),
        "link_type": data.get("link_type", ""),
        "source": data.get("source", "manual"),
        "notes": data.get("notes", ""),
        "status": data.get("status", "pending"),
    }
    resp, result = db.insert("quote_pool", payload)
    return jsonify({"result": "created"})


@app.route("/api/price/list", methods=["GET"])
def price_list():
    """List quotes."""
    domain = request.args.get("domain", "")
    limit = min(int(request.args.get("limit", 100)), 5000)

    filters = {}
    if domain:
        filters["domain"] = domain

    quotes = db.select(
        "quote_pool",
        select="*",
        filters=filters,
        limit=limit,
        order="created_at",
        ascending=False,
    )
    return jsonify({"prices": quotes or []})


@app.route("/api/price/stats", methods=["GET"])
def price_stats():
    """Price pool statistics."""
    total = db.count("quote_pool")
    suppliers = 0
    if total > 0:
        quotes = db.select("quote_pool", select="supplier", limit=5000)
        suppliers = len(set(q.get("supplier") for q in quotes if q.get("supplier")))

    return jsonify({
        "total": total,
        "suppliers": suppliers,
        "today_new": 0,
        "avg_price": 0,
    })


@app.route("/api/price/export", methods=["GET"])
def price_export():
    """Export quotes as CSV."""
    quotes = db.select("quote_pool", select="*", limit=10000,
                       order="created_at", ascending=False)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["domain", "supplier", "contact_email", "price", "currency",
                     "link_type", "source", "status", "notes", "created_at"])
    for q in (quotes or []):
        writer.writerow([
            q.get("domain", ""), q.get("supplier", ""), q.get("contact_email", ""),
            q.get("price", ""), q.get("currency", ""), q.get("link_type", ""),
            q.get("source", ""), q.get("status", ""), q.get("notes", ""),
            safe_str(q.get("created_at", ""))[:19],
        ])

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=price_pool_export.csv"}
    )


# ════════════════════════════════════════════════════════════
# Comprehensive Stats
# ════════════════════════════════════════════════════════════

@app.route("/api/stats", methods=["GET"])
def get_stats():
    """Aggregated statistics (cached 30s). Each count() already retries on SSL errors."""

    def _fetch():
        domain_total = db.count("domain_pool")
        domain_new = db.count("domain_pool", filters={"collection_status": "New"})
        domain_claimed = db.count("domain_pool", filters={"collection_status": "Claimed"})
        domain_contacted = db.count("domain_pool", filters={"collection_status": "Contacted"})
        domain_replied = db.count("domain_pool", filters={"collection_status": "Replied"})

        # Unique domain counts — single pass over all domains
        unique_counts = _count_unique_domains()  # returns {status: count}
        domain_unique_total = sum(unique_counts.values())
        domain_unique_new = unique_counts.get("New", 0)
        domain_unique_claimed = unique_counts.get("Claimed", 0)
        domain_unique_contacted = unique_counts.get("Contacted", 0)
        domain_unique_replied = unique_counts.get("Replied", 0)

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
                suppliers = db.select(
                    "supplier_pool",
                    select="notes",
                    filters={"status": "Replied"},
                    limit=1000,
                )
                reply_a = sum(1 for s in suppliers if _parse_reply_category(s.get("notes")) == "A")
                reply_b = sum(1 for s in suppliers if _parse_reply_category(s.get("notes")) == "B")
                reply_c = sum(1 for s in suppliers if _parse_reply_category(s.get("notes")) == "C")
                if reply_total > 1000 and suppliers:
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
            "email_total": domain_total,
            "email_unsent": domain_new + domain_claimed,
            "email_assigned": domain_claimed,
            "email_sent": domain_contacted,
            "email_bounce": 0,
            "reply_total": reply_total,
            "reply_unread": reply_unread,
            "reply_a": reply_a,
            "reply_b": reply_b,
            "reply_c": reply_c,
            "reply_today": 0,
            "reply_a_today": 0,
            "price_total": quote_total,
            "price_today_new": 0,
            "price_suppliers": 0,
        }

    return jsonify(_cached("stats", ttl_sec=30, fn=_fetch))


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

    return jsonify({"logs": logs, "count": len(logs)})


# ════════════════════════════════════════════════════════════

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
    for tbl in ["domain_pool", "supplier_pool", "quote_pool", "reply_pool", "config"]:
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
<p>If <b>reply_pool</b> is MISSING, copy the SQL below to your Supabase SQL Editor:</p>
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
.btn.green{background:var(--green)}.btn.amber{background:var(--amber)}.btn.coral{background:var(--coral)}.btn.purple{background:var(--purple)}
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
  <div class="tab" onclick="switchTab('price')">Price Pool</div>
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
    <label>Count</label><input id="d-count" value="200" type="number" style="width:80px">
    <label>Status</label><select id="d-status" onchange="loadDomainTable()"><option value="">All</option><option value="New">New</option><option value="Claimed">Claimed</option><option value="Contacted">Contacted</option><option value="Replied">Replied</option></select>
  </div>
  <!-- Import panel (hidden by default) -->
  <div id="import-panel" style="display:none;margin-bottom:16px;padding:14px;background:var(--card);border:0.5px solid var(--border);border-radius:10px">
    <div style="font-size:13px;font-weight:500;margin-bottom:8px">Import Domains</div>
    <textarea id="import-text" rows="5" placeholder="Paste domains, one per line&#10;example.com&#10;site.org&#10;..." style="width:100%;padding:8px;font-size:12px;border:0.5px solid var(--border);border-radius:6px;resize:vertical"></textarea>
    <div style="margin-top:8px;display:flex;align-items:center;gap:8px">
      <select id="import-status" style="padding:5px 10px;font-size:12px;border:0.5px solid var(--border);border-radius:6px">
        <option value="New">Status: New</option>
      </select>
      <button class="btn green" onclick="importDomains()">Submit Import</button>
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
    <button class="btn green" onclick="toggleEmailImport()">Import emails</button>
  </div>
  <!-- Email Import panel (hidden by default) -->
  <div id="email-import-panel" style="display:none;margin-bottom:16px;padding:14px;background:var(--card);border:0.5px solid var(--border);border-radius:10px">
    <div style="font-size:13px;font-weight:500;margin-bottom:8px">Import Emails (from Maisui collection)</div>
    <textarea id="email-import-text" rows="5" placeholder="Paste email-domain pairs, one per line&#10;email@example.com,example.com&#10;contact@site.org,site.org&#10;..." style="width:100%;padding:8px;font-size:12px;border:0.5px solid var(--border);border-radius:6px;resize:vertical"></textarea>
    <div style="margin-top:8px;display:flex;align-items:center;gap:8px">
      <button class="btn green" onclick="importEmails()">Submit Import</button>
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
  </div>
  <table id="reply-table"><tr><th>Email</th><th>Domain</th><th>Category</th><th>Status</th><th>Supplier</th><th>Reply Time</th><th>Content</th></tr></table>
</div>

<!-- Price Pool -->
<div class="page" id="page-price">
  <div class="cards" id="price-cards"></div>
  <div class="actions">
    <a class="btn" href="/api/price/export" target="_blank">Export CSV</a>
  </div>
  <table id="price-table"><tr><th>Domain</th><th>Supplier</th><th>Contact</th><th>Price</th><th>Currency</th><th>Type</th><th>Status</th><th>Created</th></tr></table>
</div>

<!-- Operation Log -->
<div class="page" id="page-log">
  <div class="cards" id="log-cards"></div>
  <div class="actions">
    <button class="btn" onclick="loadLogTable('')">All</button>
    <button class="btn green" onclick="loadLogTable('domain')">Domain Pool</button>
    <button class="btn amber" onclick="loadLogTable('email')">Email Pool</button>
    <button class="btn" onclick="loadLogTable('domain_import')">Domain Import</button>
    <button class="btn" onclick="loadLogTable('domain_export')">Domain Export</button>
    <button class="btn amber" onclick="loadLogTable('email_import')">Email Import</button>
    <button class="btn purple" onclick="loadLogTable('email_export')">Email Export</button>
    <button class="btn coral" onclick="loadLogTable('domain_distribute')">Distribute</button>
  </div>
  <table id="log-table"><tr><th>Time</th><th>User</th><th>Action</th><th>Table</th><th>Count</th><th>Detail</th></tr></table>
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
  if(name) el.value=name;
  return name;
}
function saveUserName(){
  const name=document.getElementById('d-user').value.trim();
  if(name) localStorage.setItem('shared_pool_user',name);
}
// Load saved user name on page load
(function(){
  const saved=localStorage.getItem('shared_pool_user');
  if(saved) document.getElementById('d-user').value=saved;
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
  const tabs=['domain','email','reply','price','log'];
  document.querySelectorAll('.tab').forEach((t,i)=>t.classList.toggle('active',tabs[i]===name));
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  document.getElementById('page-'+name).classList.add('active');
  if(name==='email')loadEmailTable();
  if(name==='reply')loadReplyTable('');
  if(name==='price')loadPriceTable();
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
      {l:'Total Domains',v:s.email_total,c:'blue'},{l:'Unsent',v:s.email_unsent,c:'amber'},
      {l:'Claimed',v:s.email_assigned,c:'purple'},{l:'Sent',v:s.email_sent,c:'green'},
    ].map(c=>`<div class="card"><div class="label">${c.l}</div><div class="value ${c.c}">${fmt(c.v)}</div></div>`).join('');
    document.getElementById('reply-cards').innerHTML=[
      {l:'Total',v:s.reply_total,c:'blue'},{l:'Unread',v:s.reply_unread,c:'amber'},
      {l:'A class',v:s.reply_a,c:'green'},{l:'B class',v:s.reply_b,c:'amber'},
      {l:'C class',v:s.reply_c,c:'purple'},
    ].map(c=>`<div class="card"><div class="label">${c.l}</div><div class="value ${c.c}">${fmt(c.v)}</div></div>`).join('');
    document.getElementById('price-cards').innerHTML=[
      {l:'Total quotes',v:s.price_total,c:'blue'},{l:'Today new',v:s.price_today_new,c:'amber'},
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
    document.getElementById('email-import-result').textContent='Done: '+r.imported+' imported ('+r.updated+' updated, '+r.new+' new, '+r.skipped+' skipped)';
    document.getElementById('email-import-text').value='';
    loadStats();loadEmailTable();
  }catch(e){
    document.getElementById('email-import-result').textContent='Error: '+e.message;
  }
}

async function loadLogTable(filterType){
  const url=API+'/api/log/list?limit=1000';
  const r=await fetch(url).then(r=>r.json());
  let logs=r.logs||[];

  // Compute pool-level stats from all logs
  const domainCount=logs.filter(l=>l.type && l.type.startsWith('domain_')).length;
  const emailCount=logs.filter(l=>l.type && l.type.startsWith('email_')).length;
  const replyCount=logs.filter(l=>l.type && l.type.startsWith('reply_')).length;
  const priceCount=logs.filter(l=>l.type && l.type.startsWith('price_')).length;

  document.getElementById('log-cards').innerHTML=[
    {l:'Domain Pool', v:domainCount, c:'blue'},
    {l:'Email Pool', v:emailCount, c:'amber'},
    {l:'Reply Pool', v:replyCount, c:'green'},
    {l:'Price Pool', v:priceCount, c:'purple'},
  ].map(c=>`<div class="card"><div class="label">${c.l}</div><div class="value ${c.c}">${fmt(c.v)}</div></div>`).join('');

  // Filter by pool prefix or exact type
  if(filterType){
    if(filterType==='domain'){
      logs=logs.filter(l=>l.type && l.type.startsWith('domain_'));
    }else if(filterType==='email'){
      logs=logs.filter(l=>l.type && l.type.startsWith('email_'));
    }else if(filterType==='reply'){
      logs=logs.filter(l=>l.type && l.type.startsWith('reply_'));
    }else if(filterType==='price'){
      logs=logs.filter(l=>l.type && l.type.startsWith('price_'));
    }else{
      logs=logs.filter(l=>l.type===filterType);
    }
  }

  document.getElementById('log-table').innerHTML='<tr><th>Time</th><th>User</th><th>Action</th><th>Table</th><th>Count</th><th>Detail</th></tr>'+
    logs.slice(0,100).map(l=>`<tr>
      <td>${(l.time||'').slice(0,16)}</td>
      <td>${esc(l.user)}</td>
      <td><span class="status-${l.type||'New'}">${l.type||'-'}</span></td>
      <td>${esc(l.table)}</td>
      <td>${l.count||0}</td>
      <td style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(l.detail)}</td>
    </tr>`).join('');
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

async function loadPriceTable(){
  const r=await fetch(API+'/api/price/list?limit=100').then(r=>r.json());
  document.getElementById('price-table').innerHTML='<tr><th>Domain</th><th>Supplier</th><th>Contact</th><th>Price</th><th>Currency</th><th>Type</th><th>Status</th><th>Created</th></tr>'+
    (r.prices||[]).map(p=>`<tr>
      <td>${esc(p.domain)}</td><td>${esc(p.supplier)}</td><td>${esc(p.contact_email)}</td>
      <td>${p.price||'-'}</td><td>${p.currency||'-'}</td><td>${esc(p.link_type)}</td>
      <td>${esc(p.status)}</td><td>${(p.created_at||'').slice(0,16)}</td>
    </tr>`).join('');
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
  const r=await fetch(API+'/api/email/export',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({user,count:500})}).then(r=>r.json());
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
