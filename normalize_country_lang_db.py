"""
库内 quote_pool country/languages 简写 -> 全称化。

与前端 loadQuoteTable 的 COUNTRY_FULL / LANG_FULL 映射保持一致。
流程:
  1. DRY_RUN=True 时只统计 + 备份, 不写库
  2. 备份受影响行的 (quote_id, country, languages) 到 _quarantine_/quote_country_lang_backup_<ts>.json
  3. 按 quote_id 批量 PATCH (每批 100)

用法:
  python normalize_country_lang_db.py          # dry-run (默认)
  python normalize_country_lang_db.py --apply  # 真正写库
"""
import os, sys, json, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config
import requests  # 沙箱内 urllib SSL 不通, 用 requests

SUPABASE_URL = config.SUPABASE_URL
KEY = config.SUPABASE_ANON_KEY
TABLE = "quote_pool"

HEADERS = {
    "apikey": KEY,
    "Authorization": f"Bearer {KEY}",
    "Content-Type": "application/json",
}

# ---- 与国家/语言全称映射 (与前端 COUNTRY_FULL / LANG_FULL 完全一致) ----
COUNTRY_FULL = {
    'IN': 'India', 'US': 'United States', 'GB': 'United Kingdom', 'UK': 'United Kingdom',
    'CN': 'China', 'DE': 'Germany', 'FR': 'France', 'IT': 'Italy', 'ES': 'Spain',
    'PT': 'Portugal', 'BR': 'Brazil', 'CA': 'Canada', 'AU': 'Australia', 'NZ': 'New Zealand',
    'JP': 'Japan', 'KR': 'South Korea', 'RU': 'Russia', 'MX': 'Mexico', 'AR': 'Argentina',
    'CL': 'Chile', 'CO': 'Colombia', 'PE': 'Peru', 'NL': 'Netherlands', 'BE': 'Belgium',
    'SE': 'Sweden', 'NO': 'Norway', 'FI': 'Finland', 'DK': 'Denmark', 'PL': 'Poland',
    'TR': 'Turkey', 'GR': 'Greece', 'EG': 'Egypt', 'ZA': 'South Africa', 'NG': 'Nigeria',
    'KE': 'Kenya', 'MA': 'Morocco', 'DZ': 'Algeria', 'TN': 'Tunisia', 'BF': 'Burkina Faso',
    'ML': 'Mali', 'CM': 'Cameroon', 'TD': 'Chad', 'PK': 'Pakistan', 'BD': 'Bangladesh',
    'LK': 'Sri Lanka', 'NP': 'Nepal', 'TH': 'Thailand', 'VN': 'Vietnam', 'ID': 'Indonesia',
    'MY': 'Malaysia', 'SG': 'Singapore', 'PH': 'Philippines', 'HK': 'Hong Kong', 'TW': 'Taiwan',
    'AE': 'United Arab Emirates', 'SA': 'Saudi Arabia', 'IL': 'Israel', 'CH': 'Switzerland',
    'AT': 'Austria', 'IE': 'Ireland', 'CZ': 'Czechia', 'HU': 'Hungary', 'RO': 'Romania',
    'UA': 'Ukraine', 'HN': 'Honduras', 'BO': 'Bolivia', 'VE': 'Venezuela', 'EC': 'Ecuador',
    'UY': 'Uruguay', 'PY': 'Paraguay', 'CR': 'Costa Rica', 'PA': 'Panama', 'DO': 'Dominican Republic',
    'CU': 'Cuba', 'JM': 'Jamaica', 'TT': 'Trinidad and Tobago', 'GT': 'Guatemala',
    'SV': 'El Salvador', 'NI': 'Nicaragua',
}
LANG_FULL = {
    'en': 'English', 'fr': 'French', 'es': 'Spanish', 'de': 'German', 'it': 'Italian',
    'pt': 'Portuguese', 'nl': 'Dutch', 'ru': 'Russian', 'pl': 'Polish', 'tr': 'Turkish',
    'ar': 'Arabic', 'zh': 'Chinese', 'ja': 'Japanese', 'ko': 'Korean', 'hi': 'Hindi',
    'bn': 'Bengali', 'ur': 'Urdu', 'fa': 'Persian', 'he': 'Hebrew', 'th': 'Thai',
    'vi': 'Vietnamese', 'id': 'Indonesian', 'ms': 'Malay', 'sv': 'Swedish', 'da': 'Danish',
    'fi': 'Finnish', 'cs': 'Czech', 'hu': 'Hungarian', 'ro': 'Romanian', 'el': 'Greek',
    'uk': 'Ukrainian', 'sw': 'Swahili',
}

def full_country(v):
    if not v:
        return v
    v = str(v).strip()
    if not v:
        return v
    if v.upper() in COUNTRY_FULL:
        return COUNTRY_FULL[v.upper()]
    return v  # 已经含空格(=已是全称)或不在映射表, 原样返回

def full_lang(v):
    if not v:
        return v
    v = str(v).strip()
    if not v:
        return v
    if v.lower() in LANG_FULL:
        return LANG_FULL[v.lower()]
    if '/' in v:
        ps = [s.strip() for s in v.split('/')]
        ps[0] = LANG_FULL.get(ps[0].lower(), ps[0])
        return ' / '.join(ps)
    return v

DRY_RUN = '--apply' not in sys.argv

def supa_get(url):
    last = None
    for _ in range(5):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30, verify=False)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(2)
    raise last

def supa_patch(qid, payload):
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?quote_id=eq.{qid}"
    last = None
    for _ in range(5):
        try:
            r = requests.patch(url, json=payload, headers={**HEADERS, "Prefer": "return=representation"}, timeout=30, verify=False)
            r.raise_for_status()
            return r.status_code
        except Exception as e:
            last = e
            time.sleep(2)
    raise last

def main():
    ts = time.strftime("%Y%m%d_%H%M%S")
    print(f"[{'DRY-RUN' if DRY_RUN else 'APPLY'}] starting at {ts}")
    # 全量拉 country/languages
    updates = []
    uncovered_country = set()
    uncovered_lang = set()
    offset = 0
    page = 500
    while True:
        url = f"{SUPABASE_URL}/rest/v1/{TABLE}?select=quote_id,country,languages&limit={page}&offset={offset}"
        rows = supa_get(url)
        if not rows:
            break
        for r in rows:
            qid = r.get('quote_id')
            c = r.get('country')
            l = r.get('languages')
            nc = full_country(c)
            nl = full_lang(l)
            changed = (nc != c) or (nl != l)
            if c and c != nc and c.upper() not in COUNTRY_FULL:
                uncovered_country.add(str(c))
            if l and l != nl:
                # 检查是否因首词不在映射表
                first = str(l).split('/')[0].strip().lower()
                if first in LANG_FULL or (l in LANG_FULL):
                    pass
                else:
                    uncovered_lang.add(str(l))
            if changed:
                updates.append((qid, c, l, nc, nl))
        offset += page
        if len(rows) < page:
            break
    print(f"total rows scanned, to-update: {len(updates)}")
    if uncovered_country:
        print("UNCORDRED country values (not in map, left as-is):", sorted(uncovered_country)[:30])
    if uncovered_lang:
        print("UNCOVERED lang values (first token not in map, left as-is):", sorted(uncovered_lang)[:30])

    # 备份受影响行当前值
    backup_path = f"D:/desktop/_quarantine_/quote_country_lang_backup_{ts}.json"
    backup = [{"quote_id": q, "country_old": c, "languages_old": l, "country_new": nc, "languages_new": nl}
              for (q, c, l, nc, nl) in updates]
    with open(backup_path, 'w', encoding='utf-8') as f:
        json.dump(backup, f, ensure_ascii=False, indent=2)
    print(f"backup written: {backup_path} ({len(backup)} rows)")

    if DRY_RUN:
        # 预览前 10 条
        for u in updates[:10]:
            print("  PREVIEW", u)
        print("DRY-RUN done. Re-run with --apply to write.")
        return

    # APPLY: 批量 PATCH
    BATCH = 100
    done = 0
    for i in range(0, len(updates), BATCH):
        chunk = updates[i:i+BATCH]
        for (qid, c, l, nc, nl) in chunk:
            payload = {}
            if nc != c:
                payload['country'] = nc
            if nl != l:
                payload['languages'] = nl
            try:
                supa_patch(qid, payload)
                done += 1
            except Exception as e:
                print(f"  ERR qid={qid}: {e}")
        print(f"  applied {min(i+BATCH, len(updates))}/{len(updates)}")
    print(f"APPLY done. updated {done} rows.")

if __name__ == "__main__":
    main()
