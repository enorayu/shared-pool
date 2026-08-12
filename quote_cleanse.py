# -*- coding: utf-8 -*-
"""
quote_cleanse.py — Quote Pool 数据清洗 & 标准化（流程第一步）
===============================================================

职责（仅清洗 quote_pool，不碰爬虫、不碰展示逻辑）：
  优先级1  域名补全：按序从 domain(email域名除外) → 邮箱域名 → 正文URL →
           邮件签名 → 供应商其他网站信息 → 多站点其他域名 寻找真实网站域名。
           能确认 → 写 domain；无法确认 → 保留原值 + 标记 NEED_DOMAIN。
           禁止猜域名、禁止按供应商名强行匹配。
  优先级2  货币标准化：拆 price → original_price + original_currency；
           按静态汇率表折算 normalized_price(USD)。保留原始报价不覆盖。
  优先级3  报价类型：cooperation_type 自由文本 →
           Guest Post / Link Insertion / Sponsored Post / Other → price_type。
  优先级4  数据状态：READY / NEED_DOMAIN / NEED_PRICE / NEED_REVIEW → data_status。

使用方法：
  python quote_cleanse.py --dry-run      # 只读预览，不改库，打印分布
  python quote_cleanse.py --apply        # 真正写回 6 列
  python quote_cleanse.py --apply --limit 50   # 只处理前 50 条（调试）

依赖：config.py (SUPABASE_URL, SUPABASE_ANON_KEY)
"""
import sys, os, re, json, argparse, time as _time, urllib.request, urllib.parse, urllib.error

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from config import SUPABASE_URL, SUPABASE_ANON_KEY
except ImportError:
    print("ERROR: config.py not found")
    sys.exit(1)

# service_role key（仅用于 ALTER / 写回，不从聊天传入）：
#   优先读环境变量 SUPABASE_SR_KEY，其次读同目录 _sr_key.txt（用后删除）。
#   绝不硬编码、绝不写日志。
import os as _os
SR_KEY = _os.environ.get("SUPABASE_SR_KEY") or ""
if not SR_KEY:
    _sr_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_sr_key.txt")
    if os.path.exists(_sr_path):
        with open(_sr_path, "r", encoding="utf-8") as _f:
            SR_KEY = _f.read().strip()

REST = f"{SUPABASE_URL}/rest/v1"
H = {"apikey": SUPABASE_ANON_KEY, "Authorization": f"Bearer {SUPABASE_ANON_KEY}"}
# 写回/ALTER 用 service_role（若提供），否则退回 anon（无 DDL 权限会失败并提示）
WRITE_H = {"apikey": SR_KEY, "Authorization": f"Bearer {SR_KEY}"} if SR_KEY else dict(H)

# ── 基础货币 & 静态汇率表（1 单位原币 = ? USD）──────────────
BASE_CURRENCY = "USD"
# 改这里即可调整汇率；清洗为一次性快照，不联网
FX_TO_USD = {
    "USD": 1.0,
    "EUR": 1.08,
    "GBP": 1.27,
    "INR": 0.012,
    "CNY": 0.14,
    "CAD": 0.73,
    "AUD": 0.66,
    "JPY": 0.0067,
    "RUB": 0.011,
    "BRL": 0.18,
    "ZAR": 0.054,
    "AED": 0.27,
    "SGD": 0.74,
    "CHF": 1.12,
    "SEK": 0.095,
    "NOK": 0.092,
    "PLN": 0.25,
    "MXN": 0.058,
    "TRY": 0.030,
    "IDR": 0.000063,
    "PHP": 0.017,
    "THB": 0.028,
    "MYR": 0.21,
    "NZD": 0.61,
    "HKD": 0.128,
    "KRW": 0.00074,
}

# 邮箱服务商域名（这些是"邮箱域名"，不是"网站域名"，不能当真实域名）
EMAIL_PROVIDER_DOMAINS = {
    "gmail.com","yahoo.com","hotmail.com","outlook.com","live.com","msn.com",
    "qq.com","163.com","126.com","foxmail.com","sina.com","sohu.com","yeah.net",
    "proton.me","protonmail.com","icloud.com","me.com","aol.com","gmx.com",
    "zoho.com","mail.ru","yandex.com","yandex.ru","naver.com","daum.net",
    "web.de","t-online.de","orange.fr","free.fr","wanadoo.fr","libero.it",
    "email.com","gmx.net","gmx.de","ymail.com","googlemail.com","outlook.fr",
    "outlook.de","outlook.com","hotmail.fr","hotmail.co.uk","btinternet.com",
    "btopenworld.com","blueyonder.co.uk","virginmedia.com","sky.com","talktalk.co.uk",
}

# 合作类型关键词映射（按优先级匹配主类型）
COOP_RULES = [
    ("Sponsored Post", ["sponsored", "advertorial", "native ad", "promoted"]),
    ("Link Insertion", ["link insertion", "link insert", "niche edit", "niche edits",
                        "existing post", "contextual link", "insert link", "in-content link"]),
    ("Guest Post", ["guest post", "guestpost", "guest article", "guest blog", "write for us",
                    "article", "blog post", "publish"]),
]
# 其余（banner / sidebar / homepage / permanent link / brand mention 等）归 Other
OTHER_KEYWORDS = ["banner", "sidebar", "homepage", "permanent", "brand mention",
                  "link advertisement", "link ads", "newsletter", "social media",
                  "product", "service promotion", "listicle", "press release"]

URL_RE = re.compile(r"https?://([a-z0-9-]+(?:\.[a-z0-9-]+)+)", re.I)
# 宽松域名校验：必须含点、TLD 2+ 字母、非纯数字
DOMAIN_RE = re.compile(r"^(?=.{4,253}$)([a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}$", re.I)

# ── Domain 标准化（指令第三优先级）：统一 canonical domain ──
# https://www.example.com/article?x=1  →  example.com
# 处理：scheme / www / 路径 / 查询参数 / 尾部斜杠 / 大小写 / 常见格式噪音
def canonical_domain(raw):
    """把任意域名/URL 形式归一为注册域（小写、去 scheme/www/路径/参数）。
    返回 canonical 字符串；无法解析返回 None。"""
    if not raw:
        return None
    d = str(raw).strip().lower()
    if not d:
        return None
    # 去 scheme
    d = re.sub(r"^https?://", "", d, flags=re.I)
    # 去 // 之后的路径、参数、锚点
    d = d.split("/")[0]
    d = d.split("?")[0]
    d = d.split("#")[0]
    # 去认证信息 user@host
    if "@" in d:
        d = d.split("@")[-1]
    # 去端口
    d = d.split(":")[0]
    # 去 www. 前缀（仅最左一层 www）
    if d.startswith("www."):
        d = d[4:]
    d = d.strip(" .")
    if not d or not DOMAIN_RE.match(d):
        return None
    return d
PRICE_RE = re.compile(r"([$€£₹¥])\s?(\d[\d,]*\.?\d*)")
PRICE_CODE_RE = re.compile(r"(\d[\d,]*\.?\d*)\s*(usd|eur|gbp|inr|cny|cad|aad|aud|jpy|rub|brl|zar|aed|sgd|chf|sek|nok|pln|mxn|try|idr|php|thb|myr|nzd|hkd|krw|euros?|dollars?|bucks|rs|pounds?|£|₹|¥|€|\$)", re.I)
CUR_SYM_MAP = {"$": "USD", "€": "EUR", "£": "GBP", "₹": "INR", "¥": "CNY"}
WORD_CUR_MAP = {"usd":"USD","dollars":"USD","bucks":"USD","eur":"EUR","euro":"EUR","euros":"EUR",
                "gbp":"GBP","pounds":"GBP","inr":"INR","rs":"INR","cny":"CNY","jpy":"JPY",
                "cad":"CAD","aud":"AUD","rub":"RUB","brl":"BRL","zar":"ZAR","aed":"AED",
                "sgd":"SGD","chf":"CHF","sek":"SEK","nok":"NOK","pln":"PLN","mxn":"MXN",
                "try":"TRY","idr":"IDR","php":"PHP","thb":"THB","myr":"MYR","nzd":"NZD",
                "hkd":"HKD","krw":"KRW"}


def _req(method, path, params=None, data=None, extra_headers=None, headers=None, retries=4):
    url = f"{REST}/{path}"
    if params:
        qs = "&".join(f"{urllib.parse.quote(k)}={urllib.parse.quote(str(v))}"
                      for k, v in params.items() if v is not None)
        if qs:
            url += ("?" if "?" not in url else "&") + qs
    h = dict(headers if headers else H)
    if extra_headers:
        h.update(extra_headers)
    body = json.dumps(data).encode("utf-8") if data is not None else None
    if body is not None:
        h["Content-Type"] = "application/json"
    # 强制短连接，避免连接复用导致的 SSL 中途 EOF
    h["Connection"] = "close"
    last_err = None
    for attempt in range(retries):
        req = urllib.request.Request(url, data=body, headers=h, method=method)
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                raw = r.read().decode("utf-8")
                return (r.status, json.loads(raw) if raw else None)
        except urllib.error.HTTPError as e:
            return (e.code, {"_err": e.read().decode("utf-8", "ignore")[:400]})
        except Exception as e:  # SSL EOF / 网络抖动 → 重试
            last_err = e
            _time.sleep(1.5 * (attempt + 1))
    return (-1, {"_err": f"network retry failed: {last_err}"})


def columns_exist():
    """探测 6 列是否已存在（用 anon 试查一列即可）。"""
    st, _ = _req("GET", "quote_pool?select=original_price&limit=1")
    return st == 200

def ensure_columns():
    """确认 6 列是否存在。PostgREST 不支持 DDL（ALTER），加列必须手动在
    Supabase SQL Editor 执行 add_quote_cleanse_columns.sql。本函数只做探测与提示。"""
    if columns_exist():
        print("    [columns already exist, skip]")
        return True
    print("[!] 6 个标准化列尚未创建。PostgREST 不支持 ALTER，请手动执行：")
    print("    1) 打开 Supabase 控制台 → SQL Editor")
    print("    2) 粘贴 add_quote_cleanse_columns.sql 内容 → Run")
    print("    3) 重新运行本脚本 --apply")
    return False


def fetch_all(select, order="quote_id"):
    rows = []
    offset = 0
    while True:
        st, batch = _req("GET", f"quote_pool?select={urllib.parse.quote(select)}"
                         f"&limit=1000&offset={offset}&order={order}")
        if st != 200 or not isinstance(batch, list):
            print(f"  [fetch error] status={st} body={batch}")
            break
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


# ── 域名补全（优先级1）───────────────────────────────────────
def dom_from_email(email):
    if not email or "@" not in email:
        return None
    d = email.split("@")[-1].strip().lower()
    return d if DOMAIN_RE.match(d) else None

def candidate_real_domains(row):
    """按优先级顺序返回 (来源, 域名) 列表，已过滤邮箱服务商域名与非法域名。"""
    cands = []
    seen = set()
    def add(src, d):
        if not d:
            return
        # 第三优先级：先 canonical 标准化（去 scheme/www/路径/参数/大小写）
        d = canonical_domain(d)
        if not d:
            return
        if d in seen:
            return
        if not DOMAIN_RE.match(d):
            return
        if d in EMAIL_PROVIDER_DOMAINS:
            return  # 邮箱服务商域名不是网站域名
        seen.add(d)
        cands.append((src, d))

    content = " ".join([row.get("content") or "", row.get("reply_content") or "",
                        row.get("notes") or ""])
    supplier = row.get("supplier") or ""
    email = row.get("email") or ""
    contact_email = row.get("contact_email") or ""

    # 顺序 1：当前 domain（仅当它看起来像真实网站域名时）
    cur = (row.get("domain") or "").strip()
    if cur and cur not in EMAIL_PROVIDER_DOMAINS and canonical_domain(cur):
        add("current_domain", cur)

    # 顺序 2：邮箱域名（非服务商）
    for e in (email, contact_email):
        d = dom_from_email(e)
        if d and d not in EMAIL_PROVIDER_DOMAINS:
            add("email_domain", d)

    # 顺序 3：正文 / 邮件中的 URL
    for m in URL_RE.finditer(content):
        add("content_url", m.group(1))

    # 顺序 4：邮件签名 / 供应商名里出现的域名
    #  (a) "Name (site.com)" 或 "<site.com>"
    #  (b) "Name <site.com@gmail.com>" 中 @ 前的 site.com（常见博客主把域名当邮箱前缀）
    sig_domains = set(re.findall(r"\(([a-z0-9.-]+\.[a-z]{2,})\)", supplier, re.I))
    sig_domains |= set(re.findall(r"<([a-z0-9.-]+\.[a-z]{2,})[@>]", supplier, re.I))
    sig_domains |= set(re.findall(r"<?[a-z0-9._-]*?([a-z0-9-]+\.[a-z]{2,})@[a-z0-9.-]+>", supplier, re.I))
    for d in sig_domains:
        add("signature", d)

    # 顺序 5：供应商提供的其他网站信息（notes / content 中提及的非URL裸域名）
    # 例："our network: example.com, foo.net"
    bare = set(re.findall(r"(?<!\w)([a-z0-9-]+(?:\.[a-z0-9-]+){1,}\.[a-z]{2,})(?!\w)", content, re.I))
    for d in bare:
        add("other_site_info", d)

    # 顺序 6：多站点报价中的其他域名 —— 同 supplier 下其他记录已确认的 domain
    # （这一步在脚本主循环里用全局 map 二次回填，见 run()）

    return cands

def pick_domain(cands):
    """返回 (domain, source) 或 (None, None)。规则：直接取第一个候选。
    禁止猜测；若候选为空则 None。"""
    if not cands:
        return None, None
    return cands[0][1], cands[0][0]


# ── 货币标准化（优先级2）─────────────────────────────────────
def parse_amount(text):
    if not text:
        return None, None
    t = str(text).strip()
    # 1) 货币符号
    m = PRICE_RE.search(t)
    if m:
        sym = m.group(1)
        num = m.group(2).replace(",", "")
        try:
            float(num)
        except ValueError:
            return None, None
        return num, CUR_SYM_MAP.get(sym, "USD")
    # 2) 数字 + 货币代码/词
    m = PRICE_CODE_RE.search(t)
    if m:
        num = m.group(1).replace(",", "")
        cur = WORD_CUR_MAP.get(m.group(2).lower(), "USD")
        try:
            float(num)
        except ValueError:
            return None, None
        return num, cur
    # 3) 紧邻 price/cost/rate 关键词的纯数字（默认 USD）
    m = re.search(r"(?:price|cost|rate|fee|charged?|per post|per link|pricing)\D{0,15}?(\d[\d,]*\.?\d*)", t, re.I)
    if m:
        num = m.group(1).replace(",", "")
        try:
            float(num)
        except ValueError:
            return None, None
        return num, "USD"
    return None, None

def normalize(price, cur):
    if price is None or cur is None:
        return None
    try:
        p = float(str(price).replace(",", ""))
    except ValueError:
        return None
    rate = FX_TO_USD.get(cur.upper())
    if rate is None:
        return None
    return round(p * rate, 2)


# ── 报价类型标准化（优先级3）─────────────────────────────────
def classify_coop(text):
    if not text:
        return "Other"
    low = text.lower()
    # 优先匹配更具体的 Sponsored / Link Insertion，再 Guest Post
    for label, kws in COOP_RULES:
        for kw in kws:
            if kw in low:
                return label
    for kw in OTHER_KEYWORDS:
        if kw in low:
            return "Other"
    return "Other"


# ── data_status（优先级4）───────────────────────────────────
def decide_status(domain_ok, price_ok, conflict):
    if conflict:
        return "NEED_REVIEW"
    if not domain_ok:
        return "NEED_DOMAIN"
    if not price_ok:
        return "NEED_PRICE"
    return "READY"

# 多站点供应商：同一 supplier 名下出现多个不同真实域名是正常的（媒体集团/外链中介），
# 不应因此判冲突。只有"单条记录内部"信号相互矛盾才算冲突。
def detect_conflict(row, resolved_domain, candidate_domains):
    """返回 True 仅当记录内部存在真实矛盾：
       1) 正文中出现与已解析 domain 明显不同的『主站』域名（签名/官网），暗示 domain 可能绑错；
       2) price 字段解析出两个互斥金额（区间两种货币等极端情况由人工复核）。
       多站点供应商（supplier 维度多域名）不算冲突。"""
    content = " ".join([row.get("content") or "", row.get("reply_content") or "",
                        row.get("notes") or ""])
    # 规则1（已移除）：供应商字段里的域名本身就是候选来源之一，采用它属正常，
    #   不应判冲突。多站点供应商同 supplier 多域名也正常。
    # 规则2：仅当同一条记录内解析出两个金额且折合 USD 后差距 >5 倍，
    #   才视为价格信息冲突（如 "$50 / €500"）。单纯"接受多币种"不算冲突。
    amts = []
    for m in re.finditer(r"([$€£₹¥])\s?(\d[\d,]*\.?\d*)", content):
        cur = CUR_SYM_MAP.get(m.group(1), "USD")
        try:
            v = float(m.group(2).replace(",", ""))
        except ValueError:
            continue
        if v <= 0 or v > 100000:
            continue
        amts.append((cur, v))
    if len(amts) >= 2:
        usd_vals = [v * FX_TO_USD.get(c, 1.0) for c, v in amts]
        mn, mx = min(usd_vals), max(usd_vals)
        if mn > 0 and mx / mn > 5:
            return True
    return False


# ── 主流程 ───────────────────────────────────────────────────
def run(apply=False, limit=None, dry_run=False, offset=0, batch=None):
    print(f"[*] BASE_CURRENCY={BASE_CURRENCY}; FX table has {len(FX_TO_USD)} currencies")
    select = ("quote_id,domain,email,contact_email,supplier,content,reply_content,"
              "notes,price,cooperation_type,status")
    rows = fetch_all(select)
    if limit:
        rows = rows[:limit]
    print(f"[*] loaded {len(rows)} rows")

    # 第一遍：收集每个 supplier 已确认的域名（用于顺序6 多站点回填）
    supplier_domains = {}  # supplier -> set(real domains confirmed elsewhere)

    updates = []          # (quote_id, patch)
    stats = {"READY":0,"NEED_DOMAIN":0,"NEED_PRICE":0,"NEED_REVIEW":0}
    domain_fixed = 0
    price_filled = 0
    type_filled = 0
    conflict_cases = []

    # 第一遍只算候选（不写），先建 supplier→domain 映射（用 canonical 值）
    prelim = {}
    for r in rows:
        cands = candidate_real_domains(r)
        prelim[r["quote_id"]] = cands
        sup = (r.get("supplier") or "").strip().lower()
        cur_can = canonical_domain(r.get("domain") or "")
        if sup and cur_can and cur_can not in EMAIL_PROVIDER_DOMAINS:
            supplier_domains.setdefault(sup, set()).add(cur_can)

    for r in rows:
        qid = r["quote_id"]
        cands = prelim[qid][:]
        sup = (r.get("supplier") or "").strip().lower()
        old_domain = canonical_domain(r.get("domain") or "")  # canonical 后的当前域名
        # 顺序6：多站点其他域名（同 supplier 其他记录已确认域名，且当前未命中）
        if sup in supplier_domains:
            for d in supplier_domains[sup]:
                if d != old_domain and d not in [c[1] for c in cands]:
                    cands.append(("multi_site", d))

        new_domain, src = pick_domain(cands)
        # domain_changed：canonical 后新旧不同才写回（含标准化 www/路径的情况）
        domain_changed = bool(new_domain) and new_domain != old_domain

        # 价格
        orig_price, orig_cur = parse_amount(r.get("price"))
        norm = normalize(orig_price, orig_cur)
        price_ok = orig_price is not None and norm is not None

        # 类型
        ptype = classify_coop(r.get("cooperation_type"))

        # 冲突检测：仅记录内部信号矛盾（多站点供应商不算）
        resolved = new_domain or (old_domain if (old_domain and old_domain not in EMAIL_PROVIDER_DOMAINS) else None)
        conflict = detect_conflict(r, resolved, [c[1] for c in cands])
        if conflict:
            conflict_cases.append((qid, resolved, r.get("supplier","")[:30]))

        domain_ok = bool(new_domain) or (bool(old_domain) and old_domain not in EMAIL_PROVIDER_DOMAINS)
        status = decide_status(domain_ok, price_ok, conflict)

        # 计数
        stats[status] += 1
        if domain_changed:
            domain_fixed += 1
        if orig_price is not None and r.get("price"):
            price_filled += 1
        if ptype != "Other" or (r.get("cooperation_type") or "").strip():
            type_filled += 1

        patch = {
            "original_price": orig_price,
            "original_currency": orig_cur,
            "normalized_price": norm,
            "normalized_currency": BASE_CURRENCY if norm is not None else None,
            "price_type": ptype,
            "data_status": status,
        }
        if domain_changed:
            patch["domain"] = new_domain
        updates.append((qid, patch))

    # 输出预览
    print("\n===== DRY-RUN PREVIEW =====" if dry_run else "===== APPLY PLAN =====")
    print(f"  domain fixed (email-provider -> real): {domain_fixed}")
    print(f"  price standardized:                   {price_filled}")
    print(f"  price_type set:                       {type_filled}")
    print(f"  conflict (NEED_REVIEW) candidates:    {len(conflict_cases)}")
    print("  data_status distribution:")
    for k in ("READY","NEED_DOMAIN","NEED_PRICE","NEED_REVIEW"):
        print(f"    {k:12s}: {stats[k]}")
    if conflict_cases[:5]:
        print("  sample conflicts (qid, domain, email_domain):")
        for c in conflict_cases[:5]:
            print("    ", c)

    if not apply:
        # 抽样打印几条域名修复示例
        print("\n  sample domain fixes:")
        cnt = 0
        for qid, patch in updates:
            if "domain" in patch:
                print(f"    qid={qid}: -> {patch['domain']}")
                cnt += 1
                if cnt >= 10: break
        return

    # 写回前先确保列存在（用 service_role ALTER）
    print("[*] ensuring columns exist ...")
    if not ensure_columns():
        print("[!] 加列失败，中止写回。请手动执行 add_quote_cleanse_columns.sql")
        return

    # 写回（支持 offset / batch 分批 + 断点续跑）
    if offset or batch:
        updates = updates[offset: (offset + batch) if batch else None]
        print(f"[*] batch slice offset={offset} batch={batch} -> {len(updates)} updates")
    print(f"[*] applying {len(updates)} updates ...")
    ok = 0
    fail = 0
    for i, (qid, patch) in enumerate(updates):
        # 过滤 None 值：PostgREST 不接受 null 写入 numeric/text 列（保留原值）
        clean_patch = {k: v for k, v in patch.items() if v is not None}
        try:
            st, res = _req("PATCH", f"quote_pool?quote_id=eq.{qid}", data=clean_patch, headers=WRITE_H)
        except Exception as e:
            fail += 1
            print(f"  [EXC@{offset+i}] qid={qid} {type(e).__name__}: {str(e)[:150]}")
            if fail <= 10:
                print(f"    patch={clean_patch}")
            # 网络层崩溃（SSL 中断）时停止本批，便于从断点续跑
            if "SSL" in str(e) or "EOF" in str(e) or "Connection" in str(e):
                print(f"  [!] network drop at index {offset+i}, stopping batch. Re-run with --offset {offset+i}")
                break
            continue
        if st in (200, 204):
            ok += 1
        else:
            fail += 1
            if fail <= 10:
                print(f"  [fail #{fail}] qid={qid} status={st} {str(res)[:200]}")
        # 每 10 条打一次进度并 flush（后台运行时可见实时进度）
        if (i + 1) % 10 == 0:
            print(f"  progress: {offset+i+1}/{len(updates)+offset} ok={ok} fail={fail}", flush=True)
    print(f"[*] done. ok={ok} fail={fail}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="只读预览，不改库")
    ap.add_argument("--apply", action="store_true", help="真正写回 6 列")
    ap.add_argument("--limit", type=int, default=None, help="只处理前 N 条")
    ap.add_argument("--offset", type=int, default=0, help="写回起点（断点续跑用）")
    ap.add_argument("--batch", type=int, default=None, help="每批写回条数")
    args = ap.parse_args()
    if not args.dry_run and not args.apply:
        args.dry_run = True
    run(apply=args.apply, limit=args.limit, dry_run=args.dry_run,
        offset=args.offset, batch=args.batch)
