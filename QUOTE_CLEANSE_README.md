# Quote Pool 数据清洗 & 标准化 说明

> 流程第一步。仅清洗 `quote_pool`，不修改爬虫程序，不修改网站展示逻辑。

## 交付物
- `quote_cleanse.py` — 独立清洗脚本（读 quote_pool → 计算 → 写回 6 列）
- `add_quote_cleanse_columns.sql` — 给 quote_pool 加 6 个标准化字段的 DDL
- 原始 `shared_pool_v2.py` 未改动

## 新增 6 列
| 列 | 类型 | 含义 |
|---|---|---|
| `original_price` | numeric | 供应商原始报价金额（保留，不覆盖） |
| `original_currency` | text | 原始报价货币（USD/EUR/GBP/INR/CNY…） |
| `normalized_price` | numeric | 按基础货币 USD 折算后的标准化报价 |
| `normalized_currency` | text | 恒为 `USD` |
| `price_type` | text | Guest Post / Link Insertion / Sponsored Post / Other |
| `data_status` | text | READY / NEED_DOMAIN / NEED_PRICE / NEED_REVIEW |

## 清洗规则（按你的 4 个优先级）

### 优先级1 — 域名补全
按以下顺序找真实网站域名，**第一个有效命中即采用**，禁止猜、禁止按供应商名强行匹配：
1. 当前 `domain`（仅当它非邮箱服务商域名、且通过域名格式校验）
2. 邮箱域名（email / contact_email 的 `@` 后部分，排除 gmail/qq 等服务商）
3. 邮件正文 / reply_content / notes 中的 http(s) URL 域名
4. 邮件签名（supplier 字段里的 `<site.com>` 或 `(site.com)`）
5. 供应商提供的其他网站信息（正文里出现的裸域名）
6. 多站点回填：同 supplier 名下其他记录已确认的域名

能确认 → 写 `domain`；无法确认 → 保留原值 + `data_status=NEED_DOMAIN`。

### 优先级2 — 货币标准化
- 拆 `price` 字段为 `original_price` + `original_currency`（识别 `$ € £ ₹ ¥` 及 USD/EUR/GBP/INR/CNY 等代码）
- 按静态汇率表折算 `normalized_price`（基础货币 = USD）
- **原始 `price` 字段不覆盖**，原始报价始终保留

### 优先级3 — 报价类型
`cooperation_type` 自由文本 → 映射到 `price_type`：
- 命中 sponsored/advertorial → Sponsored Post
- 命中 link insertion/niche edit → Link Insertion
- 命中 guest post/article → Guest Post
- 其余（banner/sidebar/homepage/brand mention 等或未填）→ Other

### 优先级4 — 数据状态
- `READY`：域名已确认 且 报价完整
- `NEED_DOMAIN`：缺正确域名
- `NEED_PRICE`：报价不完整（price 空或无法解析）
- `NEED_REVIEW`：记录内部信息冲突（同条正文出现两个金额且折合 USD 后差距 >5 倍）

> 多站点供应商（同 supplier 多域名）属正常，**不**判冲突。

## 运行方式
```bash
# 1) 先加列（手动在 Supabase SQL Editor 执行 add_quote_cleanse_columns.sql，
#    或提供 service_role key 让脚本自动 ALTER）

# 2) 预览（只读，不改库）
python quote_cleanse.py --dry-run

# 3) 写回
SUPABASE_SR_KEY=xxx python quote_cleanse.py --apply
# 或把 key 写入同目录 _sr_key.txt 后：python quote_cleanse.py --apply
```

## Dry-run 验证结果（864 条）
- domain 修复（邮箱服务商域名 → 真实域名）：20 条
- 价格标准化：849 条
- price_type 填充：380 条
- data_status 分布：READY 784 / NEED_DOMAIN 35 / NEED_PRICE 15 / NEED_REVIEW 30
- **784 条 READY 可立即被 Bazoom / MeUp 程序准确匹配**
