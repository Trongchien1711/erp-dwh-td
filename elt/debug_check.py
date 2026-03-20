"""
debug_check.py
Kiem tra trang thai hien tai cua staging (PostgreSQL) -- chi doc, khong ghi.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
from sqlalchemy import text
from connections import get_pg_engine
from extractor import TABLE_CONFIG

pg = get_pg_engine()

# ── Lấy danh sách bảng đang tồn tại trong staging ────────────────────────────
with pg.connect() as conn:
    existing: dict[str, int] = {}
    for row in conn.execute(text(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'staging' ORDER BY tablename"
    )).fetchall():
        tbl = row[0]
        cnt = conn.execute(text(f'SELECT COUNT(*) FROM staging."{tbl}"')).scalar()
        existing[tbl] = cnt

    # Watermark
    wm_rows = conn.execute(text(
        "SELECT table_name, last_loaded_at, updated_at "
        "FROM staging.etl_watermark ORDER BY table_name"
    )).fetchall() if "etl_watermark" in existing else []
    watermarks: dict[str, str] = {r[0]: str(r[1]) for r in wm_rows}

    # Kiểm tra cột date bị lưu TEXT trong staging
    date_col_types: dict[str, list[str]] = {}
    for tbl in existing:
        if tbl == "etl_watermark":
            continue
        bad_cols = conn.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'staging'
              AND table_name   = :tbl
              AND data_type    = 'text'
              AND column_name  ILIKE ANY(ARRAY['%date%','%time%','%created%','%updated%'])
        """), {"tbl": tbl}).fetchall()
        if bad_cols:
            date_col_types[tbl] = [r[0] for r in bad_cols]

# ── Report ────────────────────────────────────────────────────────────────────
configured = {c["source_table"]: c["watermark_col"] for c in TABLE_CONFIG}

print(f"{'TABLE':<45} {'ROWS':>8}  {'WM_COL':<20} {'LAST_LOADED_AT':<22}  STATUS")
print("-" * 115)

missing_tables = []
empty_tables   = []
text_date_warn = []

for stbl, wm_col in sorted(configured.items()):
    if stbl not in existing:
        missing_tables.append(stbl)
        wm_col_disp = wm_col or "(full load)"
        print(f"  {stbl:<43} {'--':>8}  {wm_col_disp:<20} {'--':<22}  MISSING")
        continue

    cnt  = existing[stbl]
    last = watermarks.get(stbl, "(no watermark)")
    wm_col_disp = wm_col or "(full load)"
    status = "OK"

    if cnt == 0:
        empty_tables.append(stbl)
        status = "EMPTY"

    if stbl in date_col_types:
        text_date_warn.append((stbl, date_col_types[stbl]))
        status += " | DATE->TEXT"

    print(f"  {stbl:<43} {cnt:>8,}  {wm_col_disp:<20} {last:<22}  {status}")

# Các bảng trong staging nhưng không có trong TABLE_CONFIG (ví dụ etl_watermark)
extra = sorted(set(existing) - set(configured) - {"etl_watermark"})
if extra:
    print()
    print("=== Bang staging ngoai TABLE_CONFIG ===")
    for tbl in extra:
        print(f"  {tbl:<43} {existing[tbl]:>8,}")

# ── Summary ───────────────────────────────────────────────────────────────────
total = len(configured)
ok    = total - len(missing_tables) - len(empty_tables)
print()
print(f"=== STAGING SUMMARY: {total} configured | {ok} OK | {len(empty_tables)} empty | {len(missing_tables)} missing ===")

if missing_tables:
    print(f"\n  MISSING ({len(missing_tables)}) -- chua chay pipeline lan nao:")
    for t in missing_tables:
        print(f"    {t}")

if empty_tables:
    print(f"\n  EMPTY ({len(empty_tables)}) -- da ton tai nhung 0 dong:")
    for t in empty_tables:
        print(f"    {t}")

if text_date_warn:
    print(f"\n  DATE->TEXT ({len(text_date_warn)}) -- cot ngay dang luu TEXT, nen chay fix_grant.py + pipeline lai:")
    for tbl, cols in text_date_warn:
        print(f"    staging.{tbl}: {', '.join(cols)}")



