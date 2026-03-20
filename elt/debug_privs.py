"""
debug_privs.py
Kiểm tra toàn bộ quyền PostgreSQL cần thiết trước khi chạy pipeline.
  - Schema:  staging (USAGE, CREATE), core (USAGE)
  - Staging: SELECT, INSERT, TRUNCATE, ownership (ALTER)
  - Core:    SELECT, INSERT
Thoát code 1 nếu phát hiện thiếu quyền.
"""
import sys
from sqlalchemy import text
from connections import get_pg_engine
from extractor import TABLE_CONFIG

pg = get_pg_engine()
issues: list[str] = []

with pg.connect() as conn:

    # ── 0. Identity ──────────────────────────────────────────────────────────
    r = conn.execute(text(
        "SELECT current_user, session_user, current_database()"
    )).fetchone()
    print(f"user={r[0]}  session_user={r[1]}  db={r[2]}\n")

    # ── 1. Schema-level privileges ────────────────────────────────────────────
    print("=== Schema privileges ===")
    schema_checks = [
        ("staging", "USAGE"),   # loader cần đọc/ghi bảng
        ("staging", "CREATE"),  # loader cần tạo bảng mới (if_exists='replace')
        ("core",    "USAGE"),   # transform cần ghi vào core
    ]
    for schema, priv in schema_checks:
        ok = conn.execute(
            text("SELECT has_schema_privilege(current_user, :s, :p)"),
            {"s": schema, "p": priv},
        ).scalar()
        tag = "OK  " if ok else "FAIL"
        if not ok:
            issues.append(f"Schema {schema!r}: thiếu quyền {priv}")
        print(f"  {tag}  schema={schema:<8} priv={priv}")

    # ── 2. staging table privileges ───────────────────────────────────────────
    print("\n=== Staging table privileges ===")
    all_staging = sorted({c["source_table"] for c in TABLE_CONFIG})
    all_staging.append("etl_watermark")  # watermark table

    existing_staging: set[str] = {
        r[0] for r in conn.execute(
            text("SELECT tablename FROM pg_tables WHERE schemaname = 'staging'")
        ).fetchall()
    }

    for tbl in all_staging:
        if tbl not in existing_staging:
            print(f"  ----  staging.{tbl:<42} (chưa tồn tại — sẽ tạo khi pipeline chạy lần đầu)")
            continue

        full = f"staging.{tbl}"
        privs = {
            "SELECT":        conn.execute(text("SELECT has_table_privilege(:t, 'SELECT')"),   {"t": full}).scalar(),
            "INSERT":        conn.execute(text("SELECT has_table_privilege(:t, 'INSERT')"),   {"t": full}).scalar(),
            "TRUNCATE":      conn.execute(text("SELECT has_table_privilege(:t, 'TRUNCATE')"), {"t": full}).scalar(),
            "OWNER(ALTER)":  conn.execute(
                text("SELECT tableowner = current_user FROM pg_tables "
                     "WHERE schemaname = 'staging' AND tablename = :tbl"),
                {"tbl": tbl},
            ).scalar(),
        }
        parts = []
        for label, ok in privs.items():
            parts.append(f"{label}={'OK  ' if ok else 'FAIL'}")
            if not ok:
                issues.append(f"staging.{tbl}: thiếu {label}")
        print(f"  staging.{tbl:<42}  {' | '.join(parts)}")

    # ── 3. core table privileges ──────────────────────────────────────────────
    print("\n=== Core table privileges ===")
    core_tables: list[str] = sorted(
        r[0] for r in conn.execute(
            text("SELECT tablename FROM pg_tables WHERE schemaname = 'core'")
        ).fetchall()
    )

    if not core_tables:
        print("  (không tìm thấy bảng nào trong schema core — chưa migrate?)")
    else:
        for tbl in core_tables:
            full = f"core.{tbl}"
            privs = {
                "SELECT": conn.execute(text("SELECT has_table_privilege(:t, 'SELECT')"), {"t": full}).scalar(),
                "INSERT": conn.execute(text("SELECT has_table_privilege(:t, 'INSERT')"), {"t": full}).scalar(),
            }
            parts = []
            for label, ok in privs.items():
                parts.append(f"{label}={'OK  ' if ok else 'FAIL'}")
                if not ok:
                    issues.append(f"core.{tbl}: thiếu {label}")
            print(f"  core.{tbl:<45}  {' | '.join(parts)}")

# ── Summary ───────────────────────────────────────────────────────────────────
print()
if issues:
    print(f"=== {len(issues)} VẤN ĐỀ — chạy fix_grant.py với superuser để cấp quyền ===")
    for iss in issues:
        print(f"  FAIL  {iss}")
    sys.exit(1)
else:
    print("=== ALL CHECKS PASSED — sẵn sàng chạy pipeline.py ===")
