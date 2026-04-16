"""Fix double-quote string literals in fct_stock_monthly_snapshot.sql"""
import re, pathlib

f = pathlib.Path(r'd:\Data Warehouse\dbt_project\models\marts\inventory\fct_stock_monthly_snapshot.sql')
sql = f.read_text(encoding='utf-8')

# Split by jinja blocks {{ ... }}, only fix SQL parts (not jinja args)
parts = re.split(r'(\{\{[^}]*?\}\})', sql)
result = []
for part in parts:
    if part.startswith('{{'):
        result.append(part)  # jinja call - leave as-is
    else:
        # Replace double-quoted SQL string literals with single-quoted
        # Pattern: double-quoted value after = or ( or space or , or >=
        fixed = re.sub(r'"([^"]+)"', lambda m: "'" + m.group(1) + "'", part)
        result.append(fixed)

fixed = ''.join(result)
f.write_text(fixed, encoding='utf-8')
print("Done.")
# Print sample to verify
for i, line in enumerate(fixed.split('\n')[29:45], start=30):
    print(f"{i}: {line}")
