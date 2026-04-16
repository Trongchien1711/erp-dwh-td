import sys; sys.path.insert(0, 'd:/Data Warehouse')
from elt.connections import get_pg_engine
from sqlalchemy import text

pg = get_pg_engine()
with pg.connect() as conn:
    # 1. fact_warehouse_stock NVL rows: does product_key resolve to a material?
    r = conn.execute(text("""
        SELECT ws.type_items,
               count(1)                   AS total,
               count(dm.material_key)     AS match_material,
               count(dp.product_key)      AS match_product
        FROM core.fact_warehouse_stock ws
        LEFT JOIN core.dim_product  dp ON dp.product_key = ws.product_key
        LEFT JOIN core.dim_material dm ON dm.material_id = dp.product_id
        GROUP BY ws.type_items
    """)).fetchall()
    print("fact_warehouse_stock: product_key -> dim_product -> dim_material match?")
    for row in r:
        print(f"  type={row[0]:10s} total={row[1]:,} match_material={row[2]:,} match_product={row[3]:,}")

    # 2. Sample nvl rows
    r2 = conn.execute(text("""
        SELECT ws.stock_id, ws.product_key, dp.product_id, dp.product_code, dp.type_products,
               dm2.material_code
        FROM core.fact_warehouse_stock ws
        LEFT JOIN core.dim_product  dp  ON dp.product_key  = ws.product_key
        LEFT JOIN core.dim_material dm2 ON dm2.material_id = dp.product_id
        WHERE ws.type_items = 'nvl' AND ws.product_key IS NOT NULL
        LIMIT 6
    """)).fetchall()
    print("\nSample NVL rows in fact_warehouse_stock:")
    print(f"  {'stock_id':<10} {'prod_key':<10} {'prod_id':<10} {'prod_code':<20} {'type':<14} {'mat_code'}")
    for row in r2:
        print(f"  {str(row[0]):<10} {str(row[1]):<10} {str(row[2]):<10} {str(row[3] or 'NULL'):<20} {str(row[4] or 'NULL'):<14} {row[5] or 'NULL'}")

    # 3. Try direct join: nvl lot product_key -> dim_product.product_id -> dim_material.material_id
    r3 = conn.execute(text("""
        SELECT
            count(1)                                     AS nvl_total,
            count(dm.material_key)                       AS via_product_key,
            count(dm2.material_key)                      AS via_raw_product_id
        FROM core.fact_warehouse_stock ws
        LEFT JOIN core.dim_product  dp  ON dp.product_key  = ws.product_key
        LEFT JOIN core.dim_material dm  ON dm.material_id  = dp.product_id
        LEFT JOIN (
            SELECT wp.id AS stock_id, dm3.material_key
            FROM staging.tblwarehouse_product wp
            JOIN core.dim_material dm3 ON dm3.material_id = wp.product_id
            WHERE wp.type_items = 'nvl'
        ) dm2 ON dm2.stock_id = ws.stock_id
        WHERE ws.type_items = 'nvl'
    """)).fetchone()
    print(f"\nNVL total: {r3[0]:,}")
    print(f"Match via product_key->dim_product->dim_material: {r3[1]:,}")
    print(f"Match via staging stock_id->dim_material direct:  {r3[2]:,}")
