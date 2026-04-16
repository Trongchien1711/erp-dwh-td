# Power BI Portfolio Project — ERP Data Warehouse

> **Author:** Nguyen Trong Chien — Data Analyst  
> **Stack:** MySQL (ERP) → Python ELT → PostgreSQL (Star Schema) → dbt → Power BI  
> **Data:** Anonymized production ERP data · ~72K orders · 2022–2025

---

## Mục lục
1. [Tổng quan dự án](#1-tổng-quan-dự-án)
2. [Từng bước thực hiện](#2-từng-bước-thực-hiện)
3. [Kết nối Power BI vào DWH](#3-kết-nối-power-bi-vào-dwh)
4. [Danh sách báo cáo có thể làm](#4-danh-sách-báo-cáo-có-thể-làm)
5. [Phân tích chuyên sâu](#5-phân-tích-chuyên-sâu)
6. [Mẫu layout báo cáo](#6-mẫu-layout-báo-cáo)
7. [Bảng mart — nguồn dữ liệu cho PBI](#7-bảng-mart--nguồn-dữ-liệu-cho-pbi)

---

## 1. Tổng quan dự án

Dự án xây dựng Data Warehouse hoàn chỉnh từ hệ thống ERP thực tế (nhà in bao bì), sau đó dùng Power BI để trực quan hóa insight kinh doanh. Dữ liệu đã được **ẩn danh hóa** (tên, số tiền giảm 90%) trước khi xuất bản.

**Kết quả nổi bật:**
- Tiết kiệm 85% thời gian báo cáo thủ công (4h → 15 phút/ngày)
- Phát hiện WIP tồn đọng ~20 tỷ VND
- NPL/Revenue đạt 24–32% (đúng benchmark ngành in bao bì)
- Phân khúc khách hàng RFM: 5 khách top đóng góp 80% doanh thu

---

## 2. Từng bước thực hiện

### Bước 1 — Hiểu nguồn dữ liệu
- Hệ thống ERP FOSO (MySQL 8): 25 bảng giao dịch
- Các domain: **Sales** (đơn hàng) · **Inventory** (kho + SX) · **Finance** (mua hàng + công nợ)
- Xác định grain, key, và mối quan hệ giữa các bảng (xem [bus_matrix.md](bus_matrix.md))

### Bước 2 — Xây dựng ELT Pipeline
```
Python 3.x → elt/pipeline.py
  - Incremental load (watermark) cho bảng giao dịch lớn
  - Full load cho bảng master nhỏ
  - COPY protocol (17x nhanh hơn INSERT)
  - Thời gian chạy: ~83 giây cho toàn bộ pipeline
```

### Bước 3 — Thiết kế Star Schema (Kimball)
```
PostgreSQL schema: core
  - 9 Dimension tables: dim_customer, dim_product, dim_staff,
    dim_supplier, dim_warehouse, dim_date, ...
  - 9 Fact tables: fact_orders, fact_order_items, fact_delivery_items,
    fact_warehouse_stock, fact_purchase_order_items, ...
  - Surrogate keys (SERIAL) tách khỏi ERP natural keys
  - Partitioned by date_key (YYYYMMDD INT)
```

### Bước 4 — Xây dựng Mart Layer với dbt
```
dbt Core → 12 mart models
  Sales:     fct_revenue, fct_order_performance, fct_order_items_detail,
             dim_customer_segmentation
  Inventory: fct_stock_snapshot, fct_inbound_outbound,
             fct_production_npl_cost, fct_order_npl_cost,
             fct_production_efficiency
  Finance:   fct_gross_profit, fct_purchase_cost, dim_customer_credit
```

### Bước 5 — Ẩn danh hóa dữ liệu
```sql
-- sql/09_anonymize_data.sql
- Tên khách hàng → "Khach Hang 001"
- Tên nhân viên  → "Nhan Vien 001"
- Tên nhà cung cấp → "Nha Cung Cap 001"
- Tất cả số tiền × 0.1 (giảm 90%)
```

### Bước 6 — Kết nối Power BI và xây dựng Dashboard
- Kết nối trực tiếp vào PostgreSQL schema `mart`
- Import các bảng mart (không cần viết SQL trong PBI)
- Tạo measures DAX trên nền dữ liệu đã chuẩn hóa

---

## 3. Kết nối Power BI vào DWH

### Thông tin kết nối PostgreSQL
| Trường | Giá trị |
|--------|---------|
| Server | `localhost` |
| Port | `5432` |
| Database | `erp_dwh` |
| Schema | `mart` |
| Username | `dwh_admin` |

### Cài driver
Power BI Desktop yêu cầu **Npgsql** hoặc **PostgreSQL ODBC driver**:
- Tải tại: https://github.com/npgsql/npgsql/releases (npgsql installer)

### Các bảng cần import vào PBI
```
mart.fct_revenue                 ← Doanh thu (grain: ngày × khách × chi nhánh)
mart.fct_order_performance       ← Hiệu suất đơn hàng (grain: 1 đơn)
mart.fct_order_items_detail      ← Chi tiết sản phẩm (grain: 1 dòng item)
mart.dim_customer_segmentation   ← RFM segment khách hàng (grain: 1 khách)
mart.fct_gross_profit            ← Lợi nhuận gộp (grain: ngày × khách)
mart.fct_purchase_cost           ← Chi phí mua hàng (grain: ngày × NCC × sản phẩm)
mart.dim_customer_credit         ← Công nợ khách hàng
mart.fct_stock_snapshot          ← Tồn kho hiện tại
mart.fct_inbound_outbound        ← Nhập/xuất kho
mart.fct_order_npl_cost          ← Chi phí NPL theo đơn hàng
mart.fct_production_efficiency   ← Hiệu suất sản xuất
```

### Data Model trong PBI (relationships)
```
fct_revenue           →  dim_customer_segmentation  (customer_key)
fct_order_performance →  dim_customer_segmentation  (customer_key)
fct_gross_profit      →  dim_customer_segmentation  (customer_key)
fct_order_npl_cost    →  fct_revenue                (order_id)
fct_stock_snapshot    → (standalone, filter by warehouse/product)
```

---

## 4. Danh sách báo cáo có thể làm

### 4.1 Sales Dashboard — Tổng quan doanh số
**Nguồn:** `mart.fct_revenue`, `mart.dim_customer_segmentation`

| Visual | Metric | Mô tả |
|--------|--------|-------|
| Card | Tổng doanh thu YTD | `SUM(revenue)` filter year |
| Card | Số đơn hàng | `COUNT(order_id)` |
| Card | AOV (Avg Order Value) | `revenue / order_count` |
| Line chart | Doanh thu theo tháng | Trend 12–24 tháng |
| Bar chart | Top 10 khách hàng | Xếp theo doanh thu |
| Donut | Phân loại khách hàng | type_client (B2B/B2C) |
| Map/Bar | Doanh thu theo tỉnh/thành | Trường `city` |
| Slicer | Năm / Quý / Nhóm giá | Lọc toàn bộ trang |

---

### 4.2 Customer 360 — Phân khúc khách hàng
**Nguồn:** `mart.dim_customer_segmentation`

| Visual | Metric | Mô tả |
|--------|--------|-------|
| Scatter plot | RFM: Recency vs Monetary | Bubble = Frequency |
| Bar chart | Số khách theo segment | Champions / Loyal / At Risk / Lost |
| Table | Danh sách khách At Risk | Lần mua cuối > 90 ngày |
| Card | % khách Champions | Đang chiếm bao nhiêu doanh thu |
| Funnel/Bar | Doanh thu theo RFM segment | So sánh value giữa các nhóm |

**Segment logic (có sẵn trong bảng):**
- **Champions** — mua gần đây, nhiều, chi nhiều
- **Loyal** — thường xuyên, ổn định
- **At Risk** — từng mua nhiều, không mua gần đây
- **Lost** — không mua > 180 ngày
- **New / Promising** — khách mới

---

### 4.3 Order Performance — Hiệu suất giao hàng
**Nguồn:** `mart.fct_order_performance`

| Visual | Metric | Mô tả |
|--------|--------|-------|
| Gauge | Tỉ lệ giao đủ hàng | `% is_fully_delivered` |
| Bar | Số đơn tồn đọng theo tháng | `outstanding_qty > 0` |
| Table | Đơn hàng chưa giao xong | Sort theo outstanding_qty |
| Histogram | Phân phối fulfilment_rate | Bao nhiêu đơn đạt 100% |
| Line | Trend fulfilment_rate | Theo tháng |

---

### 4.4 Gross Profit — Lợi nhuận gộp
**Nguồn:** `mart.fct_gross_profit`

| Visual | Metric | Mô tả |
|--------|--------|-------|
| Card | Gross Profit YTD | `SUM(gross_profit)` |
| Card | GP Margin % | `gross_profit / revenue × 100` |
| Waterfall | Revenue → COGS → Gross Profit | P&L waterfall |
| Line | GP Margin % theo tháng | Trend biên lợi nhuận |
| Bar | Top customer by profit | Ai mang lại lợi nhuận nhiều nhất |

> **Lưu ý:** COGS trong ERP chỉ có ~244/72K đơn → dùng `fct_order_npl_cost` để ước tính COGS thực tế tốt hơn.

---

### 4.5 NPL Cost Analysis — Chi phí nguyên vật liệu
**Nguồn:** `mart.fct_order_npl_cost`, `mart.fct_production_npl_cost`

| Visual | Metric | Mô tả |
|--------|--------|-------|
| Card | Total NPL Cost (VND) | `SUM(allocated_npl_vnd)` |
| Card | NPL/Revenue % (weighted avg) | Target 24–32% |
| Scatter | NPL% vs Revenue per order | Phát hiện outlier |
| Bar | Top NPL cost by product line | Nhóm sản phẩm tốn vật liệu nhất |
| Table | Đơn hàng suspect_data | NPL% > 500% (cần audit BOM) |
| Line | NPL% trend theo tháng | Target band 24–32% |

---

### 4.6 Procurement — Chi phí mua hàng
**Nguồn:** `mart.fct_purchase_cost`

| Visual | Metric | Mô tả |
|--------|--------|-------|
| Card | Total Purchase Cost YTD | `SUM(actual_cost)` |
| Bar | Top 10 nhà cung cấp by cost | Concentrapurchasing risk |
| Line | Chi phí mua theo tháng | Trend procurement |
| Table | Price variance by supplier | `actual_cost - expected_cost` |
| Bar | Top sản phẩm nhập nhiều nhất | Theo qty_ordered |

---

### 4.7 Inventory Snapshot — Tồn kho
**Nguồn:** `mart.fct_stock_snapshot`, `mart.fct_inbound_outbound`, `mart.dim_material_mart`

| Visual | Metric | Mô tả |
|--------|--------|-------|
| Card | Total SKU còn tồn | `COUNTROWS(FILTER(fct_stock_snapshot, quantity_left > 0))` |
| Card | Tổng giá trị tồn kho (định giá) | `SUM(stock_value)` — chỉ các lot `is_valued = TRUE` |
| Card | % lot có giá | `DIVIDE(COUNTROWS(FILTER(..., is_valued)), COUNTROWS(...))` |
| Table | Top sản phẩm tồn nhiều nhất | Sort by `quantity_left`, filter `item_type = 'product'` |
| Table | Top NPL tồn nhiều nhất | Filter `item_type = 'nvl'`, dùng `material_name`, `npl_type` |
| Bar | Tồn kho theo nhà kho | Group by `warehouse_name`, sum `quantity_left` |
| Bar | Inbound theo tháng | `fct_inbound_outbound` WHERE `movement_type = 'INBOUND'` |
| Bar | Outbound theo tháng | `fct_inbound_outbound` WHERE `movement_type = 'OUTBOUND'` |
| Slicer | Loại hàng | `item_type` = `product` / `nvl` |
| Slicer | Nhóm NPL | `npl_type` (Giay / Decal / Kem in / Muc UV / Bao bi...) |

**Lưu ý quan trọng về nguồn giá:**
- `price_source = 'lot_price'` (98%): giá lấy từ giá nhập kho gốc của lô → **đáng tin cậy nhất**
- `price_source = 'npl_po_fallback'` (~0.9%): giá lấy từ PO mua NPL gần nhất cùng vật liệu → **xấp xỉ**
- `price_source = 'po_fallback'` (sản phẩm mua ngoài): giá lấy từ PO nhà cung cấp → **xấp xỉ**
- `price_source = 'no_price'` (~1.1%): không có giá → loại khỏi tính tổng giá trị

**Không nên dùng inbound − outbound để tính tồn kho tháng quá khứ** (xem mục 5.4).

---

### 4.8 Production Efficiency — Hiệu suất sản xuất
**Nguồn:** `mart.fct_production_efficiency`

| Visual | Metric | Mô tả |
|--------|--------|-------|
| Gauge | Efficiency % tổng | `qty_produced / qty_planned` |
| Bar | Efficiency by product | Sản phẩm nào đạt tốt nhất |
| Line | Output per worker-hour trend | Năng suất lao động |
| Table | Lệnh SX dưới 80% efficiency | Cần điều tra |

---

### 4.9 Customer Credit Monitor — Theo dõi công nợ
**Nguồn:** `mart.dim_customer_credit`

| Visual | Metric | Mô tả |
|--------|--------|-------|
| Table | Khách gần vượt hạn mức | Outstanding AR vs debt_limit |
| Bar | Top khách theo outstanding AR | Ai nợ nhiều nhất |
| Gauge | Tổng AR / Tổng debt limit | Tỷ lệ sử dụng hạn mức toàn hệ thống |

---

## 5. Phân tích chuyên sâu

### 5.1 RFM Customer Segmentation
**Tại sao thú vị:** Đây là phân tích khách hàng cấp độ DA toàn diện.
```
Nguồn: mart.dim_customer_segmentation
- rfm_segment: Champions / Loyal / At Risk / Lost / New / Promising
- recency_score (1-5), frequency_score (1-5), monetary_score (1-5)
- lifetime_revenue, total_orders, last_order_date, first_order_date

Insight có thể khai thác:
  → Nhóm At Risk: ai là khách lớn đang rời bỏ?
  → Champions: top 5 khách đóng góp bao nhiêu % doanh thu?
  → Trend: segment nào đang tăng/giảm theo quý?
  → Phân tích cohort: khách hàng theo năm gia nhập
```

### 5.2 NPL Cost vs Revenue — Biên lợi nhuận thực tế
**Tại sao thú vị:** COGS trong ERP không đầy đủ, nhưng NPL từ BOM là nguồn chi phí chính xác nhất.
```
Nguồn: mart.fct_order_npl_cost JOIN mart.fct_revenue ON order_id
- Tính NPL% = allocated_npl_vnd / revenue_vnd × 100
- Target healthy: 24–32%
- Flag: normal / high_cost / suspect_data / no_revenue

Insight có thể khai thác:
  → Sản phẩm nào có NPL% cao nhất → chi phí không hiệu quả?
  → Khách hàng nào mang lại margin tốt nhất sau khi trừ NPL?
  → Tháng nào NPL % vượt benchmark → vấn đề gì trong SX?
  → 603 đơn hàng suspect_data → cần audit BOM cho 3D-MUCUV products
```

### 5.3 Delivery Performance & Outstanding Orders
**Tại sao thú vị:** Phản ánh trực tiếp khả năng fulfillment của công ty.
```
Nguồn: mart.fct_order_performance
- fulfilment_rate: % đơn giao đủ
- outstanding_qty: số lượng còn thiếu
- is_fully_delivered, is_completed

Insight có thể khai thác:
  → Tỉ lệ giao đủ hàng theo tháng có cải thiện không?
  → Khách nào thường xuyên nhận hàng thiếu?
  → Đơn hàng lớn (AOV cao) có tỉ lệ giao đủ thấp hơn không?
```

### 5.4 Inventory Analysis — Tồn kho & Vật tư
**Tại sao thú vị:** Phát hiện vốn bị chôn trong tồn kho, kiểm soát NPL nhập kho.

#### Kiến trúc dữ liệu tồn kho (đã xác minh)
```
core.fact_warehouse_stock     ← sổ cái lô hàng (lot ledger)
   ├── product_key            (sản phẩm: thành phẩm / bán thành phẩm)
   ├── material_key           (NPL: tblwarehouse_product.type_items = 'nvl')
   ├── quantity               tổng nhập gốc của lô
   ├── quantity_left          tồn hiện tại của lô
   └── quantity_exported      tổng đã xuất từ lô

mart.fct_stock_snapshot       ← view phân tích trên lot ledger
   ├── item_type              'product' | 'nvl'
   ├── item_code/item_name    coalesce(product_code, material_code)
   ├── unit_price             lot_price / quantity (fallback: PO price)
   ├── unit_price_capped      product: capped 50,000 VND; nvl: không cap
   ├── stock_value            quantity_left × unit_price_capped
   ├── price_source           'lot_price' | 'npl_po_fallback' | 'po_fallback' | 'no_price'
   └── is_valued              TRUE khi có lot_price gốc

mart.fct_inbound_outbound     ← movement log theo ngày
   ├── INBOUND  = từ fact_warehouse_stock (lot ledger, đầy đủ)
   └── OUTBOUND = từ fact_warehouse_export (export transaction log)
```

#### Tại sao KHÔNG thể reconstruct tồn tháng quá khứ
```
Vấn đề được xác nhận (14/04/2026):
  - Lot ledger outbound: 18,296,520,773 units (813K lots - products)
  - fact_warehouse_export: 18,328,282,703 units (859K rows - products)
  - Chênh lệch: +31M units / +46K rows

Nguyên nhân: fact_warehouse_export và lot ledger là 2 bảng khác nhau
  trong MySQL ERP, KHÔNG nhất thiết đối chiếu nhau:
  - Có thể bào gồm xuất từ lot đã xóa/hủy
  - Các giao dịch nội bộ được ghi double
  - Không có ngày trên từng dòng lot_quantity_export

Kết luận:
  ✓ fct_stock_snapshot   → TỒN KHO HIỆN TẠI: chính xác
  ✓ fct_inbound_outbound → TRENDING NHẬP/XUẤT theo thời gian: dùng được
  ✗ inbound - outbound   → RECONSTRUCT TỒN THÁNG QUÁ KHỨ: không tin cậy
```

#### DAX Measures cho tồn kho
```dax
-- Tổng tồn kho hiện tại (số lượng)
Total Stock Qty = SUM(fct_stock_snapshot[quantity_left])

-- Tổng giá trị tồn kho (chỉ lot có giá)
Stock Value =
    CALCULATE(
        SUM(fct_stock_snapshot[stock_value]),
        fct_stock_snapshot[is_valued] = TRUE()
    )

-- % lot được định giá
Valued Lot Pct =
    DIVIDE(
        COUNTROWS(FILTER(fct_stock_snapshot, fct_stock_snapshot[is_valued])),
        COUNTROWS(fct_stock_snapshot)
    )

-- Dead stock: lot nhập > 180 ngày chưa xuất hết
Dead Stock Qty =
    CALCULATE(
        SUM(fct_stock_snapshot[quantity_left]),
        DATEDIFF(fct_stock_snapshot[import_date_key],  -- cần join dim_date
                 TODAY(), DAY) > 180,
        fct_stock_snapshot[quantity_left] > 0
    )

-- Inbound tháng này
Inbound MTD =
    CALCULATE(
        SUM(fct_inbound_outbound[quantity_in]),
        fct_inbound_outbound[movement_type] = "INBOUND",
        DATESMTD(fct_inbound_outbound[movement_date])
    )
```

#### Insight khai thác được
```
→ Top sản phẩm tồn nhiều nhất: item_type='product', sort by quantity_left
→ Top NPL tồn nhiều nhất: item_type='nvl', group by npl_type (Giay/Decal/Kem in...)
→ Kho nào đang chứa nhiều giá trị nhất: group by warehouse_name, sum stock_value
→ Lot hết date_sd (hạng dùng): filter date_sd < TODAY(), quantity_left > 0
→ Dead stock: import_date_key cũ mà quantity_left > 0
→ Price source quality: breakdown is_valued, price_source cho audit
→ NPL coverage: 63,962/63,964 NVL lots có material_name (99.99%)
```

#### Số liệu thực tế (14/04/2026)
```
fct_stock_snapshot:  877,569 lots tổng
  product lots:      812,168  tồn = 80,736,383 units
  nvl lots:           63,961  tồn = 4,518,472 units
  UNKNOWN:             1,440  tồn =    13,929 units

Price coverage NVL (fct_stock_snapshot):
  lot_price:        62,702 lots (98.2%) — có giá gốc
  npl_po_fallback:     571 lots ( 0.9%) — ước tính từ PO
  no_price:            691 lots ( 1.1%) — không có giá

Price coverage Products (fct_stock_snapshot):
  no_price:         811,682 lots (99.9%) — ERP không ghi giá lô cho thành
                    phẩm sản xuất nội bộ (cost ở BOM/NPL level, không ở kho lô)
  lot_price:            430 lots — giá ≈ 0
  po_fallback:           56 lots — 11M VND (hàng mua ngoài)
  → Kết luận: giá trị tồn kho sản phẩm KHÔNG có trong ERP lot data.

fct_stock_monthly_snapshot: 200,574 rows — 39 tháng × product + NVL
  product: est_qty_end_month có (quantity trend), est_value_end_month = NULL
  nvl:     est_qty_end_month + est_value_end_month (~64-83% rows có giá)
           price source: fct_stock_snapshot weighted avg (lot_price → npl_po_fallback)
```

### 5.5 Supplier Concentration & Price Variance
**Tại sao thú vị:** Rủi ro chuỗi cung ứng và kiểm soát chi phí mua hàng.
```
Nguồn: mart.fct_purchase_cost
- expected_cost vs actual_cost → price_variance
- po_count per supplier per period

Insight:
  → Top 3 NCC chiếm bao nhiêu % tổng chi phí? (concentration risk)
  → NCC nào hay tính giá cao hơn kỳ vọng?
  → Trend chi phí nguyên vật liệu chính (UV ink, giấy Bristol...)
```

---

## 6. Mẫu layout báo cáo

### Template 1 — Executive Sales Dashboard (1 trang)
```
┌─────────────────────────────────────────────────────────────────┐
│  SALES OVERVIEW — [Slicer: Năm | Quý | Nhóm khách]             │
├──────────┬──────────┬──────────┬──────────────────────────────  │
│ Doanh    │  Số đơn  │   AOV    │  GP Margin %                   │
│ thu YTD  │  hàng    │  (VND)   │                                │
├──────────┴──────────┴──────────┴──────────────────────────────  │
│                                                                  │
│  [Line chart] Doanh thu theo tháng (hiện tại vs năm trước)      │
│                                                                  │
├───────────────────────────┬─────────────────────────────────────│
│  [Bar] Top 10 khách hàng  │  [Donut] Phân loại khách            │
│        by revenue         │           B2B / B2C / VIP           │
├───────────────────────────┴─────────────────────────────────────│
│  [Map/Bar] Doanh thu theo tỉnh/thành phố                        │
└─────────────────────────────────────────────────────────────────┘
```

### Template 2 — Customer Segmentation Page (1 trang)
```
┌─────────────────────────────────────────────────────────────────┐
│  CUSTOMER 360 — RFM Analysis                                    │
├──────────┬──────────┬──────────┬──────────────────────────────  │
│Champions │  Loyal   │ At Risk  │   Lost                         │
│  (n=xx)  │  (n=xx)  │  (n=xx)  │   (n=xx)                       │
├──────────┴──────────┴──────────┴──────────────────────────────  │
│                                          │                       │
│  [Scatter] Recency vs Monetary           │  [Bar] Revenue by    │
│            Bubble size = Frequency       │  RFM Segment         │
│                                          │                       │
├──────────────────────────────────────────┴──────────────────────│
│  [Table] At Risk customers — Name | Last Order | Revenue LTM    │
│          Sorted by lifetime_revenue DESC                        │
└─────────────────────────────────────────────────────────────────┘
```

### Template 3 — NPL Cost Profitability (1 trang)
```
┌─────────────────────────────────────────────────────────────────┐
│  NPL COST vs REVENUE — [Slicer: Năm | Tháng | npl_quality]     │
├──────────┬──────────┬──────────┬──────────────────────────────  │
│ NPL Cost │ Revenue  │  NPL%    │  Normal Orders %               │
│  Total   │  Total   │(weighted)│                                │
├──────────┴──────────┴──────────┴──────────────────────────────  │
│                                                                  │
│  [Line] NPL % theo tháng — với band mục tiêu 24%–32%           │
│                                                                  │
├───────────────────────────┬─────────────────────────────────────│
│  [Bar] Top sản phẩm có    │  [Scatter] NPL% vs Revenue         │
│  NPL cost cao nhất        │  Màu = npl_quality flag            │
├───────────────────────────┴─────────────────────────────────────│
│  [Table] Suspect orders — Order ID | Customer | NPL% | NOTE    │
└─────────────────────────────────────────────────────────────────┘
```

### Template 4 — Inventory & Procurement (1 trang)
```
┌─────────────────────────────────────────────────────────────────┐
│  INVENTORY & PROCUREMENT — [Slicer: Nhà kho | Loại sản phẩm]  │
├──────────┬──────────┬──────────┬──────────────────────────────  │
│ Total SKU│  Giá trị │ PO Cost  │  Price Variance                │
│  tồn kho │  tồn kho │  YTD     │  (Actual vs Budget)           │
├──────────┴──────────┴──────────┴──────────────────────────────  │
│                                          │                       │
│  [Bar] Tồn kho theo nhà kho             │  [Bar] Top 10 NCC    │
│  (quantity_left × unit_price)           │  by procurement cost  │
│                                          │                       │
├──────────────────────────────────────────┴──────────────────────│
│  [Line] Nhập/Xuất kho theo tháng (Inbound vs Outbound)         │
└─────────────────────────────────────────────────────────────────┘
```

---

## 7. Bảng mart — nguồn dữ liệu cho PBI

| Bảng mart | Grain | Số cột chính | Dùng cho báo cáo |
|-----------|-------|-------------|-----------------|
| `fct_revenue` | ngày × khách × chi nhánh | 20+ | Sales Overview, Trend |
| `fct_order_performance` | 1 đơn hàng | 15+ | Delivery KPI |
| `fct_order_items_detail` | 1 dòng sản phẩm | 20+ | Product drill-down |
| `dim_customer_segmentation` | 1 khách hàng | 15+ | RFM, Customer 360 |
| `fct_gross_profit` | ngày × khách | 15+ | Finance P&L |
| `fct_purchase_cost` | ngày × NCC × SP | 12+ | Procurement |
| `dim_customer_credit` | 1 khách hàng | 10+ | AR / Công nợ |
| `fct_stock_snapshot` | lot × kho × (SP\|NPL) | 25+ | Inventory snapshot, định giá tồn |
| `fct_inbound_outbound` | ngày × kho × (SP\|NPL) | 25+ | Kho nhập/xuất, trending |
| `dim_material_mart` | 1 NPL | 10+ | Lookup NPL, filter npl_type |
| `fct_order_npl_cost` | 1 đơn hàng | 12+ | NPL profitability |
| `fct_production_npl_cost` | 1 BOM dòng | 15+ | BOM deep dive |
| `fct_production_efficiency` | lệnh SX × ngày | 12+ | Production KPI |

---

## DAX Measures gợi ý

```dax
-- Tổng doanh thu
Total Revenue = SUM(fct_revenue[revenue])

-- Doanh thu năm trước (YoY)
Revenue PY = CALCULATE([Total Revenue], SAMEPERIODLASTYEAR(fct_revenue[order_date]))

-- YoY Growth %
Revenue YoY % = DIVIDE([Total Revenue] - [Revenue PY], [Revenue PY])

-- NPL % weighted average
NPL Pct = DIVIDE(SUM(fct_order_npl_cost[allocated_npl_vnd]),
                 SUM(fct_order_npl_cost[revenue_vnd]))

-- Tỉ lệ giao đủ hàng
Fulfilment Rate = DIVIDE(
    COUNTROWS(FILTER(fct_order_performance, fct_order_performance[is_fully_delivered] = TRUE())),
    COUNTROWS(fct_order_performance)
)

-- Average Order Value
AOV = DIVIDE([Total Revenue], DISTINCTCOUNT(fct_revenue[order_id]))

-- Tổng tồn kho số lượng (hiện tại)
Total Stock Qty = SUM(fct_stock_snapshot[quantity_left])

-- ── Giá trị tồn kho ────────────────────────────────────────────────────────

-- Giá trị tồn kho HIỆN TẠI (snapshot — không dùng để trend theo tháng)
-- Nguồn: fct_stock_snapshot. Ưu tiên: lot_price -> PO fallback.
-- NVL: 98%+ có giá. Sản phẩm: gần như không có giá trong ERP (ERP không ghi
-- giá lô cho thành phẩm sản xuất nội bộ — cost tracking ở BOM/NPL level).
Stock Value (Current) = CALCULATE(
    SUM(fct_stock_snapshot[stock_value]),
    fct_stock_snapshot[is_valued] = TRUE()
)

-- Giá trị tồn kho NPL theo tháng (historical trend)
-- Nguồn: fct_stock_monthly_snapshot. Dùng cho line chart trend 39 tháng.
-- QUAN TRỌNG: chỉ NVL có giá (is_valued=TRUE ~64-83% rows).
--             Sản phẩm trả về NULL — ERP không ghi giá lô cho thành phẩm SX.
Stock Value NVL Monthly = CALCULATE(
    SUM(fct_stock_monthly_snapshot[est_value_end_month]),
    fct_stock_monthly_snapshot[is_valued] = TRUE(),
    fct_stock_monthly_snapshot[item_type] = "nvl"
)

-- Tồn kho NPL theo tháng (số lượng, không cần is_valued)
NVL Stock Qty Monthly = CALCULATE(
    SUM(fct_stock_monthly_snapshot[est_qty_end_month]),
    fct_stock_monthly_snapshot[item_type] = "nvl"
)

-- Tồn kho sản phẩm theo tháng (số lượng)
Product Stock Qty Monthly = CALCULATE(
    SUM(fct_stock_monthly_snapshot[est_qty_end_month]),
    fct_stock_monthly_snapshot[item_type] = "product"
)

-- Tỉ lệ lot được định giá (hiện tại)
Valued Lot Pct =
    DIVIDE(
        COUNTROWS(FILTER(fct_stock_snapshot, fct_stock_snapshot[is_valued])),
        COUNTROWS(fct_stock_snapshot)
    )

-- Inbound tháng hiện tại
Inbound MTD = CALCULATE(
    SUM(fct_inbound_outbound[quantity_in]),
    fct_inbound_outbound[movement_type] = "INBOUND",
    DATESMTD(fct_inbound_outbound[movement_date])
)

-- Outbound tháng hiện tại
Outbound MTD = CALCULATE(
    SUM(fct_inbound_outbound[quantity_out]),
    fct_inbound_outbound[movement_type] = "OUTBOUND",
    DATESMTD(fct_inbound_outbound[movement_date])
)

-- ── Phân tích hiệu quả tồn kho (Advanced) ──────────────────────────

-- Tồn kho bình quân (Average Inventory Qty - Monthly)
Avg Inventory Qty = 
AVERAGEX(
    VALUES('fct_stock_monthly_snapshot'[month_end]),
    CALCULATE(SUM('fct_stock_monthly_snapshot'[est_qty_end_month]))
)

-- Vòng quay tồn kho (Inventory Turnover - Unit Based)
Inventory Turnover = 
DIVIDE(
    SUM('fct_inbound_outbound'[quantity_out]),
    [Avg Inventory Qty],
    0
)

-- Số ngày tồn kho (Days On Hand - DOH)
Days On Hand (DOH) = 
VAR DaysInPeriod = COUNTROWS('dim_date') 
RETURN
DIVIDE(
    [Avg Inventory Qty] * DaysInPeriod,
    SUM('fct_inbound_outbound'[quantity_out]),
    0
)
```

---

*Tài liệu kèm theo: [architecture_overview.md](architecture_overview.md) · [bus_matrix.md](bus_matrix.md) · [domain_sales.md](domain_sales.md) · [domain_finance.md](domain_finance.md)*
