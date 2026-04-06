# Pipeline Architecture

```mermaid
flowchart TD
    A["MySQL ERP Source\norders, products, customers, warehouse..."] --> B["PostgreSQL staging schema\nRaw mirror tables\n1:1 copy from source\nUsed for audit, reload, incremental load"]
    B --> C["Python transform_core\nUpsert dimensions and facts\nAssign surrogate keys\nBuild warehouse foundation"]
    C --> D["PostgreSQL core schema\nBase star schema\ndim_* and fact_*\nReusable integrated layer"]
    D --> E["dbt staging models\nstg_*\nRename columns, cast types\nStandardize business flags"]
    E --> F["dbt intermediate models\nint_*\nReusable joins and shared logic\nExample: orders enriched with customer and date"]
    F --> G["dbt mart models\nBusiness-ready tables\nKPI, finance, inventory, sales"]
    G --> H["Power BI / Analysis / Reports"]

    I["Python ELT\nextractor + loader + watermark"] --> B
    J["dbt tests + docs + lineage"] -. applies to .-> E
    J -. applies to .-> F
    J -. applies to .-> G

    classDef source fill:#dfe8f7,stroke:#3b5b8a,color:#111;
    classDef raw fill:#f6ead8,stroke:#9a6a2f,color:#111;
    classDef core fill:#dff0df,stroke:#3a7a3a,color:#111;
    classDef dbt fill:#fff6cc,stroke:#9b8700,color:#111;
    classDef output fill:#f7dbe7,stroke:#9a3b6a,color:#111;

    class A source;
    class B,I raw;
    class C,D core;
    class E,F,G,J dbt;
    class H output;
```

## Ý nghĩa từng lớp

- `PostgreSQL.staging`: raw mirror từ ERP, phục vụ ingest, audit, reload và incremental load.
- `PostgreSQL.core`: lớp warehouse nền tảng, chuẩn hóa thành dimension/fact để tái sử dụng.
- `dbt stg_*`: semantic cleanup trên top của `core`, không phải raw staging của database.
- `dbt int_*`: gom các join và logic dùng chung để mart không lặp code.
- `dbt mart`: bảng sẵn sàng cho KPI, dashboard và phân tích nghiệp vụ.

## Thứ tự chạy

```mermaid
sequenceDiagram
    participant U as 👤 User
    participant P as pipeline.py
    participant E as extractor.py
    participant L as loader.py
    participant T as transform_core.py
    participant D as dbt

    U->>P: python pipeline.py --stage all
    loop 25 tables
        P->>E: extract_table(table, watermark)
        E-->>P: DataFrame (new rows only)
        P->>L: load_table(df, staging_table)
        L-->>P: COPY done
        P->>P: set_watermark(table, max_val)
    end
    P->>T: run_transforms()
    loop 21 steps
        T->>T: UPSERT dim / INSERT fact
    end
    T-->>P: done
    P-->>U: Pipeline finished in ~2min

    U->>D: dbt run
    loop 20 models
        D->>D: CREATE TABLE mart.*
    end
    D-->>U: PASS=20 WARN=0 ERROR=0
```
