1. SOURCE
   - ERP: FOSO
   - EXCEL
   - DATA SIZE: 4GB
   - WEEKLY BATCH
2. Data Flow
   ERP -> MySQL -> BigQuery -> Staging -> Intermediate -> Mart -> BI Tools.
3. Modeling approach
  - Star schema
  - Conformed dimensions
  - Bus matrix
4. Scope phase 1
  - Sales domain only
  - Shared dimensions
