-- ============================================
-- BƯỚC 3: DIM_DATE
-- ============================================
CREATE TABLE core.dim_date (
    date_key        INT PRIMARY KEY,
    full_date       DATE         NOT NULL,
    day_of_week     SMALLINT,
    day_name        VARCHAR(20),
    day_of_month    SMALLINT,
    day_of_year     SMALLINT,
    week_of_year    SMALLINT,
    month_num       SMALLINT,
    month_name      VARCHAR(20),
    quarter         SMALLINT,
    year            SMALLINT,
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN      DEFAULT FALSE
);

-- Generate 2020 → 2030
INSERT INTO core.dim_date (
    date_key, full_date,
    day_of_week, day_name,
    day_of_month, day_of_year,
    week_of_year, month_num, month_name,
    quarter, year, is_weekend
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT         AS date_key,
    d                                    AS full_date,
    EXTRACT(DOW     FROM d)::SMALLINT    AS day_of_week,
    TRIM(TO_CHAR(d, 'Day'))              AS day_name,
    EXTRACT(DAY     FROM d)::SMALLINT    AS day_of_month,
    EXTRACT(DOY     FROM d)::SMALLINT    AS day_of_year,
    EXTRACT(WEEK    FROM d)::SMALLINT    AS week_of_year,
    EXTRACT(MONTH   FROM d)::SMALLINT    AS month_num,
    TRIM(TO_CHAR(d, 'Month'))            AS month_name,
    EXTRACT(QUARTER FROM d)::SMALLINT    AS quarter,
    EXTRACT(YEAR    FROM d)::SMALLINT    AS year,
    EXTRACT(DOW     FROM d) IN (0, 6)    AS is_weekend
FROM generate_series(
    '2020-01-01'::DATE,
    '2030-12-31'::DATE,
    '1 day'::INTERVAL
) AS d;
