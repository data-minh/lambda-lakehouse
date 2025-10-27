-- dim_date
CREATE TABLE IF NOT EXISTS nessie.curated.dim_date (
  date_sk         INT,
  date            DATE,
  year            INT,
  quarter         INT,
  month           INT,
  day             INT,
  week            INT,
  dow             INT,
  is_weekend      BOOLEAN
);

-- dim_exchange
CREATE TABLE IF NOT EXISTS nessie.curated.dim_exchange (
  exchange_sk     BIGINT,
  exchange_code   STRING,
  country         STRING,
  timezone        STRING
);

-- dim_currency
CREATE TABLE lakehouse.curated.dim_currency (
   currency_sk bigint,
   currency_code varchar,
   currency_name varchar,
   exchange_rate_to_vnd bigint
);

-- dim_trading_status (VN)
CREATE TABLE IF NOT EXISTS nessie.curated.dim_trading_status (
  trading_status_sk BIGINT,
  status_code       STRING,
  status_group      STRING,
  status_desc       STRING
);

-- dim_company (SCD2)
CREATE TABLE IF NOT EXISTS nessie.curated.dim_company (
  company_sk       BIGINT,
  symbol           STRING,
  country          STRING,
  exchange_code    STRING,
  company_name     STRING,
  company_name_jp  STRING,
  sector           STRING,
  industry         STRING,
  website          STRING,
  employees        BIGINT,
  effective_from   DATE,
  effective_to     DATE,
  is_current       BOOLEAN
);

CREATE TABLE IF NOT EXISTS nessie.curated.fact_stock_daily (
  -- Foreign keys
  date_sk               INT                NOT NULL,   -- yyyymmdd
  company_sk            BIGINT,
  exchange_sk           BIGINT,
  currency_sk           BIGINT,
  trading_status_sk     BIGINT,
  -- Giá & biến động
  current_price         DOUBLE,
  previous_close        DOUBLE,
  ref_price             DOUBLE,
  pct_change            DOUBLE,                         -- (current - previous) / previous
  ceiling               DOUBLE,                         -- VN-only
  floor                 DOUBLE,                         -- VN-only
  is_limit_up           BOOLEAN,                        -- VN-only
  is_limit_down         BOOLEAN,                        -- VN-only
  delta_in_week         DOUBLE,                         -- VN-only (nếu nguồn có)
  delta_in_month        DOUBLE,                         -- VN-only
  delta_in_year         DOUBLE,                         -- VN-only
  avg_match_vol_2w      BIGINT,                         -- VN-only proxy thanh khoản
  -- Snapshot công ty
  market_cap            DOUBLE,                         -- cùng đơn vị tiền tệ của 'currency'
  employees             BIGINT,
  outstanding_share     BIGINT,
  issue_share           BIGINT,
  foreign_percent       DOUBLE
)
USING iceberg
PARTITIONED BY (date_sk);
