CREATE SCHEMA IF NOT EXISTS analytic;

CREATE TABLE IF NOT EXISTS analytic.fact_merchant_kpi (
  date date,
  id_merchant varchar(50),
  tpv float,
  total_transactions float
);

CREATE TABLE IF NOT EXISTS analytic.fact_customer_kpi (
  date date,
  id_customer varchar(50),
  tpv float,
  total_transactions float
);

CREATE TABLE IF NOT EXISTS analytic.dim_merchant_category (
  id_merchant varchar(50),
  category varchar(50)
);
