# Upstart13 Assessment – Kevin Jané

## Overview
End-to-end data pipeline built with **Azure Data Factory** and **Databricks** that ingests CSV data from a REST API and transforms it to deliver insights for business users.

---

## Architecture

| Layer | Tool |
|---|---|
| Ingestion and Orchestration | Azure Data Factory (Copy Data) & Databricks |
| Storage | Azure Data Lake Storage Gen2 |
| Transformation | Databricks (PySpark / Spark SQL) |
| Version Control | GitHub |

<img width="1270" height="702" alt="diagramtest" src="https://github.com/user-attachments/assets/b6877aff-de0b-48d5-9b00-3d5cb1709bf1" />

---

## Pipeline

The ADF pipeline runs in the following order:

1. **Ingestion** (parallel) — `ingestion_products`, `ingestion_order_detail`, `ingestion_order_header`
2. **Transformation** — `Store_Products`, `Store_OrderDetail` and `Store_OrderHeader` notebooks that enriches the raw data
3. **Publishing** (parallel) — `Publish_Products` and `Publish_Orders` notebooks expose the final tables

<img width="1200" height="693" alt="pipeline_medallion" src="https://github.com/user-attachments/assets/ab21a122-b31f-4d44-842c-949fdc39dcfb" />

---
## Datasets

### Raw

- [raw_order_details.csv](https://github.com/kevinDjane/upstart13-assesment-kj/blob/cb71aa714f0f24dc5f20401dcb3fa11cbb0984b9/output_datasets/raw_order_details.csv)
- [raw_order_headers.csv](https://github.com/kevinDjane/upstart13-assesment-kj/blob/cb71aa714f0f24dc5f20401dcb3fa11cbb0984b9/output_datasets/raw_order_headers.csv)
- [raw_products.csv](https://github.com/kevinDjane/upstart13-assesment-kj/blob/cb71aa714f0f24dc5f20401dcb3fa11cbb0984b9/output_datasets/raw_products.csv)

### Store

- [store_order_details.parquet](https://github.com/kevinDjane/upstart13-assesment-kj/blob/cb71aa714f0f24dc5f20401dcb3fa11cbb0984b9/output_datasets/store_order_details.parquet)
- [store_order_headers.parquet](https://github.com/kevinDjane/upstart13-assesment-kj/blob/cb71aa714f0f24dc5f20401dcb3fa11cbb0984b9/output_datasets/store_order_headers.parquet)
- [store_products.parquet](https://github.com/kevinDjane/upstart13-assesment-kj/blob/cb71aa714f0f24dc5f20401dcb3fa11cbb0984b9/output_datasets/store_products.parquet)

### Publish

- [publish_orders.parquet](https://github.com/kevinDjane/upstart13-assesment-kj/blob/cb71aa714f0f24dc5f20401dcb3fa11cbb0984b9/output_datasets/publish_orders.parquet)
- [publish_products.parquet](https://github.com/kevinDjane/upstart13-assesment-kj/blob/cb71aa714f0f24dc5f20401dcb3fa11cbb0984b9/output_datasets/publish_products.parquet)

---

## Analysis

### Which color generated the highest revenue each year?
```sql
WITH color_sales AS (
  SELECT
    date_format(orders.order_date, 'yyyy') AS order_year,
    products.color,
    SUM(
      orders.total_line_extended_price
      - (orders.order_qty * products.standard_cost)
    ) AS total_net_revenue
  FROM publish_orders AS orders
  INNER JOIN publish_products AS products
    ON orders.product_id = products.product_id
  GROUP BY 1, 2
)
SELECT
  order_year,
  color,
  total_net_revenue
FROM color_sales
QUALIFY RANK() OVER (PARTITION BY order_year ORDER BY total_net_revenue DESC) = 1
ORDER BY order_year DESC
```

**Results:**
| Year | Top Color | Total net revenue |
|------|-----------|-------------------|
| 2024 | Black | 1190442.120 |
| 2023 | Black | 1828919.457 |
| 2022 | Red | 950245.163 |
| 2021 | Red | 950245.163 |

---

### What is the average LeadTimeInBusinessDays by ProductCategoryName?
```sql
SELECT
  products.product_category_name,
  ROUND(AVG(orders.lead_time_in_business_days), 3) AS avg_lead_time_in_business_days
FROM publish_orders AS orders
INNER JOIN publish_products AS products
  ON orders.product_id = products.product_id
GROUP BY products.product_category_name
ORDER BY avg_lead_time_in_business_days DESC
```

**Results:**
| Category | Avg. Business Days |
|---|---|
| Others | 5.010 |
| Accessories | 5.007 |
| Bikes | 5.005 |
| Clothing | 5.005 |
| Components | 5.003 |

> All categories average approximately **5 business days**, indicating consistent delivery performance across the board.

---

## Notes

- `total_line_extended_price` is calculated as `order_qty * (unit_price - unit_price_discount)`
- `lead_time_in_business_days` excludes Saturdays and Sundays from the order-to-ship window
- Net revenue metric subtracts `standard_cost` to reflect true profitability per order line
