## Databricks Data Transformation Pipeline

- **End-to-end medallion pipeline (bronze → silver → gold)** built on 1M+ synthetic e-commerce orders using Python, PySpark, Spark SQL and SQL
  - Cleaned messy raw data: deduplication with window functions, standardization, and a quarantine table for invalid records
  - Built business-ready aggregates with CTEs, `LAG`/running totals, `QUALIFY` and `NTILE` for RFM customer segmentation

- **Performance optimization with measurable results**
  - Compared Python UDFs, pandas UDFs and built-in functions, and read execution plans to compare broadcast vs sort-merge joins
  - Diagnosed data skew and applied salting; improved Delta file layout with `OPTIMIZE` and liquid clustering

- **Reporting-ready data model and semantic layer:** star schema with PK/FK constraints, documented tables, and governed KPI definitions through Unity Catalog metric views

- **Production practices:** incremental upserts with Delta `MERGE`, time travel, and automated data-quality assertions

- ## Data model (star schema)

```mermaid
erDiagram
  silver_customers ||--o{ silver_orders : places
  silver_products ||--o{ silver_orders : contains
  dim_date ||--o{ silver_orders : "ordered on"
  silver_customers ||--o| gold_customer_rfm : scored_as
  silver_products ||--o{ gold_monthly_category_revenue : "rolls up by category"

  silver_orders {
    long order_id PK
    long customer_id FK
    long product_id FK
    date order_date FK
    int quantity
    double unit_price
    double discount
    string status
    double gross_amount
    double net_amount
  }
  silver_customers {
    long customer_id PK
    string full_name
    string email
    string email_domain
    string country
    date signup_date
  }
  silver_products {
    long product_id PK
    string product_name
    string category
    double base_price
  }
  dim_date {
    date order_date PK
    int year
    int month
    string month_name
    int quarter
    boolean is_weekend
  }
  gold_customer_rfm {
    long customer_id PK
    int recency_days
    long frequency
    double monetary
    int r_score
    int f_score
    int m_score
    string segment
  }
  gold_monthly_category_revenue {
    date month
    string category
    double revenue
    long orders
    double running_revenue
    double mom_growth
  }
```

## Semantic layer flow

```mermaid
flowchart TD
    B["Bronze<br/>Raw, as ingested"] --> S["Silver<br/>Cleaned, conformed"]
    S --> G["Gold<br/>Star schema, rollups"]
    G --> SL

    subgraph SL["Semantic layer"]
        direction LR
        T["Certified tables<br/>PK/FK, comments, tags"]
        M["Metric views<br/>Reusable KPIs"]
        A["Access controls<br/>Row filters, masking"]
    end

    SL --> D["AI/BI dashboards<br/>Plus Genie"]
    SL --> E["External BI tools<br/>Power BI, Tableau"]
    SL --> Q["SQL and notebooks<br/>Analysts, data science"]
```
