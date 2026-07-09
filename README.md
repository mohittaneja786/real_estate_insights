# real_estate_insights



# Synthetic Real Estate Dataset — Databricks Data Engineering Project

Fully synthetic data (no real listings/people) built for practicing a
Bronze → Silver → Gold Medallion pipeline with full load + ~2 months of
incremental (CDC) processing, a linked transactions fact table, and a
deliberately dirty dataset for cleansing practice.

## Folder structure

```
real_estate_synthetic_data/
├── full_load/
│   ├── agents_full_load.csv                    (3,000 rows  — dimension)
│   ├── locations_full_load.csv                 (250 rows    — dimension)
│   ├── properties_full_load_part1..5.csv        (100,000 rows each → 500,000 total)
│   └── sales_transactions_full_load_part1..2.csv (224,595 rows total)
├── incremental/                                  (60 daily batches, 2026-07-01 → 2026-08-29)
│   ├── properties_incremental_YYYY-MM-DD.csv     (one file per day, 60 files)
│   └── sales_transactions_incremental_YYYY-MM-DD.csv (one file per day that had new sales)
├── dirty_data_for_cleansing/
│   ├── properties_dirty.csv                     (~41,200 rows, issues injected)
│   ├── agents_dirty.csv                         (~824 rows, issues injected)
│   └── data_quality_issue_log.csv               (exact list of what was injected + counts)
├── batch_manifest.csv                            (control table, one row per incremental batch)
├── properties_full_snapshot.parquet              (same 500K properties, single Parquet file)
└── README.md
```

## Table: `properties` (full load, 500,000 rows)

| Column | Type | Notes |
|---|---|---|
| property_id | string | PK, e.g. PT0000001 |
| listing_id | string | unique listing reference |
| agent_id | string | FK -> agents |
| location_id | string | FK -> locations |
| property_type | string | Single Family, Condo, Townhouse, Multi-Family, Land, Apartment |
| bedrooms, bathrooms, square_feet, lot_size_sqft, year_built | numeric | |
| listing_price | float | USD |
| list_date | date | |
| status | string | Active, Pending, Sold, Withdrawn |
| sold_price, sold_date | | null unless Sold |
| days_on_market, hoa_fee, garage_spaces, has_pool, has_basement | | |
| created_at, updated_at | timestamp | |

`agents` and `locations` are static dimension tables (full load only).

## Table: `sales_transactions` (full load: 224,595 rows; +36,344 more via incremental)

One row per completed sale, linked to `properties.property_id`.

| Column | Notes |
|---|---|
| transaction_id | PK |
| property_id, listing_id | FK -> properties |
| seller_agent_id, buyer_agent_id | FK -> agents |
| buyer_name | synthetic |
| sale_price, sale_date, closing_date | |
| financing_type | Conventional Mortgage, Cash, FHA Loan, VA Loan, Jumbo Loan |
| down_payment_pct, commission_rate_pct, commission_amount | |
| title_company | |
| transaction_status | Completed / Cancelled (~3%) |

## Incremental / CDC files (60 days: 2026-07-01 -> 2026-08-29)

Each `properties_incremental_YYYY-MM-DD.csv` is one day's change-data-capture
extract, ~3,000-5,500 rows/day (~254,000 change events total), tagged with:

| Column | Meaning |
|---|---|
| operation_type | INSERT (new listing), UPDATE (price change or status -> Pending/Sold), DELETE (withdrawn, soft delete) |
| cdc_timestamp | exact change time |
| batch_date | the day's batch/load date |

Whenever an UPDATE moves a property to Sold, a matching
`sales_transactions_incremental_YYYY-MM-DD.csv` row is created the same day
(operation_type=INSERT only -- transactions aren't mutated after creation) --
36,344 new transactions across the 60 days.

`batch_manifest.csv` has one row per batch: file names, record counts, and
insert/update/delete/transaction counts -- use it as a control/audit table.

## `dirty_data_for_cleansing/` -- deliberate data quality issues

`properties_dirty.csv` (41,200 rows) and `agents_dirty.csv` (824 rows) are
sampled from the full load and then deliberately corrupted. `data_quality_issue_log.csv`
lists every issue type, the affected column, and the row count, so you can
validate your cleansing logic against ground truth. Issues injected:

- Nulls in random columns (bedrooms, bathrooms, year_built, hoa_fee, agent_id, listing_price, square_feet)
- Inconsistent categorical casing ("single family", "SFH", "Single-Family" vs "Single Family")
- Invalid/out-of-range values (negative bedrooms/prices, zero square_feet, year_built=2099, agent rating=7.5)
- Currency-formatted strings in a numeric column ("$450,000" instead of 450000)
- Inconsistent date formats (MM/DD/YYYY, DD-Mon-YYYY mixed with ISO dates)
- Extra leading/trailing whitespace in string fields
- Mixed boolean representations (Yes/Y/yes/1/True etc.)
- Orphaned foreign keys (agent_id/location_id that don't exist in the dimension tables)
- Malformed emails, inconsistent phone number formatting
- Exact duplicate rows, and same property_id with conflicting values (classic dedup case)

## How to load this into Databricks (Bronze -> Silver -> Gold)

Upload the whole `real_estate_synthetic_data/` folder to a Unity Catalog Volume
or ADLS/S3/DBFS path, e.g. `/Volumes/main/real_estate/raw/`, keeping the
`full_load/`, `incremental/`, and `dirty_data_for_cleansing/` sub-folders.

### 1. Bronze -- land the raw data as-is

```python
base = "/Volumes/main/real_estate/raw"

# Dimensions -- one-time full load
agents_df = spark.read.option("header", True).option("inferSchema", True) \
    .csv(f"{base}/full_load/agents_full_load.csv")
agents_df.write.mode("overwrite").saveAsTable("bronze.agents")

locations_df = spark.read.option("header", True).option("inferSchema", True) \
    .csv(f"{base}/full_load/locations_full_load.csv")
locations_df.write.mode("overwrite").saveAsTable("bronze.locations")

# Properties -- full load (all 5 part files load together as one folder read)
props_df = spark.read.option("header", True).option("inferSchema", True) \
    .csv(f"{base}/full_load/properties_full_load_part*.csv")
props_df.write.mode("overwrite").saveAsTable("bronze.properties_full")

txn_df = spark.read.option("header", True).option("inferSchema", True) \
    .csv(f"{base}/full_load/sales_transactions_full_load_part*.csv")
txn_df.write.mode("overwrite").saveAsTable("bronze.sales_transactions_full")

# Incremental -- use Auto Loader so new daily files are picked up automatically
(spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", True)
    .option("cloudFiles.schemaLocation", f"{base}/_schema/properties_cdc")
    .load(f"{base}/incremental/properties_incremental_*.csv")
    .withColumn("_source_file", F.input_file_name())
    .writeStream.format("delta")
    .option("checkpointLocation", f"{base}/_checkpoints/properties_cdc")
    .trigger(availableNow=True)
    .toTable("bronze.properties_cdc"))

# Same pattern for sales_transactions_incremental_*.csv -> bronze.sales_transactions_cdc
```

If you'd rather not deal with streaming yet, a simple batch loop works too:
`for f in sorted(files): spark.read.csv(f).write.mode("append").saveAsTable("bronze.properties_cdc")`.

### 2. Silver -- apply CDC with MERGE, and clean the dirty data

```python
from delta.tables import DeltaTable

# First land the full load into a Silver Delta table
spark.table("bronze.properties_full").write.mode("overwrite") \
    .format("delta").saveAsTable("silver.properties")

silver_tbl = DeltaTable.forName(spark, "silver.properties")
cdc_df = spark.table("bronze.properties_cdc")

(silver_tbl.alias("t")
    .merge(cdc_df.alias("s"), "t.property_id = s.property_id")
    .whenMatchedDelete(condition="s.operation_type = 'DELETE'")
    .whenMatchedUpdateAll(condition="s.operation_type = 'UPDATE'")
    .whenNotMatchedInsertAll(condition="s.operation_type = 'INSERT'")
    .execute())
```

For the dirty files, land them in Bronze as-is, then build a Silver cleansing
step: trim whitespace, standardize `property_type` casing, normalize
Yes/No/Y/N/1/0 booleans to a single boolean type, parse mixed date formats
with `coalesce(to_date(col,"yyyy-MM-dd"), to_date(col,"MM/dd/yyyy"), to_date(col,"dd-MMM-yyyy"))`,
strip `$`/`,` from price strings before casting to numeric, drop/quarantine
rows with orphaned foreign keys or impossible values (negative price,
year_built > current year), and de-duplicate on `property_id` (e.g. keep the
row with the latest `updated_at`). `data_quality_issue_log.csv` tells you
exactly what to check for and how many rows should be affected by each rule.

### 3. Gold -- business aggregates

```python
gold_price_trends = spark.sql("""
    SELECT l.city, l.state, p.property_type,
           date_trunc('month', p.sold_date) AS sold_month,
           percentile_approx(p.sold_price, 0.5) AS median_sold_price,
           count(*) AS homes_sold
    FROM silver.properties p
    JOIN silver.locations l ON p.location_id = l.location_id
    WHERE p.status = 'Sold'
    GROUP BY 1,2,3,4
""")
gold_price_trends.write.mode("overwrite").saveAsTable("gold.price_trends_by_location")

gold_agent_performance = spark.sql("""
    SELECT a.agent_id, a.first_name, a.last_name, a.agency_name,
           count(t.transaction_id) AS deals_closed,
           sum(t.commission_amount) AS total_commission,
           avg(t.sale_price) AS avg_sale_price
    FROM silver.sales_transactions t
    JOIN silver.agents a ON t.seller_agent_id = a.agent_id
    WHERE t.transaction_status = 'Completed'
    GROUP BY 1,2,3,4
""")
gold_agent_performance.write.mode("overwrite").saveAsTable("gold.agent_performance")
```

### 4. Extras worth practicing on this dataset

- `DESCRIBE HISTORY silver.properties` + Delta Time Travel to see how a
  property's status changed across batches.
- Enable Change Data Feed (`delta.enableChangeDataFeed = true`) on
  `silver.properties` and read it with `.option("readChangeFeed", "true")`.
- Turn the manual MERGE loop into a proper Structured Streaming
  `foreachBatch` job driven by Auto Loader on the `incremental/` folder, so it
  behaves like a real daily pipeline.
- Use `batch_manifest.csv` as a control table to build a simple orchestration/
  audit layer (e.g. a Databricks Job that only processes batches not yet
  marked complete).
- Add DLT (Delta Live Tables) expectations (`@dlt.expect_or_drop`) for the
  dirty-data rules instead of hand-written filters, as a second way to do the
  same cleansing.

## Notes
- All data is synthetic (Faker + NumPy) -- no real people, agents, or properties.
- Full load: 500,000 properties, 224,595 linked transactions.
- Incremental: 60 daily batches (~254K change events, ~36K new transactions).
- Dirty data: sampled subset only (41,200 + 824 rows) -- intended for cleansing
  logic practice, not as a replacement for the clean full load.
