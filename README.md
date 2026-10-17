# PySpark ETL Pipeline with Data-Quality Gates

A production-style ETL pipeline: ingest raw orders CSV → clean & transform with
PySpark → run data-quality checks → write clean Parquet + rejected rows + a JSON
quality report. The pipeline **fails loudly** (non-zero exit) when any
error-severity check finds violations — bad data never propagates silently.

## Pipeline

```
data/sample_orders.csv
        │ extract (Spark CSV reader)
        ▼
   transform: cast types, trim strings, parse dates, line_total = qty × price
        │ quality gates (6 row-level checks + duplicate check)
        ▼
   ┌────────────┐   ┌──────────────┐   ┌─────────────────────┐
   │ clean/     │   │ rejected/    │   │ quality_report.json │
   │ (parquet)  │   │ (parquet)    │   │                     │
   └────────────┘   └──────────────┘   └─────────────────────┘
```

## Quality checks

| Check | Severity |
|---|---|
| `order_id_present` | error |
| `order_date_present_and_valid` | error |
| `quantity_positive` | error |
| `unit_price_present_and_positive` | error |
| `status_allowed_values` (`shipped`/`pending`/`cancelled`) | error |
| `region_present` | warning |
| `order_id_unique` (table-level) | error |

The sample data intentionally contains violations (duplicates, bad dates, negative
quantities, invalid statuses) so you can watch the gate fail, inspect
`quality_report.json`, then fix the data and watch it pass.

## Run it

```bash
pip install -r requirements.txt
spark-submit etl_pipeline.py --input data/sample_orders.csv --output out/
```

Exit code `2` means the quality gate failed — check `out/quality_report.json`
and `out/rejected/` for details.

## Run the unit tests (no Spark needed)

```bash
python tests/test_checks.py
```

## Project layout

```
pyspark-etl-quality/
├── etl_pipeline.py        # extract → transform → quality gates → load
├── quality/
│   └── checks.py          # check specs + pure-Python report helpers
├── data/sample_orders.csv # sample input (with intentional quality issues)
└── tests/test_checks.py   # unit tests for the check helpers
```
