"""PySpark ETL pipeline with data-quality gates.

Reads raw orders CSV -> cleans & transforms -> runs quality checks ->
writes clean Parquet + rejected rows + a JSON quality report.

Fail-loud behavior: if any ERROR-severity check finds violations, the pipeline
exits non-zero AFTER writing the rejected rows and the report, so nothing bad
propagates silently downstream.

Run:
    spark-submit etl_pipeline.py --input data/sample_orders.csv --output out/
"""

import argparse
import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from quality.checks import CHECKS, DUPLICATE_CHECK, format_report, summarize


def build_spark(app_name="orders-etl-quality"):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def extract(spark, input_path):
    return spark.read.option("header", "true").csv(input_path)


def transform(df):
    """Clean and enrich: trim strings, parse dates, compute line totals."""
    df = df.withColumn("order_id", F.col("order_id").cast("int"))
    df = df.withColumn("quantity", F.col("quantity").cast("int"))
    df = df.withColumn("unit_price", F.col("unit_price").cast("double"))
    for c in ("product", "status", "region"):
        df = df.withColumn(c, F.trim(F.col(c)))
    df = df.withColumn("order_date_parsed", F.to_date("order_date", "yyyy-MM-dd"))
    df = df.withColumn("line_total", F.col("quantity") * F.col("unit_price"))
    return df


def run_quality_checks(df):
    """Run row-level checks + duplicate check. Returns (results, bad_ids)."""
    total = df.count()
    results = []
    bad_ids = set()

    for check in CHECKS:
        viol = df.filter(check["bad_row_predicate"])
        n = viol.count()
        results.append(
            {
                "name": check["name"],
                "description": check["description"],
                "severity": check["severity"],
                "violations": n,
                "total": total,
            }
        )
        if n:
            bad_ids.update(
                r["order_id"]
                for r in viol.select("order_id").collect()
                if r["order_id"] is not None
            )

    # table-level duplicate check
    dupes = (
        df.groupBy("order_id")
        .count()
        .filter("count > 1")
        .select("order_id")
        .collect()
    )
    dup_ids = [r["order_id"] for r in dupes]
    results.append(
        {
            "name": DUPLICATE_CHECK["name"],
            "description": DUPLICATE_CHECK["description"],
            "severity": DUPLICATE_CHECK["severity"],
            "violations": len(dup_ids),
            "total": total,
        }
    )
    bad_ids.update(dup_ids)

    return results, bad_ids


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = build_spark()
    try:
        raw = extract(spark, args.input)
        df = transform(raw)

        results, bad_ids = run_quality_checks(df)
        summary = summarize(results)
        report = format_report(results, summary)
        print("\n" + report + "\n")

        os.makedirs(args.output, exist_ok=True)
        with open(os.path.join(args.output, "quality_report.json"), "w") as f:
            json.dump({"summary": summary, "checks": results}, f, indent=2)

        clean = df.filter(~F.col("order_id").isin(list(bad_ids)))
        rejected = df.filter(F.col("order_id").isin(list(bad_ids)))
        clean.write.mode("overwrite").parquet(os.path.join(args.output, "clean"))
        rejected.write.mode("overwrite").parquet(os.path.join(args.output, "rejected"))
        print(f"Wrote {clean.count()} clean rows, {rejected.count()} rejected rows.")

        if not summary["passed"]:
            print("QUALITY GATE FAILED — see quality_report.json", file=sys.stderr)
            sys.exit(2)
        print("QUALITY GATE PASSED")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
