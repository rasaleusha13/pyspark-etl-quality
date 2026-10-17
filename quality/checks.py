"""Data-quality check definitions.

Each check is a spec: a name, the column(s) it inspects, a Spark SQL predicate
that flags BAD rows, and a severity. The pipeline in etl_pipeline.py executes
these against a DataFrame and produces a report.

Pure-Python helpers (summarize, report formatting) are unit-tested in
tests/test_checks.py without needing Spark.
"""

CHECKS = [
    {
        "name": "order_id_present",
        "description": "order_id must be present",
        "bad_row_predicate": "order_id IS NULL",
        "severity": "error",
    },
    {
        "name": "order_date_present_and_valid",
        "description": "order_date must be present and a real calendar date",
        "bad_row_predicate": "order_date IS NULL OR to_date(order_date, 'yyyy-MM-dd') IS NULL",
        "severity": "error",
    },
    {
        "name": "quantity_positive",
        "description": "quantity must be a positive integer",
        "bad_row_predicate": "quantity IS NULL OR quantity <= 0",
        "severity": "error",
    },
    {
        "name": "unit_price_present_and_positive",
        "description": "unit_price must be present and positive",
        "bad_row_predicate": "unit_price IS NULL OR unit_price <= 0",
        "severity": "error",
    },
    {
        "name": "status_allowed_values",
        "description": "status must be one of shipped/pending/cancelled",
        "bad_row_predicate": "status NOT IN ('shipped', 'pending', 'cancelled')",
        "severity": "error",
    },
    {
        "name": "region_present",
        "description": "region should be present (warning only)",
        "bad_row_predicate": "region IS NULL OR trim(region) = ''",
        "severity": "warning",
    },
]

# Table-level check run separately (needs aggregation, not a row predicate)
DUPLICATE_CHECK = {
    "name": "order_id_unique",
    "description": "order_id must be unique across the dataset",
    "severity": "error",
}


def summarize(results):
    """Pure-Python summary of per-check violation counts.

    results: list of {"name":..., "severity":..., "violations": int, "total": int}
    Returns a dict with totals and an overall pass/fail verdict.
    """
    errors = sum(r["violations"] for r in results if r["severity"] == "error")
    warnings = sum(r["violations"] for r in results if r["severity"] == "warning")
    total = results[0]["total"] if results else 0
    return {
        "total_rows": total,
        "error_violations": errors,
        "warning_violations": warnings,
        "passed": errors == 0,
        "checks_run": len(results),
    }


def format_report(results, summary):
    """Render a human-readable quality report."""
    lines = ["DATA QUALITY REPORT", "=" * 40]
    lines.append(f"Rows scanned : {summary['total_rows']}")
    lines.append(f"Checks run   : {summary['checks_run']}")
    lines.append(f"Errors       : {summary['error_violations']}")
    lines.append(f"Warnings     : {summary['warning_violations']}")
    lines.append(f"Verdict      : {'PASS' if summary['passed'] else 'FAIL'}")
    lines.append("")
    for r in results:
        flag = "FAIL" if r["violations"] else "ok"
        lines.append(
            f"[{flag}] {r['name']} ({r['severity']}): "
            f"{r['violations']} violating rows"
        )
    return "\n".join(lines)
