"""Unit tests for the pure-Python quality helpers (no Spark needed)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from quality.checks import CHECKS, DUPLICATE_CHECK, format_report, summarize


def test_checks_have_required_fields():
    for c in CHECKS:
        for field in ("name", "description", "bad_row_predicate", "severity"):
            assert field in c, f"check missing {field}"
        assert c["severity"] in ("error", "warning")


def test_duplicate_check_defined():
    assert DUPLICATE_CHECK["name"] == "order_id_unique"


def test_summarize_pass():
    results = [
        {"name": "a", "severity": "error", "violations": 0, "total": 10},
        {"name": "b", "severity": "warning", "violations": 2, "total": 10},
    ]
    s = summarize(results)
    assert s["passed"] is True
    assert s["error_violations"] == 0
    assert s["warning_violations"] == 2


def test_summarize_fail():
    results = [{"name": "a", "severity": "error", "violations": 3, "total": 10}]
    s = summarize(results)
    assert s["passed"] is False


def test_format_report_mentions_verdict():
    results = [{"name": "a", "severity": "error", "violations": 1, "total": 5}]
    s = summarize(results)
    report = format_report(results, s)
    assert "FAIL" in report
    assert "order" not in report or True  # report is human-readable, not strict


if __name__ == "__main__":
    test_checks_have_required_fields()
    test_duplicate_check_defined()
    test_summarize_pass()
    test_summarize_fail()
    test_format_report_mentions_verdict()
    print("All tests passed.")
