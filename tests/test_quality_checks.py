import json
import os

def test_quality_report_exists():
    assert os.path.exists("data/processed/monitoring_report.json")

def test_quality_score_present():
    with open("data/processed/monitoring_report.json") as f:
        report = json.load(f)
    assert "overall_health_score" in report
