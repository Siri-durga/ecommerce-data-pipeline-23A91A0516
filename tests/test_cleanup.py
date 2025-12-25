import os
from scripts.cleanup_old_data import should_preserve

def test_should_preserve_summary_files():
    assert should_preserve("daily_summary.csv") is True
    assert should_preserve("pipeline_report.json") is True

def test_should_not_preserve_random_file():
    assert should_preserve("old_data.csv") is False
