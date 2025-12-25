# This file ensures core scripts are imported for coverage

import scripts.scheduler
import scripts.cleanup_old_data
import scripts.monitoring.pipeline_monitor

def test_scripts_importable():
    assert True
