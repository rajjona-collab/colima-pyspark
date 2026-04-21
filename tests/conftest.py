"""
Pytest configuration and fixtures for colima-pyspark tests.
"""

import sys
import os
from unittest.mock import MagicMock

# Mock PySpark modules before any imports
sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()
sys.modules['pyspark.sql.functions'] = MagicMock()
sys.modules['pyspark.sql.types'] = MagicMock()
sys.modules['requests'] = MagicMock()


def pytest_configure(config):
    """Pytest hook: configure test run."""
    # Add project root to path
    project_root = os.path.join(os.path.dirname(__file__), '..')
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
