import importlib.util
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]


def load_module(relative_path, module_name):
    module_path = REPO_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_metric_maker_uses_safe_reference_parsing():
    metric_maker = load_module(
        "Great_Lakes_HPC/pys/metric_maker.py", "metric_maker_test"
    )

    assert metric_maker.conspiracy_labler("['conspiracy']") == 1
    assert metric_maker.neutral_labler("['python']") == 1
    with pytest.raises(ValueError):
        metric_maker.parse_ref_list("__import__('os').system('echo unsafe')")


def test_quarterly_metric_maker_uses_safe_reference_parsing():
    quarterly_metric_maker = load_module(
        "Great_Lakes_HPC/pys/quarterly_metric_maker.py",
        "quarterly_metric_maker_test",
    )

    assert quarterly_metric_maker.political_labler("['antiwork']") == 1
    assert quarterly_metric_maker.parse_ref_list(None) == []
    with pytest.raises(ValueError):
        quarterly_metric_maker.parse_ref_list("{'unexpected': 'shape'}")
