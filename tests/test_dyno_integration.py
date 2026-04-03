"""Dyno-lab integration tests for pushshift_python.

Uses ``dyno_lab.module.load_module_by_path`` in place of the custom
``load_module`` helper defined in test_hpc_metric_scripts.py, and exercises
the main package via ``dyno_lab.fs.TempWorkdir`` for file-based operations.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from dyno_lab.fs import TempWorkdir
from dyno_lab.module import load_module_by_path

REPO_ROOT = Path(__file__).resolve().parents[1]


# ── fixtures ──────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def metric_maker():
    return load_module_by_path(
        "Great_Lakes_HPC/pys/metric_maker.py",
        "metric_maker_dyno",
        repo_root=REPO_ROOT,
    )


@pytest.fixture(scope="module")
def quarterly_metric_maker():
    return load_module_by_path(
        "Great_Lakes_HPC/pys/quarterly_metric_maker.py",
        "quarterly_metric_maker_dyno",
        repo_root=REPO_ROOT,
    )


# ── HPC metric_maker tests ────────────────────────────────────────────────


def test_metric_maker_conspiracy_label(metric_maker):
    assert metric_maker.conspiracy_labler("['conspiracy']") == 1


def test_metric_maker_neutral_label(metric_maker):
    assert metric_maker.neutral_labler("['python']") == 1


def test_metric_maker_rejects_unsafe_reference(metric_maker):
    with pytest.raises(ValueError):
        metric_maker.parse_ref_list("__import__('os').system('echo unsafe')")


# ── HPC quarterly_metric_maker tests ─────────────────────────────────────


def test_quarterly_political_label(quarterly_metric_maker):
    assert quarterly_metric_maker.political_labler("['antiwork']") == 1


def test_quarterly_parse_ref_list_none(quarterly_metric_maker):
    assert quarterly_metric_maker.parse_ref_list(None) == []


def test_quarterly_rejects_dict_shape(quarterly_metric_maker):
    with pytest.raises(ValueError):
        quarterly_metric_maker.parse_ref_list("{'unexpected': 'shape'}")


# ── file_handler tests using TempWorkdir ──────────────────────────────────


def test_file_handler_combine_merges_two_csvs():
    from _pushshift_file_handler import file_handler

    with TempWorkdir() as wd:
        wd.write("a.csv", "col1,col2\n1,2\n3,4\n")
        wd.write("b.csv", "col1,col2\n5,6\n7,8\n")
        out = str(wd.path / "combined.csv")
        handler = file_handler()
        handler.combine(
            headers=["col1", "col2"],
            files=[str(wd.path / "a.csv"), str(wd.path / "b.csv")],
            path_out=out,
        )
        lines = [ln for ln in (wd.path / "combined.csv").read_text().splitlines() if ln]
        assert lines[0] == "col1,col2"
        assert handler.written == 4
        assert handler.errors == 0


def test_file_handler_combine_single_file_written_count():
    from _pushshift_file_handler import file_handler

    with TempWorkdir() as wd:
        wd.write("only.csv", "x,y\n10,20\n30,40\n")
        out = str(wd.path / "out.csv")
        handler = file_handler()
        handler.combine(
            headers=["x", "y"],
            files=[str(wd.path / "only.csv")],
            path_out=out,
        )
        assert handler.written == 2
        assert handler.errors == 0


def test_file_handler_combine_no_explicit_headers():
    from _pushshift_file_handler import file_handler

    with TempWorkdir() as wd:
        wd.write("data.csv", "a,b\n1,2\n")
        out = str(wd.path / "result.csv")
        handler = file_handler()
        handler.combine(
            files=[str(wd.path / "data.csv")],
            path_out=out,
        )
        content = (wd.path / "result.csv").read_text()
        assert "1,2" in content
