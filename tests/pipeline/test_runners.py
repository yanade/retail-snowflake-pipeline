"""Unit tests for pipeline/runners.py, with shell scripts standing in for dbt and DVT."""

from pathlib import Path

import pytest

from pipeline.runners import run_dbt_build, run_dvt


def _fake_tool(folder: Path, exit_code: int) -> Path:
    """An executable that records its working folder and arguments, then exits with exit_code."""
    tool = folder / "tool"
    tool.write_text(f'#!/bin/sh\necho "$(pwd -P) $@" > "{folder}/called.txt"\nexit {exit_code}\n')
    tool.chmod(0o755)
    return tool


def test_dbt_build_runs_in_the_project_and_returns_dbts_exit_code(tmp_path: Path):
    project = tmp_path / "dbt"
    (project / "target").mkdir(parents=True)

    exit_code = run_dbt_build(_fake_tool(tmp_path, 1), project_dir=project)

    assert exit_code == 1  # a failed test is data, not an exception
    assert (tmp_path / "called.txt").read_text().strip() == f"{project.resolve()} build"


def test_dbt_build_removes_the_previous_run_results_first(tmp_path: Path):
    project = tmp_path / "dbt"
    stale = project / "target" / "run_results.json"
    stale.parent.mkdir(parents=True)
    stale.write_text("{}")

    run_dbt_build(_fake_tool(tmp_path, 0), project_dir=project)

    assert not stale.exists()  # the fake wrote none, so a stale file would have survived


def test_dvt_removes_only_this_runs_results_and_passes_window_and_run(tmp_path: Path):
    results = tmp_path / "results"
    results.mkdir()
    (results / "run-1.json").write_text("[]")
    (results / "run-0.json").write_text("[]")

    exit_code = run_dvt(_fake_tool(tmp_path, 0), "2026-10-13 20:05:18", "run-1",
                        results_dir=results, project_root=tmp_path)

    assert exit_code == 0
    assert not (results / "run-1.json").exists()
    assert (results / "run-0.json").exists()  # other runs' files are history
    called = (tmp_path / "called.txt").read_text()
    assert "--window-end 2026-10-13 20:05:18 --run-id run-1" in called


def test_a_missing_executable_raises_instead_of_returning_a_code(tmp_path: Path):
    with pytest.raises(FileNotFoundError):  # a wrong venv path is a crash, not a tool's verdict
        run_dbt_build(tmp_path / "no-such-dbt", project_dir=tmp_path)