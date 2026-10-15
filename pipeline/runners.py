"""Run dbt and DVT as subprocesses and return their exit codes; the outcome mappers judge them (ADR-029)."""

import logging
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent  # the repo on the laptop, /opt/airflow/project in Airflow
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt"
DVT_RESULTS_DIR = PROJECT_ROOT / "validation" / "results"
DVT_MODULE = "validation.run_validations"

logger = logging.getLogger(__name__)


def run_dbt_build(dbt: Path, project_dir: Path = DBT_PROJECT_DIR) -> int:
    """
    Run dbt build after removing the previous run_results.json.

    Args:
        dbt: The dbt executable in venv-dbt.
        project_dir: Folder with dbt_project.yml and profiles.yml.

    Returns:
        dbt's exit code, for dbt_outcome with target/run_results.json.
    """
    (project_dir / "target" / "run_results.json").unlink(missing_ok=True)  # a crash must not leave the last run's file
    return _run([str(dbt), "build"], cwd=project_dir)


def run_dvt(python: Path, window_end: str, run_id: str,
            results_dir: Path = DVT_RESULTS_DIR, project_root: Path = PROJECT_ROOT) -> int:
    """
    Run the DVT suite for one window after removing this run's previous results file.

    Args:
        python: The interpreter in venv-dvt.
        window_end: UTC end of the load window, as run_validations parses it.
        run_id: The Airflow run, written into every result row.
        results_dir: Folder run_validations writes <run_id>.json to.
        project_root: Folder the validation package is imported from.

    Returns:
        run_validations' exit code, for dvt_outcome with the results file.
    """
    (results_dir / f"{run_id}.json").unlink(missing_ok=True)  # ADR-026: a crashed retry must not read the last attempt
    command = [str(python), "-m", DVT_MODULE, "--window-end", window_end, "--run-id", run_id]
    return _run(command, cwd=project_root)


def _run(command: list[str], cwd: Path) -> int:
    """
    Run a command, log its combined output, and return its exit code.

    Args:
        command: Executable and arguments.
        cwd: Working directory.

    Returns:
        The exit code; a missing executable raises FileNotFoundError instead.
    """
    logger.info("running %s in %s", " ".join(command), cwd)
    completed = subprocess.run(
        command, cwd=cwd, check=False,  # a non-zero exit is data for the outcome mapper
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    logger.info("exit code %d, output:\n%s", completed.returncode, completed.stdout)
    return completed.returncode