import ast
from pathlib import Path

import pytest

DAG_PATH = Path(__file__).parent.parent / "airflow_dags" / "transfer_pipeline_dag.py"


def test_dag_file_is_valid_python():
    ast.parse(DAG_PATH.read_text())


def test_dag_has_expected_tasks_and_order(monkeypatch):
    airflow_models = pytest.importorskip("airflow.models")
    monkeypatch.setenv("INGESTKIT_PSEUDONYM_SALT", "salt-for-dag-test")
    dag_bag = airflow_models.DagBag(dag_folder=str(DAG_PATH))

    assert dag_bag.import_errors == {}
    transfer_dag = dag_bag.dags["ingestkit_transfer_pipeline"]
    assert set(transfer_dag.task_ids) == {"create_run", "list_batches", "transfer_batch", "complete_run"}
    assert transfer_dag.get_task("list_batches").downstream_task_ids == {"transfer_batch"}
    assert transfer_dag.get_task("complete_run").trigger_rule == "all_done"
