import pytest
import os
import sys

# Pytest will skip this entire file if airflow is not installed 
# (e.g., when running tests on the Windows host instead of WSL2)
pytest.importorskip("airflow")

from airflow.models import DagBag

def test_dagbag_import():
    """
    Verify that all DAGs can be parsed without syntax errors or missing imports.
    This test must be executed inside the WSL2 Airflow environment.
    """
    # Assuming tests are run from the project root in WSL2
    dag_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'airflow', 'dags')
    
    dagbag = DagBag(dag_folder=dag_dir, include_examples=False)
    
    assert len(dagbag.import_errors) == 0, f"DAG import failures: {dagbag.import_errors}"
    assert 'ecommerce_batch_pipeline' in dagbag.dags
    
    dag = dagbag.get_dag('ecommerce_batch_pipeline')
    assert dag is not None
    assert len(dag.tasks) > 0
