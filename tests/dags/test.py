from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
with DAG(
    "etl_pruebas",
    start_date=days_ago(1),
    schedule_interval=None,
) as dag:
    task_a = DummyOperator(task_id="task_a")
    task_b = DummyOperator(task_id="task_b")
    task_a >> [task_b]
