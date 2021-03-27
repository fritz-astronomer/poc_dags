from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from jinja2 import Template

from dags.caching_util import get_with_cache, get_dags, get_tasks

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 3, 26),
}


def _generate_jinja(**kwargs):
    input_fp = '/usr/local/airflow/dags/dynamic_dags/template_dag.j2'
    output_fp = f'/usr/local/airflow/dags/dynamic_dags/generated/{dag_id}.py'
    with open(input_fp, 'r') as template, open(output_fp, 'w') as output:
        output.write(
            Template(template.read()).render(kwargs)
        )


with DAG(dag_id="jinja_template_generator", schedule_interval="@hourly", default_args=default_args, catchup=False) as dag:
    for dag_id, schedule_interval in get_with_cache(get_dags, "cached_dags.cache"):
        tasks = get_with_cache(get_tasks, f"{dag_id}_cached_tasks.cache", _dag_id=dag_id)

        PythonOperator(
            task_id=f"generate_{dag_id}",
            python_callable=_generate_jinja,
            op_kwargs={
                "dag_id": dag_id,
                "schedule_interval": schedule_interval,
                "tasks": tasks,
                "operators": {operator for (_, operator, _) in tasks}
            }
        )
