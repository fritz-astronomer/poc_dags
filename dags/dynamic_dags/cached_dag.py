import importlib
from datetime import datetime

from airflow import DAG

from dags.caching_util import get_with_cache, get_dags, get_tasks

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 3, 26),
}


"""
Note: Dynamic DAGs and Tasks should be avoided for many reasons, performance, stability, and maintainability being
the primary ones. Use with caution!
"""
for dag_id, schedule_interval in get_with_cache(get_dags, "cached_dags.cache"):
    with DAG(dag_id=dag_id, schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:
        tasks = get_with_cache(get_tasks, f"{dag_id}_cached_tasks.cache", _dag_id=dag_id)

        # import operator classes
        tasks_with_imports = [(
            task_id,
            getattr(importlib.import_module('.'.join(operator_class.split('.')[:-1])), operator_class.split('.')[-1]),
            depends_on_task_id
            ) for task_id, operator_class, depends_on_task_id in tasks
        ]

        # define tasks
        created_tasks = {}
        for task_id, operator, _ in tasks_with_imports:
            created_tasks[task_id] = operator(task_id=task_id)

        # link tasks
        for task_id, _, depends_on_task_id in tasks_with_imports:
            if task_id is not None and depends_on_task_id is not None:
                created_tasks[task_id] << created_tasks[depends_on_task_id]
