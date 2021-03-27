import importlib
import logging
from datetime import datetime
from typing import List, Union

from airflow import DAG


# --------------- CACHING FUNCTION -----------------------
import os
import pickle
import time
from typing import Any, Callable

from airflow.hooks.sqlite_hook import SqliteHook


def get_with_cache(fn: Callable = None, cache_path: str = None, expire_at_sec: int = 3600, *args, **kwargs) -> Any:
    """A function, intended to be used at top-level in a dag, to return database/networked results
    with the intent of building dynamic dags / tasks, while keeping scheduler degradation to a minimum.

    A good alternative to this function would be to keep the things you are generating your DAGs/tasks from on disk
    or using the [cachier](https://pypi.org/project/cachier/) library or some other alternative

    Note: Dynamic DAGs / tasks can always fail or degrade performance in unpredictable ways, be cautious
    before you rely on a feature like this, and make SURE that you have backups and trust the source you are
    building off. In particular, airflow expects DAGs / tasks to remain MOSTLY static. Unexpected or breaking
    behavior can happen.

    :param fn: A function, with optional arguments, receives *args
    :param expire_at_sec: Number of seconds after which cached results should be considered stale=
    default 1 hour (3600 seconds), cache will be refreshed eventually after, but before this number
    :param cache_path: a string which is a file path to store the pickled object as a disk-based cache
    :return: the results of whichever function is given as "fn"
    """
    if cache_path is None or fn is None:
        raise RuntimeError("Function and cache path required, cache failed to load!")

    if os.path.isfile(cache_path) and int(time.time() - os.path.getmtime(cache_path)) < expire_at_sec:
        return pickle.load(open(cache_path, 'rb'))
    else:
        results = fn(*args, **kwargs)
        pickle.dump(results, open(cache_path, 'wb'))
        return results
# --------------- CACHING FUNCTION -----------------------


def _get_dags() -> List[str]:
    """Dynamic method to get DAGs from SQLITE database"""
    sql = f"""SELECT dag_id, schedule_interval FROM cached_dags;"""
    return SqliteHook().get_records(sql)


def _get_tasks(_dag_id: str) -> List[Union[str, int]]:
    """Dynamic method to get tasks from SQLITE database"""
    sql = """SELECT task_id, operator, depends_on_task_id FROM cached_dag_tasks WHERE dag_id = :dag_id;"""
    return SqliteHook().get_records(sql, {"dag_id": dag_id})


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 3, 26),
}


"""
Note: Dynamic DAGs and Tasks should be avoided for many reasons, performance, stability, and maintainability being
the primary ones. Use with caution!
"""
for dag_id, schedule_interval in get_with_cache(_get_dags, "cached_dags.cache"):
    with DAG(dag_id=dag_id, schedule_interval=schedule_interval, catchup=False, default_args=default_args) as dag:
        tasks = get_with_cache(_get_tasks, f"{dag_id}_cached_tasks.cache", _dag_id=dag_id)

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
