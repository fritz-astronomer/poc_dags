import os
import pickle
import time
from typing import Any, Callable, List, Union

from airflow.hooks.sqlite_hook import SqliteHook


def get_dags() -> List[str]:
    """Dynamic method to get DAGs from SQLITE database"""
    sql = f"""SELECT dag_id, schedule_interval FROM cached_dags;"""
    return SqliteHook().get_records(sql)


def get_tasks(_dag_id: str) -> List[Union[str, int]]:
    """Dynamic method to get tasks from SQLITE database"""
    sql = """SELECT task_id, operator, depends_on_task_id FROM cached_dag_tasks WHERE dag_id = :dag_id;"""
    return SqliteHook().get_records(sql, {"dag_id": _dag_id})


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
