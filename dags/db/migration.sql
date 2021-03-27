CREATE TABLE IF NOT EXISTS cached_dags (
    dag_id TEXT PRIMARY KEY,
    schedule_interval TEXT DEFAULT '@hourly',
    inserted_at TEXT DEFAULT '1970-01-01 00:00:00.000',
    last_modifed TEXT  DEFAULT '1970-01-01 00:00:00.000'
);

CREATE TABLE IF NOT EXISTS cached_dag_tasks (
    dag_id TEXT,
    task_id TEXT PRIMARY KEY,
    depends_on_task_id TEXT,
    operator TEXT DEFAULT 'airflow.operators.dummy_operator.DummyOperator',
    inserted_at TEXT DEFAULT '1970-01-01 00:00:00.000',
    last_modifed TEXT  DEFAULT '1970-01-01 00:00:00.000',
    FOREIGN KEY(dag_id) REFERENCES cached_dags(dag_id)
);

INSERT INTO cached_dags (dag_id, schedule_interval)
VALUES ('cached_dag', '@hourly');

insert into cached_dag_tasks (dag_id, task_id, operator, depends_on_task_id)
VALUES ('cached_dag', 'task_1', 'airflow.operators.dummy_operator.DummyOperator', null),
       ('cached_dag', 'task_2', 'airflow.operators.dummy_operator.DummyOperator', 'task_1');

select * from cached_dags
select * from cached_dag_tasks