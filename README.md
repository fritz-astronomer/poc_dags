# Proof-of-concept DAGs for Airflow

# Quickstart
## SQLite3
1) Run `sqlite3 dags/db/db.sqlite < dags/db/migration.sql`
Airflow's scheduler will connect to this database in `dags/dynamic_dags/cached_dag.py`

## Astro CLI
Astronomer is a utility for quickly bringing up Airflow deployments, and hosting deployments on-prem or in the cloud.
1) Download `astro` cli, [instructions here](https://www.astronomer.io/docs/cloud/stable/get-started/quickstart)
2) ~~Run `astro dev stop && astro dev start && astro dev logs -f`, to start or restart a local airflow stack~~
- Note: use `docker compose up` instead. Needed to fall-back to docker compose to mount the scheduler dags as read/write
3) Go to [http://localhost:8080](http://localhost:8080) , after a few seconds (up to a minute)
4) Log in with `admin:admin`

## Dynamic Dags
Note: Dynamic DAGs and Tasks should be avoided for many reasons - especially at scale. 
Performance, stability, and maintainability may be negatively affected. Use with caution!
### [cached_dag.py](./dags/dynamic_dags/cached_dag.py)
- Uses sqlite database results to dynamically generate a dag and it's tasks
- Queries the database during the *scheduler's* dag parsing loop
- Utilizes a results file pickled to disk to reduce database / parsing performance implications
- NOTE: Something like the [cachier](https://pypi.org/project/cachier/) library could also be used here, however the author has experienced issues previously with it.
- Can alleviate performance impacts for dynamic dags and tasks

### [jinja_templated_generator.py](./dags/dynamic_dags/jinja_templated_generator.py) 
- Uses sqlite database results to dynamically generate a dag and it's tasks
- Utilizes a helper DAG to assemble and produce the Jinja templates
- Can alleviate performance impacts for dynamic dags and tasks