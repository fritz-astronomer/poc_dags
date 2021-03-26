# Proof-of-concept DAGs for Airflow using the Astronomer.io Platform

## Dynamic Dags 
### Cached DAG 
- uses database results to dynamically generate tasks. 
- queries the database during the dag parsing loop
- utilizes a results file pickled to disk to reduce database / parsing performance implications

### Jinja Templating
- uses database results to dynamically generate dags and tasks
- utilizes a helper DAG to assemble and produce the Jinja templates
- can alleviate performance impacts for dynamic dags and tasks