FROM quay.io/astronomer/ap-airflow:1.10.14-buster-onbuild

ENV AIRFLOW_CONN_SQLITE_DEFAULT="sqlite://%2Fusr%2Flocal%2Fairflow%2Fdags%2Fdb%2Fdb.sqlite"
