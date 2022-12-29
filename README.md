# micro-mobility-platform

## Prerequisites:
- [docker](https://docs.docker.com/engine/installation/) installed on your machine
- [docker-compose](https://docs.docker.com/get-started/08_using_compose/) installed on your machine

Execute the following command to check if any postgres instance is currently occupying port `5432` :
```shell
sudo ss -lptn 'sport = :5432'
```

If the result is not empty, please stop postgresql service with this command :
```shell
sudo service postgresql stop
```

## Launch micromobility platform
1. Open a terminal and navigate to the project home directory
2. If you are using Linux, execute the following command : `echo -e "AIRFLOW_UID=$(id -u)" > .env`
3. Launch the platform using docker compose by running the following commands in this order:

```shell
docker compose down --volumes  --rmi all
docker compose up airflow-init
docker compose up
```

## Start the ingestion DAG

After all containers are started, the Airflow web interface should be available at http://0.0.0.0:8080/home.

Please use these credentials to log in :
- user : `airflow`
- password : `airflow`

Click on the activation button to enable live ingestion. When activated, the DAG will trigger an ingestion 
of bike share stations statuses and description **every 5min** for the following systems:
- *capital-bikeshare* (Washington DC, USA)


![alt text](./docs/activate_dag.png "Activate the ingestion dag")

You can verify that the ingestion is actually inserting rows in the postgres database by connecting to it :
1. If not done already, install *psql* CLI : https://www.timescale.com/blog/how-to-install-psql-on-mac-ubuntu-debian-windows/
2. Open a terminal and connect to the `mobiliy_data` database: `psql postgresql://ingest:ingest@0.0.0.0:5432/mobility_data`
3. Query the populated tables. Examples:
    - `SELECT * FROM stations.status_history LIMIT 10;`
    - `SELECT COUNT(1) FROM stations.status_history;`
    - `SELECT * FROM stations.inventory LIMIT 10;`
    - `SELECT COUNT(1) FROM stations.inventory;`


## Export hourly-aggregations

Have fun!