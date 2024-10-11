## How to Run Airflow via Docker Compose With LocalExecutor

This will only 2 containers

1. Clone the repo to your computer
```
git clone https://github.com/keeyong/sjsu-data226.git
```
2. Change the current directory to sjsu-data226/week8/airflow
```
cd sjsu-data226/week8/airflow
```
3. First initialize Airflow environment
```
docker compose -f docker-compose-min.yaml up airflow-init
```
4. Next run the Airflow service
```
docker compose -f docker-compose-min.yaml up
```
5. Wait some time, then visit http://localhost:8081 and log in (Use ID:PW of airflow:airflow)

Set up Connections (snowflake_conn for example) and Variables accordingly 

7. Now let's log in to airflow docker container
 - For that end, run "docker ps" command and get the container ID of airflow-airflow-1. In the following case, it is "a9fc54d4b0b3"
```
docker ps  # look for airflow-1
CONTAINER ID   IMAGE                  COMMAND                  CREATED          STATUS                             PORTS                    NAMES
a9fc54d4b0b3   apache/airflow:2.9.1   "/usr/bin/dumb-init …"   9 minutes ago    Up 21 seconds (health: starting)   0.0.0.0:8081->8080/tcp   airflow-airflow-1
c9ca72c25eb4   postgres:13            "docker-entrypoint.s…"   30 minutes ago   Up 33 seconds (healthy)            5432/tcp                 airflow-postgres-1
```
 - Now run "docker exec -it" command with the ID to log in
```
docker exec -it a9fc54d4b0b3 sh
(airflow)
```
8. Let's run a few Airflow commands
```
(airflow)airflow dags list
(airflow)airflow tasks list YfinanceToSnowflake
(airflow)airflow dags test YfinanceToSnowflake 2024-10-10
```
