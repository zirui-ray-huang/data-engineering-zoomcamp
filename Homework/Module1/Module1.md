# Question 1
```console
$ docker run -it --rm --entrypoint=bash python:3.12.8-slim
$ pip --version
pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)
```

# Question 2
Given the following `docker-compose.yaml`, what is the `hostname` and `port` that pgadmin should use to connect to the postgres database?
```console
localhost:5432
```

To ingest trip data and zone data into Postgres, first, run a containerized version of Postgres
```console
mkdir '../Homework/Module1/ny_taxi_postgres_data'
cd '../Homework/Module1'
docker run -it -e POSTGRES_USER="root" -e POSTGRES_PASSWORD="root" -e POSTGRES_DB="ny_taxi" -v ny_taxi_postgres_data:/var/lib/postgresql -p 5432:5432 postgres:18
```

Meanwhile, you can use `pgcli` to interact with PostgresSQL in CLI. 

```console
uv run pgcli -h localhost -p 5432 -u root -d ny_taxi
```

Run data ingestion process:
```console
uv run python ../Homework/Module1/ingest_data.py
```
# Question 2+
If you want to use `pgadmin4` to manage database:

First, create a docker network:
```console
docker network create pg-network
```

Then, run a containerized version of Postgres on the docker network:
```console
mkdir '../Homework/Module1/ny_taxi_postgres_data'
cd '../Homework/Module1'
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v ny_taxi_postgres_data:/var/lib/postgresql \
  -p 5432:5432 \
  --network=pg-network \
  --name pgdatabase \
  postgres:18
```

At last, run `pgAdmin` on the same network:
```console
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -v pgadmin_data:/var/lib/pgadmin \
  -p 8085:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4
```

In `pgAdmin`, you need to add a server, with `hostname` as docker postgres custom container name `pgdatabase`.

Under this situation, if you want to run `pgcli`, run:
```console
uv run pgcli -h localhost -u root -d ny_taxi
```

And ingesting data script should be run as:

```console
uv run python ../Homework/Module1/ingest_data.py
```

# Question 3
```sql
SELECT COUNT(1)
FROM trips_2019_10
WHERE lpep_pickup_datetime >= '2019-10-01' 
  AND lpep_pickup_datetime <  '2019-11-01'
  AND lpep_dropoff_datetime >= '2019-10-01' 
  AND lpep_dropoff_datetime <  '2019-11-01'
  AND trip_distance <= 1
```
The answer is: 104,802; 198,924; 109,603; 27,678; 35,189

# Question 4
```sql
SELECT CAST(lpep_pickup_datetime AS DATE) AS pickup_date, 
       MAX(trip_distance) AS longest_trip
FROM trips_2019_10
GROUP BY pickup_date
ORDER BY longest_trip DESC
```
The answer is: 2019-10-31

# Question 5
```sql
SELECT CAST(t.lpep_pickup_datetime AS DATE) AS pickup_date,
       z."Zone" AS zone,
       SUM(t.total_amount) AS total_amount
FROM trips_2019_10 AS t 
JOIN zones AS z ON t."PULocationID" = z."LocationID"
WHERE CAST(t.lpep_pickup_datetime AS DATE) = '2019-10-18' 
GROUP BY pickup_date, zone
HAVING SUM(t.total_amount) > 13000
ORDER BY total_amount DESC;
```
Tha answer is: East Harlem North, East Harlem South, Morningside Heights

# Question 6
```sql
SELECT zdo."Zone" AS dropoff_zone,
       MAX(t.tip_amount) AS max_tip
FROM trips_2019_10 AS t 
JOIN zones AS zpu ON t."PULocationID" = zpu."LocationID"
JOIN zones AS zdo ON t."DOLocationID" = zdo."LocationID"
WHERE CAST(t.lpep_pickup_datetime AS DATE) >= '2019-10-01' AND 
    CAST(t.lpep_pickup_datetime AS DATE) <= '2019-10-31' AND
    zpu."Zone" = 'East Harlem North'
GROUP BY dropoff_zone
ORDER BY max_tip DESC;
```
The answer is: JFK Airport

# Question 7
The answer is: terraform init, terraform apply -auto-approve, terraform destroy