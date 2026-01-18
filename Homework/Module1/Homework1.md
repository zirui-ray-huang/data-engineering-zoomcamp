# Homework 1
## Question 1
```console
$ docker run -it --rm --entrypoint=bash python:3.13-slim
$ pip --version
pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)
```
The answer is: 

`25.3`
## Question 2
`hostname`: postgres

`port`: 5433

## ingest_data.py
refer to `Homework/ingest_data.py` for the data ingestion script.

## Question 3
```sql
SELECT COUNT(1)
FROM trips_2025_11
WHERE lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime <  '2025-12-01'
  AND trip_distance <= 1;
```
The answer is: 

`8,007`

## Question 4
```sql
SELECT t.lpep_pickup_datetime as date,
	MAX(trip_distance) as max_dist
FROM trips_2025_11 as t
WHERE trip_distance < 100
GROUP BY lpep_pickup_datetime
ORDER BY max_dist DESC;
```
The answer is: 

`2025-11-14`

## Question 5
```sql
SELECT z."Zone" as puzone,
	SUM(t.total_amount) as totamount
FROM trips_2025_11 as t
	JOIN zones as z
	ON t."PULocationID" = z."LocationID"
WHERE CAST(t.lpep_pickup_datetime AS DATE) = '2025-11-18'
GROUP BY z."Zone"
ORDER BY totamount DESC;
```
The answer is: 

`East Harlem North`

## Question 6
```sql
SELECT zdo."Zone" as dozone,
	MAX(t.tip_amount) as maxtip
FROM trips_2025_11 as t
	JOIN zones as zpu ON t."PULocationID" = zpu."LocationID"
	JOIN zones as zdo ON t."DOLocationID" = zdo."LocationID"
WHERE lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime <  '2025-12-01'
  AND zpu."Zone" = 'East Harlem North'
GROUP BY zdo."Zone"
ORDER BY maxtip DESC;

```
The answer is: 

`Yorkville West`

## Question 7
The answer is: 

`terraform init, terraform apply -auto-approve, terraform destroy`