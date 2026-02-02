# Question 2
```sql
SELECT COUNT(1) FROM stg_yellow_trips
WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2020;
```

# Question 3
```sql
SELECT COUNT(1) FROM stg_green_trips
WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2020;
```

# Question 4
```sql
SELECT COUNT(1) FROM stg_green_trips
WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2021 AND 
      EXTRACT(MONTH FROM tpep_pickup_datetime) = 3;
```


