import pandas as pd
from sqlalchemy import create_engine
import click

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
def main(pg_user, pg_pass, pg_host, pg_port, pg_db):
    year = 2019
    month = 10
    chunk_size = 100000
    trip_target_table = f'trips_{year}_{month}'
    zone_target_table = 'zones'
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    """Ingest Trip data into a database."""
    trip_filepath = r'../Homework/Module1/green_tripdata_2019-10.csv.gz'
    trip_dtype = {
        "VendorID": "Int64",
        "passenger_count": "Int64",
        "trip_distance": "float64",
        "RatecodeID": "Int64",
        "store_and_fwd_flag": "string",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "payment_type": "Int64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64"
    }
    trip_parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    trip_df_iter = pd.read_csv(
        trip_filepath,
        dtype=trip_dtype,
        parse_dates=trip_parse_dates,
        chunksize=chunk_size)

    trip_first_chunk = next(trip_df_iter)
    trip_first_chunk.head(n=0).to_sql(name=trip_target_table,
                                      con=engine,
                                      if_exists='replace')

    trip_first_chunk.to_sql(name=trip_target_table,
                            con=engine,
                            if_exists='append')
    print(f"Trip Data * Inserted first chunk: {len(trip_first_chunk)}")

    for df_chunk in trip_df_iter:
        df_chunk.to_sql(
            name=trip_target_table,
            con=engine,
            if_exists="append"
        )
        print(f"Trip Data * Inserted chunk: {len(df_chunk)}")

    print(f'Trip Data * Done ingesting to {trip_target_table}')



    zone_filepath = r'../Homework/Module1/taxi_zone_lookup.csv'
    zone_dtype = {
        "LocationID": "Int64",
        "Borough": "string",
        "Zone": "string",
        "service_zone": "string"
    }
    df_zone = pd.read_csv(zone_filepath, dtype=zone_dtype)

    df_zone.head(n=0).to_sql(name=zone_target_table,
                             con=engine,
                             if_exists='replace')
    df_zone.to_sql(name=zone_target_table,
                   con=engine,
                   if_exists='append')

    print(f'Zone Data * Done ingesting to {zone_target_table}')


if __name__ == "__main__":
    main()