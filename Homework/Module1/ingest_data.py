import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
import click

@click.command()
@click.option('--pg-user', default='postgres', help='PostgreSQL username')
@click.option('--pg-pass', default='postgres', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5433', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
def main(pg_user, pg_pass, pg_host, pg_port, pg_db):
    year = 2025
    month = 11
    chunk_size = 100000
    trip_target_table = f'trips_{year}_{month}'
    zone_target_table = 'zones'
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    """Ingest Trip data into a database."""
    trip_filepath = fr'green_tripdata_{year}-{month}.parquet'

    parquet_file = pq.ParquetFile(trip_filepath)
    first_chunk = True

    for batch in parquet_file.iter_batches(batch_size=chunk_size):
        # Convert the PyArrow batch to a Pandas DataFrame
        df_chunk = batch.to_pandas()

        if first_chunk:
            # Create the table schema (replace) and insert first batch
            df_chunk.head(n=0).to_sql(name=trip_target_table, con=engine, if_exists='replace')
            df_chunk.to_sql(name=trip_target_table, con=engine, if_exists='append')
            print(f"Trip Data * Inserted first chunk: {len(df_chunk)}")
            first_chunk = False
        else:
            # Append subsequent batches
            df_chunk.to_sql(name=trip_target_table, con=engine, if_exists="append")
            print(f"Trip Data * Inserted chunk: {len(df_chunk)}")

    print(f'Trip Data * Done ingesting to {trip_target_table}')


    zone_filepath = r'taxi_zone_lookup.csv'
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