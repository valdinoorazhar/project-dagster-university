# src/dagster_essentials/defs/assets/trips.py
import duckdb
import os
import dagster as dg
from dagster._utils.backoff import backoff
import requests
from dagster_essentials.defs.assets import constants

# Asset that fetches taxi trip data from NYC Open Data Portal API 
@dg.asset
def taxi_trips_file(context) -> None:
    """
      The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    month_to_fetch = '2023-03'
    file_path = constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)

    # Request the data from NYC Open Data Portal
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )
    raw_trips.raise_for_status()

    with open(file_path, "wb") as output_file_trip:
        output_file_trip.write(raw_trips.content)

    context.log.info("Saved taxi trip file to path")


@dg.asset(
    deps=["taxi_trips_file"]
)
def taxi_trips(context) -> None:
    """
      The raw taxi trips dataset, loaded into a DuckDB database
    """
    query = """
        create or replace table trips as (
          select
            VendorID as vendor_id,
            PULocationID as pickup_zone_id,
            DOLocationID as dropoff_zone_id,
            RatecodeID as rate_code_id,
            payment_type as payment_type,
            tpep_dropoff_datetime as dropoff_datetime,
            tpep_pickup_datetime as pickup_datetime,
            trip_distance as trip_distance,
            passenger_count as passenger_count,
            total_amount as total_amount
          from 'data/raw/taxi_trips_2023-03.parquet'
        );
    """

    conn = backoff(
        fn=duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        kwargs={
            "database": os.getenv("DUCKDB_DATABASE"),
        },
        max_retries=10,
    )

    context.log.info("Running query to create 'trips' table")
    conn.execute(query)
    conn.close()
    context.log.info("Finished loading taxi trips into DuckDB")