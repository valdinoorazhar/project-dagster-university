# src/dagster_essentials/defs/assets/trips.py
import os
import dagster as dg
from dagster_duckdb import DuckDBResource
import requests
from dagster_essentials.defs.assets import constants
from dagster_essentials.defs.partitions import monthly_partition

# Asset that fetches taxi trip data from NYC Open Data Portal API 
@dg.asset(
        partitions_def = monthly_partition
)
def taxi_trips_file(context) -> None:
    """
      The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]
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
def taxi_trips(context, database:DuckDBResource) -> None:
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

    with database.get_connection() as conn:
      context.log.info("Running query to create 'trips' table")
      conn.execute(query)

    #conn.close()
    #context.log.info("Finished loading taxi trips into DuckDB")