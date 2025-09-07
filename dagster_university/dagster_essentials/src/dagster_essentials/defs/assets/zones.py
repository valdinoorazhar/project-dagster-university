# src/dagster_essentials/defs/assets/zones.py
import duckdb
import os
import dagster as dg
from dagster._utils.backoff import backoff
import requests
from dagster_essentials.defs.assets import constants

@dg.asset
def taxi_zones_file(context) -> None:
    """
      The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    raw_zones = requests.get(
        "https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv"
    )

    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file_zone:
        output_file_zone.write(raw_zones.content)

    context.log.info("Saved taxi zone file to path")


@dg.asset(
    deps=["taxi_zones_file"]
)
def taxi_zones(context) -> None:
    """
      The raw taxi zones dataset, loaded into a DuckDB database
    """
    query = """
        create or replace table zones as (
          select
            LocationID as zone_id,
            zone as zone,
            borough as borough,
            the_geom as geometry
          from 'data/raw/taxi_zones.csv'
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

    context.log.info("Running query to create 'zones' table")
    conn.execute(query)
    conn.close()
    context.log.info("Finished loading taxi zones into DuckDB")