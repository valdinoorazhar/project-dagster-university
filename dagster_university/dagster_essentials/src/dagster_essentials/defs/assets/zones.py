# src/dagster_essentials/defs/assets/zones.py
import requests
from dagster_essentials.defs.assets import constants
import dagster as dg

@dg.asset
def taxi_zones_file() -> None:
    """
      The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    raw_zones = requests.get(
        "https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv"
    )

    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file_zone:
        output_file_zone.write(raw_zones.content)