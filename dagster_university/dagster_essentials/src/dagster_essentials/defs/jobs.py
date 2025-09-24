# src/dagster_essentials/defs/jobs.py
import dagster as dg

# select trips_by_week asset
trips_by_week = dg.AssetSelection.assets("trips_by_week")

# a job that includes all asset other than trips_by_week
trip_update_job = dg.define_asset_job(
    name="trip_update_job",
    selection=dg.AssetSelection.all() - trips_by_week
)

# this job will materialize trips_by_week asset
weekly_update_job = dg.define_asset_job(
    name="weekly_update_job",
    selection=trips_by_week,
)