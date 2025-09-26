# src/dagster_essentials/defs/partitions.py
import dagster as dg
from dagster_essentials.defs.assets import constants

start_date = constants.START_DATE
end_date = constants.END_DATE

monthly_partition = dg.MonthlyPartitionsDefinition(
    start_date=start_date,
    end_date=end_date
)

weekly_parition = dg.WeeklyPartitionsDefinition (
    start_date=start_date,
    end_date=end_date
)

