import duckdb
#from dagster_duckdb import DuckDBResource
conn = duckdb.connect(database="staging/data.duckdb")
conn.execute("drop table trips;")
