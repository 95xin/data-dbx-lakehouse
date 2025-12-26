from pyspark import pipelines as dp
from pyspark.sql.functions import col

bronze_catalog = spark.conf.get("bronze_catalog")
silver_catalog = spark.conf.get("silver_catalog")
schema = spark.conf.get("schema")
src = f"{bronze_catalog}.{schema}.bronze_data"
sink = f"{silver_catalog}.{schema}.silver_data"

@dp.table(
    name=sink,
    comment="Cleaned and business logic applied silver table from bronze_data"
)
def silver_data():
    df = spark.read.table(src)
    # Data cleaning: remove rows with nulls in critical columns
    df_clean = df.dropna(subset=["timestamp", "energy", "turbine_id"])
    # Business logic: filter out negative energy readings and ensure sensor values are within expected range
    df_filtered = df_clean.filter(
        (col("energy") >= 0) &
        (col("sensor_A").between(0, 100)) &
        (col("sensor_B").between(0, 100)) &
        (col("sensor_C").between(0, 100)) &
        (col("sensor_D").between(0, 100)) &
        (col("sensor_E").between(0, 100)) &
        (col("sensor_F").between(0, 100))
    )
    return df_filtered