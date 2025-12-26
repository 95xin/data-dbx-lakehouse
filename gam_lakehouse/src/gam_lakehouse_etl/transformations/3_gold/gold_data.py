from pyspark import pipelines as dp
from pyspark.sql.functions import col

bronze_catalog = spark.conf.get("bronze_catalog")
silver_catalog = spark.conf.get("silver_catalog")
gold_catalog = spark.conf.get("gold_catalog")
schema = spark.conf.get("schema")
src = f"{silver_catalog}.{schema}.silver_data_2"
sink = f"{gold_catalog}.{schema}.gold_data"

@dp.table(
    name=sink,
    comment="Cleaned and business logic applied gold table from bronze_data"
)
def silver_data():
    df = spark.read.table(src)
    # Data cleaning: remove rows with nulls in critical columns
    # Business logic: filter out negative energy readings and ensure sensor values are within expected range
    df_filtered = df.filter(
        (col("sensor_A").between(0, 100))
    )
    return df_filtered