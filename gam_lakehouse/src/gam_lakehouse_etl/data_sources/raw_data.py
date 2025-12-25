from pyspark import pipelines as dp
bronze_catalog = spark.conf.get("bronze_catalog")
target = f"{bronze_catalog}.gam.raw_data"
@dp.table(
    name=target,
    comment="Raw dataset loaded from Parquet in prod_bronze.gam volume"
)
def raw_data():
    return spark.read.format("parquet").load("/Volumes/prod_bronze/gam/raw_data")