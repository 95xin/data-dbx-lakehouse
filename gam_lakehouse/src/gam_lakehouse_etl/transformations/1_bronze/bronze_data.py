from pyspark import pipelines as dp
bronze_catalog = spark.conf.get("bronze_catalog")
target = f"{bronze_catalog}.gam.bronze_data"
@dp.table(
    name = target,
    comment="Bronze table directly reading from dev_bronze.gam.raw_data"
)
def bronze_data():
    return spark.read.table("dev_bronze.gam.raw_data")