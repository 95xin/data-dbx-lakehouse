from pyspark import pipelines as dp
bronze_catalog = spark.conf.get("bronze_catalog")
schema = spark.conf.get("schema")
target = f"{bronze_catalog}.{schema}.bronze_data"
src = f"{bronze_catalog}.{schema}.raw_data"
@dp.table(
    name = target,
    comment="Bronze table directly reading from dev_bronze.gam.raw_data"
)
def bronze_data():
    return spark.read.table(src)