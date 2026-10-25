from pyspark.sql.functions import current_timestamp, lit
from common import get_spark

spark = get_spark("bronze-ingest")

for t in ["policyholders", "policies", "claims"]:
    df = spark.read.option("header", True).csv(f"data/landing/{t}.csv")  # all columns stay strings: raw means raw
    df = (df.withColumn("ingestion_ts", current_timestamp())
            .withColumn("source_file", lit(f"{t}.csv")))
    df.write.format("delta").mode("append").save(f"data/bronze/{t}")
    print(t, "bronze rows:", spark.read.format("delta").load(f"data/bronze/{t}").count())

spark.stop()
