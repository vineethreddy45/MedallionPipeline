import os
os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/jdk1.8.0_241.jdk/Contents/Home"

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (SparkSession.builder
    .appName("medallion-smoke-test")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

spark = configure_spark_with_delta_pip(builder).getOrCreate()

data = [(1, "CLM-1001", 2500.0), (2, "CLM-1002", 840.0), (3, "CLM-1003", 12999.0)]
df = spark.createDataFrame(data, ["claim_id", "claim_number", "amount"])

df.write.format("delta").mode("overwrite").save("data/smoke_delta")
spark.read.format("delta").load("data/smoke_delta").show()

print("SMOKE TEST PASSED: Delta write + read worked")
spark.stop()
