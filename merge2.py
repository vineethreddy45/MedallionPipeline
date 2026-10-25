from pyspark.sql import functions as F
from delta.tables import DeltaTable
from common import get_spark

spark = get_spark("silver-merge")

print("silver claims BEFORE:",
      spark.read.format("delta").load("data/silver/claims").count())

# 1) new claims land in bronze, raw, exactly like batch 1
raw = (spark.read.option("header", True).csv("data/landing/claims_new.csv")
       .withColumn("ingestion_ts", F.current_timestamp())
       .withColumn("source_file", F.lit("claims_new.csv")))
raw.write.format("delta").mode("append").save("data/bronze/claims")

# 2) clean the delta with the same rules as silver.py
delta = (raw.drop("ingestion_ts", "source_file")
    .withColumn("claim_id", F.col("claim_id").cast("int"))
    .withColumn("policy_id", F.col("policy_id").cast("int"))
    .withColumn("claim_date", F.to_date("claim_date"))
    .withColumn("amount", F.col("amount").cast("double"))
    .withColumn("status", F.upper(F.trim("status")))
    .withColumn("updated_at", F.to_timestamp("updated_at")))

valid_policy_ids = [r.policy_id for r in
    spark.read.format("delta").load("data/silver/policies").select("policy_id").collect()]
delta = delta.withColumn("quarantine_reason",
    F.when(F.col("amount").isNull(), "missing amount")
     .when(F.col("amount") < 0, "negative amount")
     .when(F.col("claim_date").isNull(), "invalid claim_date")
     .when(~F.col("policy_id").isin(valid_policy_ids), "orphan policy_id"))

good_new = delta.filter("quarantine_reason IS NULL").drop("quarantine_reason")
bad_new = (delta.filter("quarantine_reason IS NOT NULL")
                .withColumn("quarantined_at", F.current_timestamp()))
bad_new.write.format("delta").mode("append").save("data/silver/claims_quarantine")
print("new rows quarantined:", bad_new.count())

# 3) MERGE clean new claims into silver (insert new, update if newer)
(DeltaTable.forPath(spark, "data/silver/claims").alias("target")
    .merge(good_new.alias("source"), "target.claim_id = source.claim_id")
    .whenMatchedUpdateAll("source.updated_at > target.updated_at")
    .whenNotMatchedInsertAll()
    .execute())

# 4) MERGE the status updates (fresh handle - Delta tables are versioned)
updates = (spark.read.option("header", True).csv("data/landing/status_updates.csv")
    .withColumn("claim_id", F.col("claim_id").cast("int"))
    .withColumn("updated_at", F.to_timestamp("updated_at"))
    .select("claim_id", F.col("new_status").alias("status"), "updated_at"))
(DeltaTable.forPath(spark, "data/silver/claims").alias("target")
    .merge(updates.alias("source"), "target.claim_id = source.claim_id")
    .whenMatchedUpdate(condition="source.updated_at > target.updated_at",
                       set={"status": "source.status", "updated_at": "source.updated_at"})
    .execute())

after = spark.read.format("delta").load("data/silver/claims")
print("silver claims AFTER:", after.count())
after.filter("claim_id IN (2, 6, 10)").select("claim_id", "status").orderBy("claim_id").show()
print("quarantine total:",
      spark.read.format("delta").load("data/silver/claims_quarantine").count())
spark.stop()
