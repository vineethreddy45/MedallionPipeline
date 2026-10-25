from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common import get_spark

spark = get_spark("silver-clean")

def read_bronze(t):
    return spark.read.format("delta").load(f"data/bronze/{t}")

# ---------- policyholders: type + tidy ----------
holders = (read_bronze("policyholders")
    .withColumn("policyholder_id", F.col("policyholder_id").cast("int"))
    .withColumn("dob", F.to_date("dob"))
    .withColumn("email", F.lower(F.trim("email")))
    .dropDuplicates(["policyholder_id"])
    .drop("ingestion_ts", "source_file"))
holders.write.format("delta").mode("overwrite").save("data/silver/policyholders")

# ---------- policies: type ----------
policies = (read_bronze("policies")
    .withColumn("policy_id", F.col("policy_id").cast("int"))
    .withColumn("policyholder_id", F.col("policyholder_id").cast("int"))
    .withColumn("premium", F.col("premium").cast("double"))
    .withColumn("start_date", F.to_date("start_date"))
    .withColumn("end_date", F.to_date("end_date"))
    .dropDuplicates(["policy_id"])
    .drop("ingestion_ts", "source_file"))
policies.write.format("delta").mode("overwrite").save("data/silver/policies")

# ---------- claims: cast, standardize, dedupe ----------
claims = (read_bronze("claims")
    .withColumn("claim_id", F.col("claim_id").cast("int"))
    .withColumn("policy_id", F.col("policy_id").cast("int"))
    .withColumn("claim_date", F.to_date("claim_date"))            # '2024-13-45' becomes null here
    .withColumn("amount", F.col("amount").cast("double"))          # '' becomes null here
    .withColumn("status", F.upper(F.trim("status")))               # approved/APPROVED/' Approved ' -> APPROVED
    .withColumn("updated_at", F.to_timestamp("updated_at"))
    .drop("ingestion_ts", "source_file"))

# dedupe: if the source re-sends a claim, keep the latest version
w = Window.partitionBy("claim_id").orderBy(F.col("updated_at").desc())
claims = (claims.withColumn("rn", F.row_number().over(w))
                .filter("rn = 1").drop("rn"))

# ---------- validation: valid rows vs quarantine (with reason) ----------
valid_policy_ids = [r.policy_id for r in policies.select("policy_id").collect()]

claims = claims.withColumn("quarantine_reason",
    F.when(F.col("amount").isNull(), "missing amount")
     .when(F.col("amount") < 0, "negative amount")
     .when(F.col("claim_date").isNull(), "invalid claim_date")
     .when(~F.col("policy_id").isin(valid_policy_ids), "orphan policy_id"))

good = claims.filter("quarantine_reason IS NULL").drop("quarantine_reason")
bad  = (claims.filter("quarantine_reason IS NOT NULL")
              .withColumn("quarantined_at", F.current_timestamp()))

good.write.format("delta").mode("overwrite").save("data/silver/claims")
bad.write.format("delta").mode("overwrite").save("data/silver/claims_quarantine")

print("silver policyholders:", holders.count())
print("silver policies:", policies.count())
print("silver claims (clean):", good.count())
print("quarantined:", bad.count())
bad.groupBy("quarantine_reason").count().show()

spark.stop()
