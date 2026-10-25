from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common import get_spark

spark = get_spark("gold-star-schema")

holders  = spark.read.format("delta").load("data/silver/policyholders")
policies = spark.read.format("delta").load("data/silver/policies")
claims   = spark.read.format("delta").load("data/silver/claims")

# ---------- dimensions with surrogate keys ----------
dim_policyholder = (holders
    .withColumn("policyholder_key", F.row_number().over(Window.orderBy("policyholder_id")))
    .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
    .select("policyholder_key", "policyholder_id", "full_name", "state", "email", "dob"))

dim_policy = (policies
    .withColumn("policy_key", F.row_number().over(Window.orderBy("policy_id")))
    .select("policy_key", "policy_id", "policy_type", "premium", "status",
            "start_date", "end_date", "policyholder_id"))

dim_claim_type = (claims.select("claim_type").distinct()
    .withColumn("claim_type_key", F.row_number().over(Window.orderBy("claim_type")))
    .select("claim_type_key", "claim_type"))

dim_date = (claims.select("claim_date").distinct()
    .withColumn("date_key", F.date_format("claim_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("claim_date"))
    .withColumn("month", F.month("claim_date"))
    .withColumn("month_name", F.date_format("claim_date", "MMMM"))
    .withColumn("quarter", F.quarter("claim_date"))
    .select("date_key", F.col("claim_date").alias("date"),
            "year", "month", "month_name", "quarter"))

for name, df in [("dim_policyholder", dim_policyholder), ("dim_policy", dim_policy),
                 ("dim_claim_type", dim_claim_type), ("dim_date", dim_date)]:
    df.write.format("delta").mode("overwrite").save(f"data/gold/{name}")
    print(name, df.count())

# ---------- fact table: measures + dimension keys only ----------
fact = (claims
    .join(policies.select("policy_id", "policyholder_id"), "policy_id")
    .join(dim_policy.select("policy_key", "policy_id"), "policy_id")
    .join(dim_policyholder.select("policyholder_key", "policyholder_id"), "policyholder_id")
    .join(dim_claim_type, "claim_type")
    .withColumn("date_key", F.date_format("claim_date", "yyyyMMdd").cast("int"))
    .select("claim_id", "policy_key", "policyholder_key", "date_key", "claim_type_key",
            F.col("status").alias("claim_status"), "amount"))

fact.write.format("delta").mode("overwrite").save("data/gold/fact_claims")
print("fact_claims", fact.count())

# ---------- data-quality report ----------
bronze_n = spark.read.format("delta").load("data/bronze/claims").count()
quar_n   = spark.read.format("delta").load("data/silver/claims_quarantine").count()
print("\n--- DQ REPORT ---")
print("1. Bronze = clean + quarantined + duplicates removed:",
      bronze_n, "=", claims.count(), "+", quar_n, "+", bronze_n - claims.count() - quar_n)
print("2. Fact rows == silver claims:", fact.count() == claims.count())
print("3. No null amounts in fact:", fact.filter("amount IS NULL").count() == 0)
print("4. Fact rows missing a policy dimension:",
      fact.join(dim_policy, "policy_key", "left_anti").count())
s_fact = round(fact.agg(F.sum("amount")).first()[0], 2)
s_silv = round(claims.agg(F.sum("amount")).first()[0], 2)
print("5. Amount totals match (fact vs silver):", s_fact == s_silv, "->", s_silv)

# ---------- the payoff: a business question in 4 lines ----------
fact.join(dim_policyholder, "policyholder_key") \
    .groupBy("state").agg(F.round(F.sum("amount"), 2).alias("total_claim_amount")) \
    .orderBy(F.col("total_claim_amount").desc()).show()

spark.stop()
