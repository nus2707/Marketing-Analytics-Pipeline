# Databricks notebook source — Ads Analytics Pipeline
# Cluster: Runtime 14.x LTS (PySpark 3.5, Delta Lake 3.x), 4+ cores
#
# Before running:
#   • Upload the ads_pipeline/ folder to /Workspace/Repos/AdsPipeline/
#   • Set KAFKA_BOOTSTRAP and DELTA_BASE below
#   • Store the Event Hubs connection string in Databricks Secrets:
#       databricks secrets put --scope ads-pipeline --key eventhub-conn-str

# COMMAND ----------
# MAGIC %md ## 1 · Config

# COMMAND ----------
KAFKA_BOOTSTRAP = "YOUR-EVENTHUB.servicebus.windows.net:9093"
KAFKA_TOPIC     = "ad-impressions"
SASL_CONN_STR   = dbutils.secrets.get("ads-pipeline", "eventhub-conn-str")

# ADLS Gen2 path (replace with your storage account)
DELTA_BASE = "abfss://ads@YOURACCOUNT.dfs.core.windows.net/delta"

# COMMAND ----------
# MAGIC %md ## 2 · Ingest Raw Events from Event Hubs

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import *

RAW_SCHEMA = StructType([
    StructField("event_id",      StringType(),  False),
    StructField("timestamp",     StringType(),  False),
    StructField("campaign_id",   StringType(),  False),
    StructField("campaign_name", StringType(),  False),
    StructField("campaign_type", StringType(),  False),
    StructField("advertiser",    StringType(),  False),
    StructField("platform",      StringType(),  False),
    StructField("channel",       StringType(),  False),
    StructField("date",          StringType(),  False),
    StructField("impressions",   IntegerType(), False),
    StructField("clicks",        IntegerType(), False),
    StructField("conversions",   IntegerType(), False),
    StructField("spend_usd",     DoubleType(),  False),
    StructField("revenue_usd",   DoubleType(),  False),
])

# Structured Streaming from Event Hubs (Kafka protocol)
raw_stream = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
         .option("kafka.sasl.mechanism", "PLAIN")
         .option("kafka.security.protocol", "SASL_SSL")
         .option("kafka.sasl.jaas.config",
                 f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                 f'username="$ConnectionString" password="{SASL_CONN_STR}";')
         .option("subscribe", KAFKA_TOPIC)
         .option("startingOffsets", "earliest")
         .load()
)

parsed_stream = (
    raw_stream
    .select(F.from_json(F.col("value").cast("string"), RAW_SCHEMA).alias("d"))
    .select("d.*")
)

# Write to Bronze (append)
(parsed_stream.writeStream
              .format("delta")
              .outputMode("append")
              .option("checkpointLocation", f"{DELTA_BASE}/_ckpt/bronze")
              .start(f"{DELTA_BASE}/bronze/raw_ad_events"))

# COMMAND ----------
# MAGIC %md ## 3 · Silver — Metrics + Window Functions (batch over Bronze)

# COMMAND ----------
from pyspark.sql.window import Window

bronze = spark.read.format("delta").load(f"{DELTA_BASE}/bronze/raw_ad_events")

silver = (
    bronze
    .withColumn("date", F.to_date("date"))
    .withColumn("ctr",  F.when(F.col("impressions") > 0,
                               F.col("clicks") / F.col("impressions")).otherwise(0.0))
    .withColumn("cpc",  F.when(F.col("clicks") > 0,
                               F.col("spend_usd") / F.col("clicks")).otherwise(0.0))
    .withColumn("roas", F.when(F.col("spend_usd") > 0,
                               F.col("revenue_usd") / F.col("spend_usd")).otherwise(0.0))
)

# Window: 7-day rolling avg per campaign
w7 = (Window.partitionBy("campaign_id")
            .orderBy(F.col("date").cast("timestamp").cast("long"))
            .rangeBetween(-6 * 86400, 0))

# Window: running total spend per campaign
w_run = (Window.partitionBy("campaign_id")
               .orderBy(F.col("date").cast("timestamp").cast("long"))
               .rowsBetween(Window.unboundedPreceding, 0))

# Window: rank by ROAS within channel × week
w_rank = Window.partitionBy("channel", F.weekofyear("date")).orderBy(F.col("roas").desc())

silver_w = (
    silver
    .withColumn("rolling_7d_impressions", F.avg("impressions").over(w7))
    .withColumn("rolling_7d_clicks",      F.avg("clicks").over(w7))
    .withColumn("rolling_7d_spend",       F.avg("spend_usd").over(w7))
    .withColumn("running_total_spend",    F.sum("spend_usd").over(w_run))
    .withColumn("roas_rank_in_channel",   F.rank().over(w_rank))
)

(silver_w.write.format("delta").mode("overwrite")
          .partitionBy("platform", "date")
          .save(f"{DELTA_BASE}/silver/ad_metrics"))

spark.sql(f"CREATE TABLE IF NOT EXISTS silver.ad_metrics USING DELTA LOCATION '{DELTA_BASE}/silver/ad_metrics'")
print("Silver done")

# COMMAND ----------
# MAGIC %md ## 4 · Gold — Star Schema

# COMMAND ----------
# dim_campaign
dim_campaign = (silver_w.select("campaign_id","campaign_name","campaign_type","advertiser")
                         .distinct()
                         .withColumn("campaign_key", F.monotonically_increasing_id()))
dim_campaign.write.format("delta").mode("overwrite").save(f"{DELTA_BASE}/gold/dim_campaign")

# dim_channel
dim_channel = (
    silver_w.select("channel","platform").distinct()
            .withColumnRenamed("channel","channel_name")
            .withColumn("channel_type",
                        F.when(F.col("channel_name").contains("Search"), "search")
                         .when(F.col("channel_name").contains("Display"),"display")
                         .when(F.col("channel_name").contains("Feed"),   "social_feed")
                         .otherwise("social_story"))
            .withColumn("channel_key", F.monotonically_increasing_id())
)
dim_channel.write.format("delta").mode("overwrite").save(f"{DELTA_BASE}/gold/dim_channel")

# dim_date
dim_date = (
    silver_w.select("date").distinct()
            .withColumn("year",        F.year("date"))
            .withColumn("quarter",     F.quarter("date"))
            .withColumn("month",       F.month("date"))
            .withColumn("week",        F.weekofyear("date"))
            .withColumn("day",         F.dayofmonth("date"))
            .withColumn("day_of_week", F.date_format("date","EEEE"))
            .withColumn("is_weekend",  F.dayofweek("date").isin([1,7]))
            .withColumn("date_key",    F.date_format("date","yyyyMMdd").cast("int"))
            .withColumnRenamed("date","full_date")
)
dim_date.write.format("delta").mode("overwrite").save(f"{DELTA_BASE}/gold/dim_date")

# fact_ad_impressions
fact = (
    silver_w
    .join(dim_campaign.select("campaign_id","campaign_key"), on="campaign_id")
    .join(dim_channel.select(F.col("channel_name").alias("channel"),"channel_key"), on="channel")
    .join(dim_date.select(F.col("full_date").alias("date"),"date_key"), on="date")
    .select(
        "event_id","campaign_key","date_key","channel_key",
        "impressions","clicks","conversions","spend_usd","revenue_usd",
        F.round("ctr",4).alias("ctr"), F.round("cpc",4).alias("cpc"),
        F.round("roas",4).alias("roas"),
        F.round("rolling_7d_impressions",1).alias("rolling_7d_impressions"),
        F.round("rolling_7d_spend",2).alias("rolling_7d_spend"),
        "running_total_spend","roas_rank_in_channel",
    )
)
(fact.write.format("delta").mode("overwrite")
          .partitionBy("date_key")
          .save(f"{DELTA_BASE}/gold/fact_ad_impressions"))

print(f"fact rows: {fact.count():,}")
display(fact.limit(5))

# COMMAND ----------
# MAGIC %md ## 5 · Validation

# COMMAND ----------
spark.sql(f"""
SELECT dc.campaign_name, dch.channel_name,
       SUM(f.impressions) AS impressions,
       ROUND(SUM(f.clicks)*100.0/NULLIF(SUM(f.impressions),0),2) AS ctr_pct,
       ROUND(SUM(f.revenue_usd)/NULLIF(SUM(f.spend_usd),0),2)    AS roas,
       SUM(f.spend_usd)   AS total_spend
FROM   delta.`{DELTA_BASE}/gold/fact_ad_impressions` f
JOIN   delta.`{DELTA_BASE}/gold/dim_campaign`        dc  ON f.campaign_key = dc.campaign_key
JOIN   delta.`{DELTA_BASE}/gold/dim_channel`         dch ON f.channel_key  = dch.channel_key
GROUP BY dc.campaign_name, dch.channel_name
ORDER BY total_spend DESC
""").display()

# COMMAND ----------
# MAGIC %md ## 6 · Delta Optimization

# COMMAND ----------
spark.sql(f"OPTIMIZE delta.`{DELTA_BASE}/gold/fact_ad_impressions` ZORDER BY (campaign_key, date_key)")
spark.sql(f"VACUUM  delta.`{DELTA_BASE}/gold/fact_ad_impressions` RETAIN 168 HOURS")
print("Optimized.")
