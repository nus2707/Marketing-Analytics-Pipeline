"""
PySpark processor — mirrors a Databricks notebook job.

Reads raw ad events from a local JSON file (batch) or Kafka (stream).

Layers written to Delta Lake:
  delta_lake/bronze/raw_ad_events
  delta_lake/silver/ad_metrics          (derived metrics + window functions)
  delta_lake/gold/dim_campaign
  delta_lake/gold/dim_date
  delta_lake/gold/dim_channel
  delta_lake/gold/fact_ad_impressions

Window functions applied in Silver:
  • 7-day rolling avg impressions / clicks / spend  per campaign
  • Running total spend per campaign
  • ROAS rank within channel × week

Usage:
    python -m ads_pipeline.spark_processor --mode batch --input data/ads_raw.json
    python -m ads_pipeline.spark_processor --mode stream
"""
from __future__ import annotations
import argparse
import logging

logger = logging.getLogger(__name__)

DELTA_BASE      = "delta_lake"
KAFKA_SERVERS   = "localhost:9092"
KAFKA_TOPIC     = "ad-impressions"


def _get_spark():
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip

        builder = (
            SparkSession.builder
            .appName("AdsPipeline")
            .master("local[*]")
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()
    except ImportError as e:
        raise RuntimeError(
            "PySpark + delta-spark required.\n"
            "  pip install pyspark==3.5.0 delta-spark==3.1.0\n"
            "  Java 11+ must be on PATH."
        ) from e


def _raw_schema():
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, DoubleType,
    )
    return StructType([
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


# ── Batch ───────────────────────────────────────────────────────────────────

def run_batch(spark, input_path: str) -> None:
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    raw = spark.read.schema(_raw_schema()).json(input_path)

    # Bronze
    bronze_path = f"{DELTA_BASE}/bronze/raw_ad_events"
    raw.write.format("delta").mode("overwrite").save(bronze_path)
    logger.info("Bronze → %s", bronze_path)

    # Silver: metrics + window functions
    silver = (
        raw
        .withColumn("date", F.to_date("date"))
        .withColumn("ctr",  F.when(F.col("impressions") > 0,
                                   F.col("clicks") / F.col("impressions")).otherwise(0.0))
        .withColumn("cpc",  F.when(F.col("clicks") > 0,
                                   F.col("spend_usd") / F.col("clicks")).otherwise(0.0))
        .withColumn("roas", F.when(F.col("spend_usd") > 0,
                                   F.col("revenue_usd") / F.col("spend_usd")).otherwise(0.0))
        .withColumn("conversion_rate",
                    F.when(F.col("clicks") > 0,
                           F.col("conversions") / F.col("clicks")).otherwise(0.0))
    )

    w7 = (
        Window.partitionBy("campaign_id")
              .orderBy(F.col("date").cast("timestamp").cast("long"))
              .rangeBetween(-6 * 86400, 0)
    )
    w_run = (
        Window.partitionBy("campaign_id")
              .orderBy(F.col("date").cast("timestamp").cast("long"))
              .rowsBetween(Window.unboundedPreceding, 0)
    )
    w_rank = Window.partitionBy("channel", F.weekofyear("date")).orderBy(F.col("roas").desc())

    silver_w = (
        silver
        .withColumn("rolling_7d_impressions", F.avg("impressions").over(w7))
        .withColumn("rolling_7d_clicks",      F.avg("clicks").over(w7))
        .withColumn("rolling_7d_spend",       F.avg("spend_usd").over(w7))
        .withColumn("running_total_spend",    F.sum("spend_usd").over(w_run))
        .withColumn("roas_rank_in_channel",   F.rank().over(w_rank))
    )

    silver_path = f"{DELTA_BASE}/silver/ad_metrics"
    (silver_w.write.format("delta").mode("overwrite")
             .partitionBy("platform", "date").save(silver_path))
    logger.info("Silver → %s", silver_path)

    _write_gold(spark, silver_w)


def _write_gold(spark, df) -> None:
    from pyspark.sql import functions as F

    # dim_campaign
    dim_campaign = (
        df.select("campaign_id", "campaign_name", "campaign_type", "advertiser")
          .distinct()
          .withColumn("campaign_key", F.monotonically_increasing_id())
    )
    dim_campaign.write.format("delta").mode("overwrite").save(f"{DELTA_BASE}/gold/dim_campaign")

    # dim_channel
    dim_channel = (
        df.select("channel", "platform").distinct()
          .withColumnRenamed("channel", "channel_name")
          .withColumn("channel_type",
                      F.when(F.col("channel_name").contains("Search"),  "search")
                       .when(F.col("channel_name").contains("Display"), "display")
                       .when(F.col("channel_name").contains("Feed"),    "social_feed")
                       .otherwise("social_story"))
          .withColumn("channel_key", F.monotonically_increasing_id())
    )
    dim_channel.write.format("delta").mode("overwrite").save(f"{DELTA_BASE}/gold/dim_channel")

    # dim_date
    dim_date = (
        df.select("date").distinct()
          .withColumn("year",        F.year("date"))
          .withColumn("quarter",     F.quarter("date"))
          .withColumn("month",       F.month("date"))
          .withColumn("week",        F.weekofyear("date"))
          .withColumn("day",         F.dayofmonth("date"))
          .withColumn("day_of_week", F.date_format("date", "EEEE"))
          .withColumn("is_weekend",  F.dayofweek("date").isin([1, 7]))
          .withColumn("date_key",    F.date_format("date", "yyyyMMdd").cast("int"))
          .withColumnRenamed("date", "full_date")
    )
    dim_date.write.format("delta").mode("overwrite").save(f"{DELTA_BASE}/gold/dim_date")

    # fact_ad_impressions — join surrogate keys
    fact = (
        df
        .join(dim_campaign.select("campaign_id", "campaign_key"), on="campaign_id")
        .join(dim_channel.select(F.col("channel_name").alias("channel"), "channel_key"), on="channel")
        .join(dim_date.select(F.col("full_date").alias("date"), "date_key"), on="date")
        .select(
            "event_id", "campaign_key", "date_key", "channel_key",
            "impressions", "clicks", "conversions",
            "spend_usd", "revenue_usd",
            F.round("ctr",  4).alias("ctr"),
            F.round("cpc",  4).alias("cpc"),
            F.round("roas", 4).alias("roas"),
            F.round("rolling_7d_impressions", 1).alias("rolling_7d_impressions"),
            F.round("rolling_7d_spend",       2).alias("rolling_7d_spend"),
            "running_total_spend",
            "roas_rank_in_channel",
        )
    )
    (fact.write.format("delta").mode("overwrite")
          .partitionBy("date_key").save(f"{DELTA_BASE}/gold/fact_ad_impressions"))
    logger.info("Gold layer written — fact rows: %d", fact.count())


# ── Streaming ───────────────────────────────────────────────────────────────

def run_stream(spark) -> None:
    from pyspark.sql import functions as F

    raw_stream = (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", KAFKA_SERVERS)
             .option("subscribe", KAFKA_TOPIC)
             .option("startingOffsets", "earliest")
             .option("failOnDataLoss", "false")
             .load()
    )

    parsed = (
        raw_stream
        .select(F.from_json(F.col("value").cast("string"), _raw_schema()).alias("d"))
        .select("d.*")
        .withColumn("date",  F.to_date("date"))
        .withColumn("ctr",   F.when(F.col("impressions") > 0,
                                    F.col("clicks") / F.col("impressions")).otherwise(0.0))
        .withColumn("cpc",   F.when(F.col("clicks") > 0,
                                    F.col("spend_usd") / F.col("clicks")).otherwise(0.0))
        .withColumn("roas",  F.when(F.col("spend_usd") > 0,
                                    F.col("revenue_usd") / F.col("spend_usd")).otherwise(0.0))
    )

    query = (
        parsed.writeStream
              .format("delta")
              .outputMode("append")
              .option("checkpointLocation", f"{DELTA_BASE}/_checkpoints/ad_impressions")
              .partitionBy("platform")
              .start(f"{DELTA_BASE}/silver/ad_metrics_stream")
    )
    logger.info("Streaming query running — awaiting termination.")
    query.awaitTermination()


# ── Aggregation helpers ──────────────────────────────────────────────────────

def campaign_daily_summary(spark):
    from pyspark.sql import functions as F
    df = spark.read.format("delta").load(f"{DELTA_BASE}/silver/ad_metrics")
    return (
        df.groupBy("campaign_id", "campaign_name", "campaign_type", "advertiser", "date")
          .agg(
              F.sum("impressions").alias("total_impressions"),
              F.sum("clicks").alias("total_clicks"),
              F.sum("conversions").alias("total_conversions"),
              F.sum("spend_usd").alias("total_spend"),
              F.sum("revenue_usd").alias("total_revenue"),
              F.avg("ctr").alias("avg_ctr"),
              F.avg("roas").alias("avg_roas"),
          )
          .orderBy("campaign_id", "date")
    )


def channel_weekly_summary(spark):
    from pyspark.sql import functions as F
    df = spark.read.format("delta").load(f"{DELTA_BASE}/silver/ad_metrics")
    return (
        df.withColumn("week", F.weekofyear("date"))
          .withColumn("year", F.year("date"))
          .groupBy("channel", "platform", "year", "week")
          .agg(
              F.sum("impressions").alias("total_impressions"),
              F.sum("clicks").alias("total_clicks"),
              F.sum("spend_usd").alias("total_spend"),
              F.sum("revenue_usd").alias("total_revenue"),
              F.avg("roas").alias("avg_roas"),
              F.avg("ctr").alias("avg_ctr"),
          )
          .orderBy("year", "week", "channel")
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["batch", "stream"], default="batch")
    parser.add_argument("--input", default="data/ads_raw.json")
    args = parser.parse_args()

    spark = _get_spark()
    spark.sparkContext.setLogLevel("WARN")

    if args.mode == "batch":
        run_batch(spark, args.input)
    else:
        run_stream(spark)
