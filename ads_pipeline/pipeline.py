"""
End-to-end pipeline orchestrator.

Steps:
  1. Generate synthetic Google / Meta ad events
  2. Produce to Kafka topic 'ad-impressions'  (skipped with --local-only)
  3. PySpark: Bronze → Silver → Gold Delta Lake  (skipped with --local-only)
  4. Export star-schema CSVs + DAX measures for Power BI
  5. Render dashboard preview chart

Usage:
    # Full run (needs Kafka + Java 11 + PySpark)
    python -m ads_pipeline.pipeline

    # Local only — no Kafka, no Spark; pandas does everything
    python -m ads_pipeline.pipeline --local-only

    # Skip chart
    python -m ads_pipeline.pipeline --local-only --no-chart
"""
from __future__ import annotations
import argparse
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
RAW_DATA_PATH = "data/ads_raw.json"


def step_generate() -> None:
    from ads_pipeline.data_generator import generate_events
    Path("data").mkdir(exist_ok=True)
    events = list(generate_events())
    with open(RAW_DATA_PATH, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    logger.info("Step 1 ✓  %d events → %s", len(events), RAW_DATA_PATH)


def step_kafka() -> None:
    from ads_pipeline.kafka_ads_producer import produce_events
    logger.info("Step 2 — Kafka ingestion...")
    produce_events(batch_mode=True)
    logger.info("Step 2 ✓  Kafka done.")


def step_spark() -> None:
    from ads_pipeline.spark_processor import _get_spark, run_batch
    logger.info("Step 3 — PySpark + Delta Lake...")
    spark = _get_spark()
    spark.sparkContext.setLogLevel("WARN")
    run_batch(spark, RAW_DATA_PATH)
    logger.info("Step 3 ✓  Delta Lake Gold layer ready.")


def step_export(render_chart: bool = True) -> None:
    from ads_pipeline.power_bi_export import (
        export_csvs, write_dax_measures, render_dashboard_preview,
    )
    logger.info("Step 4 — Power BI export...")
    export_csvs()
    write_dax_measures()
    if render_chart:
        render_dashboard_preview()
    logger.info("Step 4 ✓  Exports in ./power_bi_exports/")


def run(local_only: bool = False, no_chart: bool = False) -> None:
    step_generate()
    if not local_only:
        step_kafka()
        step_spark()
    step_export(render_chart=not no_chart)

    print("\n" + "=" * 58)
    print("  Pipeline complete!")
    print("  Exports   -> ./power_bi_exports/")
    if not no_chart:
        print("  Preview   -> ./power_bi_exports/dashboard_preview.png")
    print("=" * 58 + "\n")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--local-only", action="store_true",
                        help="Skip Kafka + Spark (pandas only, no Java)")
    parser.add_argument("--no-chart", action="store_true")
    args = parser.parse_args()
    run(local_only=args.local_only, no_chart=args.no_chart)
