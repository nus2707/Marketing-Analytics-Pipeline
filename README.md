(https://github.com/user-attachments/files/29088653/README.md)
# Ads Analytics Pipeline

End-to-end data engineering pipeline that ingests public **Google Ads** and **Meta Ads** benchmark data, processes it with **PySpark on Databricks**, stores it in a **Delta Lake star schema**, and surfaces insights through a **Power BI dashboard**.

---

## Architecture

<img width="2544" height="1072" alt="architecture" src="https://github.com/user-attachments/assets/c95f261a-be71-4490-ad9f-4316fb945008" />


### Data Flow

```
Google Ads / Meta Ads  ──►  Kafka / Azure Event Hubs  ──►  Databricks PySpark
  (public benchmarks)           topic: ad-impressions         Bronze → Silver → Gold
                                                                     │
                                                              Delta Lake (ADLS Gen2)
                                                                     │
                                                              Power BI Dashboard
```

| Layer | Technology | Description |
|---|---|---|
| **Sources** | Google Ads, Meta Ads | Synthetic benchmark data (CTR/CPC/ROAS industry averages) |
| **Ingestion** | Apache Kafka / Azure Event Hubs | Streams `RawAdEvent` JSON to `ad-impressions` topic |
| **Bronze** | Delta Lake | Raw events persisted as-is, schema enforced |
| **Silver** | PySpark + Delta Lake | Derived metrics (CTR, CPC, ROAS) + window functions |
| **Gold** | Delta Lake star schema | `fact_ad_impressions` joined to 3 dimension tables |
| **Visualization** | Power BI / matplotlib | KPI cards, trend lines, funnel, channel mix |

---

## Power BI Dashboard Preview

<img width="3026" height="2008" alt="dashboard" src="https://github.com/user-attachments/assets/a7e7066d-9aa6-4a7a-850f-eb6cb97cc28b" />


### Report Pages

| Page | Visuals |
|---|---|
| **Overview** | KPI cards — Impressions, Clicks, Spend, ROAS with MoM delta |
| **Campaign Performance** | 7-day rolling spend trend · ROAS by campaign bar · Monthly ROAS lines |
| **Channel Mix** | Weekly impressions stacked bar · CTR% vs CPC scatter · Google vs Meta donut · Conversion funnel |

---

## Star Schema

```
                    ┌─────────────────┐
                    │  dim_campaign   │
                    │─────────────────│
                    │ campaign_key PK │
                    │ campaign_id     │
                    │ campaign_name   │
                    │ campaign_type   │
                    │ advertiser      │
                    └────────┬────────┘
                             │
┌──────────────┐    ┌────────▼──────────────┐    ┌─────────────┐
│  dim_date    │    │  fact_ad_impressions  │    │ dim_channel │
│──────────────│    │───────────────────────│    │─────────────│
│ date_key  PK │◄───│ campaign_key     FK   │───►│ channel_key │
│ full_date    │    │ date_key         FK   │    │ channel_name│
│ year/quarter │    │ channel_key      FK   │    │ platform    │
│ month/week   │    │ impressions           │    │ channel_type│
│ day_of_week  │    │ clicks                │    └─────────────┘
│ is_weekend   │    │ conversions           │
└──────────────┘    │ spend_usd             │
                    │ revenue_usd           │
                    │ ctr / cpc / roas      │
                    │ rolling_7d_impressions│
                    │ rolling_7d_spend      │
                    │ running_total_spend   │
                    │ roas_rank_in_channel  │
                    └───────────────────────┘
```

---

## PySpark Window Functions (Silver Layer)

```python
# 7-day rolling average per campaign
w7 = Window.partitionBy("campaign_id") \
           .orderBy(col("date").cast("timestamp").cast("long")) \
           .rangeBetween(-6 * 86400, 0)

# Running total spend per campaign
w_run = Window.partitionBy("campaign_id") \
              .orderBy(col("date").cast("timestamp").cast("long")) \
              .rowsBetween(Window.unboundedPreceding, 0)

# ROAS rank within channel × week
w_rank = Window.partitionBy("channel", weekofyear("date")) \
               .orderBy(col("roas").desc())
```

---

## Project Structure

```
AdsPipeline/
├── ads_pipeline/
│   ├── schemas.py              # Pydantic models + star-schema dataclasses
│   ├── data_generator.py       # Synthetic Google / Meta benchmark events
│   ├── kafka_ads_producer.py   # Kafka producer (works with Azure Event Hubs)
│   ├── spark_processor.py      # PySpark: Bronze → Silver → Gold
│   ├── power_bi_export.py      # CSV export + DAX measures + preview chart
│   └── pipeline.py             # End-to-end orchestrator
├── notebooks/
│   └── ads_pipeline_databricks.py   # Databricks notebook (Event Hubs + ADLS Gen2)
├── scripts/
│   └── generate_assets.py      # Re-generates docs/architecture.png + docs/dashboard.png
├── docs/
│   ├── architecture.png
│   └── dashboard.png
├── power_bi_exports/           # CSVs + DAX_measures.txt (generated at runtime)
├── docker-compose.yml          # Kafka + Zookeeper + Kafka-UI
└── requirements.txt
```

---

## Quick Start

### Option A — Local only (no Java/Spark required)

```bash
git clone <repo-url>
cd AdsPipeline

python -m venv venv
# Windows
.\venv\Scripts\activate
# macOS / Linux
source venv/bin/activate

pip install pandas matplotlib pydantic

python -m ads_pipeline.pipeline --local-only
```

Outputs:
- `power_bi_exports/` — 6 CSVs ready for Power BI import
- `power_bi_exports/DAX_measures.txt` — paste-ready KPI measures
- `power_bi_exports/dashboard_preview.png` — local preview

### Option B — Full pipeline (Kafka + PySpark)

**Prerequisites:** Docker Desktop, Java 11+

```bash
# 1. Start Kafka
docker compose up -d

# 2. Install all dependencies
pip install -r requirements.txt

# 3. Run full pipeline
python -m ads_pipeline.pipeline
```

Kafka UI is available at [http://localhost:8080](http://localhost:8080).

### Option C — Databricks (production)

1. Upload `ads_pipeline/` to `/Workspace/Repos/AdsPipeline/`
2. Store your Event Hubs connection string in Databricks Secrets:
   ```bash
   databricks secrets put --scope ads-pipeline --key eventhub-conn-str
   ```
3. Open `notebooks/ads_pipeline_databricks.py`, update `KAFKA_BOOTSTRAP` and `DELTA_BASE`
4. Run cells sequentially on a Runtime 14.x LTS cluster

---

## Power BI Connection

1. Open **Power BI Desktop**
2. **Get Data → Text/CSV** → import all files from `power_bi_exports/`
3. In **Model view**, create relationships:

| From | To | Key |
|---|---|---|
| `fact_ad_impressions.campaign_key` | `dim_campaign.campaign_key` | Many-to-one |
| `fact_ad_impressions.date_key` | `dim_date.date_key` | Many-to-one |
| `fact_ad_impressions.channel_key` | `dim_channel.channel_key` | Many-to-one |

4. Create a new table called **Measures**, paste contents of `DAX_measures.txt`

For **live Delta Lake connection** from Databricks, use  
**Get Data → Azure Databricks → Delta Lake** and point to your Gold tables.

---

## DAX Measures (excerpt)

```dax
ROAS =
    DIVIDE(SUM(fact_ad_impressions[revenue_usd]),
           SUM(fact_ad_impressions[spend_usd]), 0)

CTR =
    DIVIDE(SUM(fact_ad_impressions[clicks]),
           SUM(fact_ad_impressions[impressions]), 0)

Spend MoM % =
    VAR cur = [Total Spend]
    VAR prv = CALCULATE([Total Spend], DATEADD(dim_date[full_date], -1, MONTH))
    RETURN DIVIDE(cur - prv, prv, BLANK())

Impressions 7D Avg =
    AVERAGEX(
        DATESINPERIOD(dim_date[full_date], LASTDATE(dim_date[full_date]), -7, DAY),
        CALCULATE(SUM(fact_ad_impressions[impressions]))
    )
```

Full measures file: [`power_bi_exports/DAX_measures.txt`](power_bi_exports/DAX_measures.txt)

---

## Dataset

Synthetic data generated from publicly available industry benchmarks:

| Channel | CTR Range | CPC Range | ROAS Range |
|---|---|---|---|
| Google Search | 2.0 – 5.0% | $1.00 – $4.00 | 2.0 – 5.0× |
| Google Display | 0.1 – 0.5% | $0.50 – $2.00 | 1.0 – 3.0× |
| Meta Feed | 0.9 – 1.5% | $0.50 – $1.50 | 1.5 – 4.0× |
| Meta Stories | 0.5 – 1.0% | $0.30 – $1.00 | 1.2 – 3.5× |

Sources: WordStream Google Ads benchmarks, Meta Business benchmark reports.

**Coverage:** Jan 2024 – Jun 2024 · 5 campaigns · 4 channels · ~2,300 daily events

---

## Tech Stack

| Component | Technology |
|---|---|
| Streaming | Apache Kafka · Azure Event Hubs (Kafka protocol) |
| Processing | Databricks · PySpark 3.5 · Delta Spark 3.1 |
| Storage | Delta Lake · ADLS Gen2 (prod) / local filesystem (dev) |
| Orchestration | `pipeline.py` (local) · Databricks Jobs (prod) |
| Visualization | Power BI Desktop · matplotlib (preview) |
| Language | Python 3.12 · PySpark · DAX |
