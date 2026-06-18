"""
Power BI Export layer.

1. Reads Gold Delta tables (PySpark) or regenerates from pandas if Spark unavailable.
2. Exports CSVs to ./power_bi_exports/ — import via Power BI Desktop "Get Data → Text/CSV".
3. Writes DAX_measures.txt with ready-to-paste KPI measures.
4. Renders a dashboard preview (matplotlib) matching three Power BI report pages.

Usage:
    python -m ads_pipeline.power_bi_export
    python -m ads_pipeline.power_bi_export --no-chart
"""
from __future__ import annotations
import argparse
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
EXPORT_DIR = Path("power_bi_exports")
DELTA_BASE = "delta_lake"


# ── 1. CSV Export ────────────────────────────────────────────────────────────

def export_csvs() -> None:
    EXPORT_DIR.mkdir(exist_ok=True)
    try:
        _export_via_spark()
    except Exception as e:
        logger.warning("PySpark unavailable (%s) — using pandas fallback", e)
        _export_via_pandas()


def _export_via_spark() -> None:
    from ads_pipeline.spark_processor import (
        _get_spark, campaign_daily_summary, channel_weekly_summary,
    )
    spark = _get_spark()
    spark.sparkContext.setLogLevel("WARN")
    for name, path in {
        "dim_campaign":        f"{DELTA_BASE}/gold/dim_campaign",
        "dim_date":            f"{DELTA_BASE}/gold/dim_date",
        "dim_channel":         f"{DELTA_BASE}/gold/dim_channel",
        "fact_ad_impressions": f"{DELTA_BASE}/gold/fact_ad_impressions",
    }.items():
        df = spark.read.format("delta").load(path)
        df.toPandas().to_csv(EXPORT_DIR / f"{name}.csv", index=False)
        logger.info("Exported %s", name)
    campaign_daily_summary(spark).toPandas().to_csv(
        EXPORT_DIR / "campaign_daily_summary.csv", index=False)
    channel_weekly_summary(spark).toPandas().to_csv(
        EXPORT_DIR / "channel_weekly_summary.csv", index=False)


def _export_via_pandas() -> None:
    import pandas as pd
    from ads_pipeline.data_generator import generate_events

    logger.info("Building star schema with pandas...")
    rows = list(generate_events())
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df["ctr"]  = (df["clicks"] / df["impressions"].replace(0, float("nan"))).fillna(0)
    df["cpc"]  = (df["spend_usd"] / df["clicks"].replace(0, float("nan"))).fillna(0)
    df["roas"] = (df["revenue_usd"] / df["spend_usd"].replace(0, float("nan"))).fillna(0)

    # dim_campaign
    dim_campaign = (
        df[["campaign_id", "campaign_name", "campaign_type", "advertiser"]]
        .drop_duplicates().reset_index(drop=True)
    )
    dim_campaign.index.name = "campaign_key"
    dim_campaign = dim_campaign.reset_index()

    # dim_channel
    dim_channel = (
        df[["channel", "platform"]].rename(columns={"channel": "channel_name"})
        .drop_duplicates().reset_index(drop=True)
    )
    dim_channel.index.name = "channel_key"
    dim_channel = dim_channel.reset_index()
    dim_channel["channel_type"] = dim_channel["channel_name"].map({
        "Google Search": "search", "Google Display": "display",
        "Meta Feed": "social_feed", "Meta Stories": "social_story",
    })

    # dim_date
    dates = pd.DataFrame({"full_date": pd.to_datetime(df["date"].dt.date.unique())})
    dates["date_key"]    = dates["full_date"].dt.strftime("%Y%m%d").astype(int)
    dates["year"]        = dates["full_date"].dt.year
    dates["quarter"]     = dates["full_date"].dt.quarter
    dates["month"]       = dates["full_date"].dt.month
    dates["week"]        = dates["full_date"].dt.isocalendar().week.astype(int)
    dates["day"]         = dates["full_date"].dt.day
    dates["day_of_week"] = dates["full_date"].dt.day_name()
    dates["is_weekend"]  = dates["full_date"].dt.dayofweek >= 5

    # fact
    fact = (
        df.merge(dim_campaign[["campaign_key", "campaign_id"]], on="campaign_id")
          .merge(dim_channel[["channel_key", "channel_name"]],
                 left_on="channel", right_on="channel_name")
    )
    fact["date_str"] = fact["date"].dt.date.astype(str)
    dates["date_str"] = dates["full_date"].dt.date.astype(str)
    fact = fact.merge(dates[["date_key", "date_str"]], on="date_str")
    fact = fact[[
        "event_id", "campaign_key", "date_key", "channel_key",
        "impressions", "clicks", "conversions",
        "spend_usd", "revenue_usd", "ctr", "cpc", "roas",
    ]]

    # summaries
    campaign_daily = (
        df.groupby(["campaign_id", "campaign_name", "campaign_type", "advertiser", "date"])
          .agg(total_impressions=("impressions","sum"), total_clicks=("clicks","sum"),
               total_conversions=("conversions","sum"), total_spend=("spend_usd","sum"),
               total_revenue=("revenue_usd","sum"), avg_ctr=("ctr","mean"), avg_roas=("roas","mean"))
          .reset_index()
    )
    df["week"] = df["date"].dt.isocalendar().week.astype(int)
    df["year"] = df["date"].dt.year
    channel_weekly = (
        df.groupby(["channel", "platform", "year", "week"])
          .agg(total_impressions=("impressions","sum"), total_clicks=("clicks","sum"),
               total_spend=("spend_usd","sum"), total_revenue=("revenue_usd","sum"),
               avg_roas=("roas","mean"), avg_ctr=("ctr","mean"))
          .reset_index()
    )

    for name, tbl in {
        "dim_campaign": dim_campaign, "dim_date": dates, "dim_channel": dim_channel,
        "fact_ad_impressions": fact, "campaign_daily_summary": campaign_daily,
        "channel_weekly_summary": channel_weekly,
    }.items():
        out = EXPORT_DIR / f"{name}.csv"
        tbl.to_csv(out, index=False)
        logger.info("Exported %s (%d rows) → %s", name, len(tbl), out)


# ── 2. DAX Measures ──────────────────────────────────────────────────────────

DAX = """\
// ─────────────────────────────────────────────────────────────────
// Power BI DAX Measures — Ads Analytics Dashboard
// Paste into a dedicated "Measures" table in Power BI Desktop
// ─────────────────────────────────────────────────────────────────

Total Impressions = SUM(fact_ad_impressions[impressions])
Total Clicks      = SUM(fact_ad_impressions[clicks])
Total Conversions = SUM(fact_ad_impressions[conversions])
Total Spend       = SUM(fact_ad_impressions[spend_usd])
Total Revenue     = SUM(fact_ad_impressions[revenue_usd])

CTR =
    DIVIDE(SUM(fact_ad_impressions[clicks]),
           SUM(fact_ad_impressions[impressions]), 0)

CPC =
    DIVIDE(SUM(fact_ad_impressions[spend_usd]),
           SUM(fact_ad_impressions[clicks]), 0)

Conversion Rate =
    DIVIDE(SUM(fact_ad_impressions[conversions]),
           SUM(fact_ad_impressions[clicks]), 0)

ROAS =
    DIVIDE(SUM(fact_ad_impressions[revenue_usd]),
           SUM(fact_ad_impressions[spend_usd]), 0)

Spend MoM % =
    VAR cur = [Total Spend]
    VAR prv = CALCULATE([Total Spend], DATEADD(dim_date[full_date], -1, MONTH))
    RETURN DIVIDE(cur - prv, prv, BLANK())

Impressions 7D Avg =
    AVERAGEX(
        DATESINPERIOD(dim_date[full_date], LASTDATE(dim_date[full_date]), -7, DAY),
        CALCULATE(SUM(fact_ad_impressions[impressions]))
    )

Cost per Conversion =
    DIVIDE([Total Spend], [Total Conversions], 0)

Revenue per Click =
    DIVIDE([Total Revenue], [Total Clicks], 0)
"""


def write_dax_measures() -> None:
    out = EXPORT_DIR / "DAX_measures.txt"
    out.write_text(DAX, encoding="utf-8")
    logger.info("DAX measures → %s", out)


# ── 3. Dashboard preview ─────────────────────────────────────────────────────

def render_dashboard_preview() -> None:
    import pandas as pd
    import matplotlib.pyplot as plt
    import matplotlib.gridspec as gridspec
    from matplotlib.ticker import FuncFormatter

    campaign_daily = pd.read_csv(EXPORT_DIR / "campaign_daily_summary.csv", parse_dates=["date"])
    channel_weekly = pd.read_csv(EXPORT_DIR / "channel_weekly_summary.csv")

    PALETTE    = ["#7c3aed", "#2563eb", "#059669", "#d97706", "#dc2626"]
    TEXT_COLOR = "#e2e8f0"
    GRID_COLOR = "#334155"
    BG_COLOR   = "#0f172a"

    fig = plt.figure(figsize=(18, 11), facecolor="#1e1e2e")
    fig.suptitle("Ads Analytics Dashboard  ·  Power BI Preview",
                 color="white", fontsize=15, fontweight="bold", y=0.98)
    gs = gridspec.GridSpec(2, 3, figure=fig, hspace=0.45, wspace=0.35)

    def _style(ax):
        ax.set_facecolor(BG_COLOR)
        ax.tick_params(colors=TEXT_COLOR, labelsize=8)
        ax.xaxis.label.set_color(TEXT_COLOR)
        ax.yaxis.label.set_color(TEXT_COLOR)
        ax.title.set_color(TEXT_COLOR)
        for sp in ax.spines.values():
            sp.set_edgecolor(GRID_COLOR)
        ax.yaxis.grid(True, color=GRID_COLOR, linewidth=0.5, linestyle="--")
        ax.set_axisbelow(True)

    # Daily spend by campaign (line)
    ax1 = fig.add_subplot(gs[0, :2])
    for i, (cid, grp) in enumerate(campaign_daily.groupby("campaign_id")):
        daily = grp.groupby("date")["total_spend"].sum()
        ax1.plot(daily.index, daily.values,
                 label=grp["campaign_name"].iloc[0],
                 color=PALETTE[i % len(PALETTE)], linewidth=1.6)
    ax1.set_title("Daily Spend by Campaign (USD)")
    ax1.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"${x:,.0f}"))
    ax1.legend(fontsize=7, labelcolor=TEXT_COLOR,
               facecolor=BG_COLOR, edgecolor=GRID_COLOR, loc="upper left")
    _style(ax1)

    # ROAS by campaign (horizontal bar)
    ax2 = fig.add_subplot(gs[0, 2])
    roas = (
        campaign_daily.groupby("campaign_name")
                      .apply(lambda g: g["total_revenue"].sum() / max(g["total_spend"].sum(), 1e-9))
                      .sort_values()
    )
    bars = ax2.barh(roas.index, roas.values, color=PALETTE[:len(roas)])
    ax2.set_title("ROAS by Campaign")
    ax2.axvline(x=1, color="#64748b", linewidth=1, linestyle="--")
    for bar, val in zip(bars, roas.values):
        ax2.text(val + 0.05, bar.get_y() + bar.get_height() / 2,
                 f"{val:.2f}×", va="center", color=TEXT_COLOR, fontsize=7)
    _style(ax2)
    ax2.tick_params(axis="y", labelsize=6)

    # Weekly impressions by channel (stacked bar)
    ax3 = fig.add_subplot(gs[1, :2])
    pivot = channel_weekly.pivot_table(
        index="week", columns="channel", values="total_impressions", aggfunc="sum"
    ).fillna(0)
    bottom = None
    for i, col in enumerate(pivot.columns):
        vals = pivot[col].values
        ax3.bar(pivot.index, vals,
                bottom=0 if bottom is None else bottom,
                label=col, color=PALETTE[i % len(PALETTE)], alpha=0.85, width=0.7)
        bottom = vals if bottom is None else bottom + vals
    ax3.set_title("Weekly Impressions by Channel")
    ax3.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x/1e6:.1f}M"))
    ax3.set_xlabel("Week of Year")
    ax3.legend(fontsize=7, labelcolor=TEXT_COLOR,
               facecolor=BG_COLOR, edgecolor=GRID_COLOR)
    _style(ax3)

    # CTR% vs weekly spend scatter by channel
    ax4 = fig.add_subplot(gs[1, 2])
    ch_agg = channel_weekly.groupby("channel").agg(
        avg_ctr=("avg_ctr", "mean"), total_spend=("total_spend", "sum"),
        total_clicks=("total_clicks", "sum"),
    ).reset_index()
    ch_agg["avg_cpc"] = ch_agg["total_spend"] / ch_agg["total_clicks"].replace(0, float("nan"))
    for i, row in ch_agg.iterrows():
        ax4.scatter(row["avg_ctr"] * 100, row["avg_cpc"],
                    s=130, color=PALETTE[i % len(PALETTE)], zorder=3)
        ax4.annotate(row["channel"],
                     (row["avg_ctr"] * 100, row["avg_cpc"]),
                     textcoords="offset points", xytext=(5, 3),
                     color=TEXT_COLOR, fontsize=6)
    ax4.set_title("CTR% vs CPC by Channel")
    ax4.set_xlabel("CTR (%)")
    ax4.set_ylabel("CPC (USD)")
    _style(ax4)

    out = EXPORT_DIR / "dashboard_preview.png"
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close()
    logger.info("Dashboard preview → %s", out)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-chart", action="store_true")
    args = parser.parse_args()
    export_csvs()
    write_dax_measures()
    if not args.no_chart:
        render_dashboard_preview()
    logger.info("Done → %s/", EXPORT_DIR)
