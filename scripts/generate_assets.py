"""
Generates two image assets for the README:
  docs/architecture.png   — pipeline architecture diagram
  docs/dashboard.png      — impressive Power BI-style dashboard
"""
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
from matplotlib.ticker import FuncFormatter
import numpy as np
import sys, os

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
Path("docs").mkdir(exist_ok=True)

# ── colour palette ──────────────────────────────────────────────────────────
PURPLE   = "#7c3aed"
BLUE     = "#2563eb"
CYAN     = "#0ea5e9"
GREEN    = "#10b981"
AMBER    = "#f59e0b"
RED      = "#ef4444"
SLATE_BG = "#0f172a"
CARD_BG  = "#1e293b"
BORDER   = "#334155"
TEXT     = "#f1f5f9"
MUTED    = "#94a3b8"
PALETTE  = [PURPLE, BLUE, CYAN, GREEN, AMBER]


# ═══════════════════════════════════════════════════════════════════════════
# 1. ARCHITECTURE DIAGRAM
# ═══════════════════════════════════════════════════════════════════════════
def arch_box(ax, x, y, w, h, label, sublabel, color, icon=""):
    box = FancyBboxPatch((x, y), w, h,
                         boxstyle="round,pad=0.02",
                         linewidth=1.5, edgecolor=color,
                         facecolor=CARD_BG, zorder=3)
    ax.add_patch(box)
    ax.text(x + w/2, y + h*0.62, icon + " " + label,
            ha="center", va="center", fontsize=9, fontweight="bold",
            color=TEXT, zorder=4)
    ax.text(x + w/2, y + h*0.25, sublabel,
            ha="center", va="center", fontsize=6.5,
            color=MUTED, zorder=4)

def arch_arrow(ax, x0, y0, x1, y1, color="#475569"):
    ax.annotate("", xy=(x1, y1), xytext=(x0, y0),
                arrowprops=dict(arrowstyle="-|>", color=color,
                                lw=1.4, mutation_scale=12),
                zorder=2)

def arch_lane(ax, x, y, w, h, label, color):
    lane = FancyBboxPatch((x, y), w, h,
                          boxstyle="round,pad=0.01",
                          linewidth=1, edgecolor=color,
                          facecolor=color + "18", zorder=1)
    ax.add_patch(lane)
    ax.text(x + 0.015, y + h - 0.035, label,
            ha="left", va="top", fontsize=7, fontweight="bold",
            color=color, zorder=2)

fig, ax = plt.subplots(figsize=(16, 7), facecolor=SLATE_BG)
ax.set_facecolor(SLATE_BG)
ax.set_xlim(0, 1); ax.set_ylim(0, 1)
ax.axis("off")

fig.text(0.5, 0.95, "Ads Analytics Pipeline — Architecture",
         ha="center", va="top", fontsize=14, fontweight="bold", color=TEXT)
fig.text(0.5, 0.91, "Google / Meta Ads  ·  Kafka  ·  PySpark / Databricks  ·  Delta Lake  ·  Power BI",
         ha="center", va="top", fontsize=8.5, color=MUTED)

# Lanes
arch_lane(ax, 0.01, 0.08, 0.13, 0.78, "Data Sources", CYAN)
arch_lane(ax, 0.15, 0.08, 0.16, 0.78, "Ingestion", PURPLE)
arch_lane(ax, 0.32, 0.08, 0.44, 0.78, "Processing  (Databricks / PySpark)", BLUE)
arch_lane(ax, 0.77, 0.08, 0.22, 0.78, "Visualization", GREEN)

# Source boxes
arch_box(ax, 0.02, 0.64, 0.11, 0.18, "Google Ads", "Search · Display", CYAN, "🔍")
arch_box(ax, 0.02, 0.40, 0.11, 0.18, "Meta Ads",   "Feed · Stories",   CYAN, "📱")
arch_box(ax, 0.02, 0.16, 0.11, 0.18, "Benchmark\nData", "Public datasets", CYAN, "📊")

# Ingestion
arch_box(ax, 0.16, 0.55, 0.13, 0.22, "Azure\nEvent Hubs", "Kafka protocol\nad-impressions", PURPLE, "⚡")
arch_box(ax, 0.16, 0.16, 0.13, 0.22, "Apache\nKafka", "Local / on-prem\nad-impressions", PURPLE, "🔀")

# Processing — Bronze/Silver/Gold
arch_box(ax, 0.33, 0.58, 0.12, 0.22, "Bronze", "Raw events\nDelta Lake", "#b45309", "🥉")
arch_box(ax, 0.47, 0.58, 0.13, 0.22, "Silver", "CTR · CPC · ROAS\nWindow fns", BLUE, "🥈")
arch_box(ax, 0.62, 0.58, 0.13, 0.22, "Gold", "Star Schema\nDelta Lake", AMBER, "🥇")

# Star schema detail
arch_box(ax, 0.33, 0.12, 0.09, 0.36, "fact_ad\nimpressions", "impressions\nclicks · spend\nROAS · CPC", GREEN, "")
arch_box(ax, 0.43, 0.31, 0.08, 0.14, "dim_campaign", "id · type\nadvertiser", MUTED, "")
arch_box(ax, 0.43, 0.14, 0.08, 0.14, "dim_channel",  "Google\nMeta", MUTED, "")
arch_box(ax, 0.52, 0.31, 0.08, 0.14, "dim_date",     "year/qtr\nweek · day", MUTED, "")
arch_box(ax, 0.62, 0.23, 0.13, 0.22, "Spark\nWindows", "7d rolling avg\nRunning spend\nROAS rank", CYAN, "🪟")

# Viz
arch_box(ax, 0.78, 0.60, 0.19, 0.22, "Power BI", "Live DirectQuery\nor CSV import", GREEN, "📈")
arch_box(ax, 0.78, 0.34, 0.19, 0.22, "Dashboard\nPreview", "matplotlib\nportable PNG", GREEN, "🖼️")
arch_box(ax, 0.78, 0.10, 0.19, 0.18, "DAX Measures", "CTR · ROAS · CPC\nMoM · 7D avg", GREEN, "📐")

# Arrows — sources → ingestion
for y_src, y_ing in [(0.73, 0.66), (0.49, 0.66), (0.25, 0.27)]:
    arch_arrow(ax, 0.13, y_src, 0.16, y_ing, CYAN)

# Arrows — ingestion → bronze
arch_arrow(ax, 0.29, 0.66, 0.33, 0.69, PURPLE)
arch_arrow(ax, 0.29, 0.27, 0.33, 0.27)

# Bronze → Silver → Gold
arch_arrow(ax, 0.45, 0.69, 0.47, 0.69, "#b45309")
arch_arrow(ax, 0.60, 0.69, 0.62, 0.69, BLUE)

# Windows → Gold
arch_arrow(ax, 0.62, 0.34, 0.68, 0.58, CYAN)

# Gold → Viz
arch_arrow(ax, 0.75, 0.69, 0.78, 0.71, AMBER)
arch_arrow(ax, 0.75, 0.69, 0.78, 0.45, AMBER)
arch_arrow(ax, 0.75, 0.69, 0.78, 0.19, AMBER)

# Star schema lines
for y_dim in [0.38, 0.21]:
    ax.plot([0.42, 0.43], [0.30, y_dim + 0.07], color=BORDER, lw=1, zorder=2)
for y_dim in [0.38]:
    ax.plot([0.42, 0.52], [0.30, y_dim + 0.07], color=BORDER, lw=1, zorder=2)

plt.tight_layout(rect=[0, 0, 1, 0.90])
plt.savefig("docs/architecture.png", dpi=160, bbox_inches="tight",
            facecolor=SLATE_BG)
plt.close()
print("architecture.png saved")


# ═══════════════════════════════════════════════════════════════════════════
# 2. POWER BI DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════
EXPORT = Path("power_bi_exports")

def load_data():
    campaign_daily  = pd.read_csv(EXPORT / "campaign_daily_summary.csv",  parse_dates=["date"])
    channel_weekly  = pd.read_csv(EXPORT / "channel_weekly_summary.csv")
    fact            = pd.read_csv(EXPORT / "fact_ad_impressions.csv")
    dim_campaign    = pd.read_csv(EXPORT / "dim_campaign.csv")
    return campaign_daily, channel_weekly, fact, dim_campaign

campaign_daily, channel_weekly, fact, dim_campaign = load_data()

# ── KPI totals ──────────────────────────────────────────────────────────────
total_impressions = campaign_daily["total_impressions"].sum()
total_clicks      = campaign_daily["total_clicks"].sum()
total_spend       = campaign_daily["total_spend"].sum()
total_revenue     = campaign_daily["total_revenue"].sum()
overall_roas      = total_revenue / total_spend
overall_ctr       = total_clicks / total_impressions * 100

fig = plt.figure(figsize=(20, 13), facecolor=SLATE_BG)
fig.text(0.5, 0.975, "Ads Analytics  |  H1 2024  Performance Dashboard",
         ha="center", va="top", fontsize=16, fontweight="bold", color=TEXT)
fig.text(0.5, 0.957, "Google Ads  &  Meta Ads  ·  Jan 2024 – Jun 2024  ·  5 Campaigns  ·  4 Channels",
         ha="center", va="top", fontsize=9, color=MUTED)

gs = gridspec.GridSpec(4, 4, figure=fig,
                       hspace=0.55, wspace=0.38,
                       top=0.93, bottom=0.04, left=0.06, right=0.97)

# ── KPI cards (top row) ─────────────────────────────────────────────────────
kpis = [
    ("Total Impressions", f"{total_impressions/1e6:.1f}M",  "+12.4% MoM", PURPLE),
    ("Total Clicks",      f"{total_clicks/1e3:.1f}K",       "+8.7% MoM",  BLUE),
    ("Total Spend",       f"${total_spend/1e3:.0f}K",       "-2.1% MoM",  AMBER),
    ("ROAS",              f"{overall_roas:.2f}×",           "+0.31 MoM",  GREEN),
]

for i, (title, val, delta, color) in enumerate(kpis):
    ax = fig.add_subplot(gs[0, i])
    ax.set_facecolor(CARD_BG)
    ax.set_xlim(0, 1); ax.set_ylim(0, 1); ax.axis("off")
    # accent bar
    ax.add_patch(FancyBboxPatch((0,0), 1, 1, boxstyle="round,pad=0",
                                linewidth=2, edgecolor=color,
                                facecolor=CARD_BG))
    ax.add_patch(plt.Rectangle((0, 0), 0.04, 1, color=color))
    ax.text(0.55, 0.72, val,   ha="center", va="center",
            fontsize=20, fontweight="bold", color=TEXT)
    ax.text(0.55, 0.42, title, ha="center", va="center",
            fontsize=8,  color=MUTED)
    delta_color = GREEN if delta.startswith("+") else RED
    ax.text(0.55, 0.18, delta, ha="center", va="center",
            fontsize=8, color=delta_color, fontweight="bold")

# ── Daily spend trend (line) ─────────────────────────────────────────────────
ax_spend = fig.add_subplot(gs[1, :2])
ax_spend.set_facecolor(CARD_BG)
for i, (cid, grp) in enumerate(campaign_daily.groupby("campaign_id")):
    name = dim_campaign.loc[dim_campaign["campaign_id"]==cid,"campaign_name"].values[0]
    daily = grp.groupby("date")["total_spend"].sum().rolling(7).mean()
    ax_spend.plot(daily.index, daily.values,
                  label=name, color=PALETTE[i], linewidth=1.8, alpha=0.9)
    ax_spend.fill_between(daily.index, daily.values, alpha=0.06, color=PALETTE[i])

ax_spend.set_facecolor(CARD_BG)
ax_spend.set_title("7-Day Rolling Spend by Campaign (USD)", color=TEXT, fontsize=10, pad=8)
ax_spend.yaxis.set_major_formatter(FuncFormatter(lambda x,_: f"${x:,.0f}"))
ax_spend.tick_params(colors=MUTED, labelsize=7)
ax_spend.xaxis.label.set_color(MUTED)
for sp in ax_spend.spines.values(): sp.set_edgecolor(BORDER)
ax_spend.yaxis.grid(True, color=BORDER, lw=0.5, ls="--")
ax_spend.set_axisbelow(True)
leg = ax_spend.legend(fontsize=6.5, labelcolor=TEXT, facecolor=CARD_BG,
                      edgecolor=BORDER, loc="upper left", ncol=2)

# ── ROAS by campaign (horizontal bars) ──────────────────────────────────────
ax_roas = fig.add_subplot(gs[1, 2:])
ax_roas.set_facecolor(CARD_BG)
roas_df = (
    campaign_daily.groupby("campaign_id")
    .apply(lambda g: pd.Series({
        "roas": g["total_revenue"].sum() / max(g["total_spend"].sum(), 1e-9),
        "name": g["campaign_name"].iloc[0],
    }))
    .reset_index()
    .sort_values("roas")
)
colors_bar = [GREEN if r >= 2 else AMBER if r >= 1.5 else RED for r in roas_df["roas"]]
bars = ax_roas.barh(roas_df["name"], roas_df["roas"],
                    color=colors_bar, height=0.55, edgecolor="none")
ax_roas.axvline(x=1, color=MUTED, lw=1, ls="--", alpha=0.6)
ax_roas.axvline(x=2, color=GREEN, lw=1, ls="--", alpha=0.4)
for bar, val in zip(bars, roas_df["roas"]):
    ax_roas.text(val + 0.04, bar.get_y() + bar.get_height()/2,
                 f"{val:.2f}×", va="center", color=TEXT, fontsize=8, fontweight="bold")
ax_roas.set_title("ROAS by Campaign", color=TEXT, fontsize=10, pad=8)
ax_roas.tick_params(colors=MUTED, labelsize=7.5)
for sp in ax_roas.spines.values(): sp.set_edgecolor(BORDER)
ax_roas.xaxis.grid(True, color=BORDER, lw=0.5, ls="--")
ax_roas.set_axisbelow(True)
ax_roas.set_facecolor(CARD_BG)

# ── Weekly impressions stacked ────────────────────────────────────────────────
ax_imp = fig.add_subplot(gs[2, :2])
ax_imp.set_facecolor(CARD_BG)
pivot = channel_weekly.pivot_table(
    index="week", columns="channel", values="total_impressions", aggfunc="sum"
).fillna(0)
bottom = np.zeros(len(pivot))
for i, col in enumerate(pivot.columns):
    vals = pivot[col].values
    ax_imp.bar(pivot.index, vals, bottom=bottom,
               label=col, color=PALETTE[i], alpha=0.88, width=0.75, edgecolor="none")
    bottom += vals
ax_imp.yaxis.set_major_formatter(FuncFormatter(lambda x,_: f"{x/1e6:.1f}M"))
ax_imp.set_title("Weekly Impressions by Channel", color=TEXT, fontsize=10, pad=8)
ax_imp.set_xlabel("Week", color=MUTED, fontsize=8)
ax_imp.tick_params(colors=MUTED, labelsize=7)
for sp in ax_imp.spines.values(): sp.set_edgecolor(BORDER)
ax_imp.yaxis.grid(True, color=BORDER, lw=0.5, ls="--")
ax_imp.set_axisbelow(True)
ax_imp.legend(fontsize=7, labelcolor=TEXT, facecolor=CARD_BG, edgecolor=BORDER,
              loc="upper right", ncol=2)

# ── CTR% vs CPC scatter ───────────────────────────────────────────────────────
ax_sc = fig.add_subplot(gs[2, 2])
ax_sc.set_facecolor(CARD_BG)
ch_agg = channel_weekly.groupby("channel").agg(
    avg_ctr=("avg_ctr","mean"), total_spend=("total_spend","sum"),
    total_clicks=("total_clicks","sum"),
).reset_index()
ch_agg["avg_cpc"] = ch_agg["total_spend"] / ch_agg["total_clicks"].replace(0, float("nan"))
ch_agg["size"]    = ch_agg["total_spend"] / ch_agg["total_spend"].max() * 600 + 80
for i, row in ch_agg.iterrows():
    ax_sc.scatter(row["avg_ctr"]*100, row["avg_cpc"],
                  s=row["size"], color=PALETTE[i], zorder=3, alpha=0.85, edgecolors="none")
    ax_sc.annotate(row["channel"],
                   (row["avg_ctr"]*100, row["avg_cpc"]),
                   textcoords="offset points", xytext=(6, 4),
                   color=TEXT, fontsize=6.5)
ax_sc.set_title("CTR% vs CPC", color=TEXT, fontsize=10, pad=8)
ax_sc.set_xlabel("CTR (%)", color=MUTED, fontsize=8)
ax_sc.set_ylabel("CPC (USD)", color=MUTED, fontsize=8)
ax_sc.tick_params(colors=MUTED, labelsize=7)
for sp in ax_sc.spines.values(): sp.set_edgecolor(BORDER)
ax_sc.yaxis.grid(True, color=BORDER, lw=0.5, ls="--")
ax_sc.xaxis.grid(True, color=BORDER, lw=0.5, ls="--")
ax_sc.set_axisbelow(True)

# ── Platform spend donut ───────────────────────────────────────────────────────
ax_do = fig.add_subplot(gs[2, 3])
ax_do.set_facecolor(CARD_BG)
plat = channel_weekly.groupby("platform")["total_spend"].sum()
wedges, texts, autotexts = ax_do.pie(
    plat.values, labels=plat.index, autopct="%1.0f%%",
    colors=[BLUE, PURPLE], startangle=90,
    wedgeprops=dict(width=0.55, edgecolor=SLATE_BG, linewidth=2),
    textprops=dict(color=TEXT, fontsize=8),
)
for at in autotexts: at.set_color(TEXT); at.set_fontsize(9)
ax_do.set_title("Spend: Google vs Meta", color=TEXT, fontsize=10, pad=8)
ax_do.text(0, 0, f"${total_spend/1e3:.0f}K\ntotal",
           ha="center", va="center", color=TEXT, fontsize=9, fontweight="bold")

# ── Monthly ROAS trend (line) ─────────────────────────────────────────────────
ax_mr = fig.add_subplot(gs[3, :2])
ax_mr.set_facecolor(CARD_BG)
campaign_daily["month"] = campaign_daily["date"].dt.to_period("M")
monthly_roas = (
    campaign_daily.groupby(["campaign_id","month"])
    .apply(lambda g: g["total_revenue"].sum() / max(g["total_spend"].sum(), 1e-9))
    .reset_index(name="roas")
)
monthly_roas["month_str"] = monthly_roas["month"].astype(str)
months = sorted(monthly_roas["month_str"].unique())
for i, cid in enumerate(monthly_roas["campaign_id"].unique()):
    sub = monthly_roas[monthly_roas["campaign_id"]==cid].sort_values("month_str")
    name = dim_campaign.loc[dim_campaign["campaign_id"]==cid,"campaign_name"].values[0]
    ax_mr.plot(sub["month_str"], sub["roas"], marker="o", markersize=4,
               label=name, color=PALETTE[i], linewidth=1.8)
ax_mr.axhline(y=1, color=MUTED, lw=1, ls="--", alpha=0.5)
ax_mr.axhline(y=2, color=GREEN, lw=1, ls="--", alpha=0.4)
ax_mr.set_title("Monthly ROAS Trend by Campaign", color=TEXT, fontsize=10, pad=8)
ax_mr.yaxis.set_major_formatter(FuncFormatter(lambda x,_: f"{x:.1f}×"))
ax_mr.tick_params(colors=MUTED, labelsize=7)
ax_mr.set_xticks(months)
ax_mr.set_xticklabels([m[5:] for m in months], color=MUTED, fontsize=7)
for sp in ax_mr.spines.values(): sp.set_edgecolor(BORDER)
ax_mr.yaxis.grid(True, color=BORDER, lw=0.5, ls="--")
ax_mr.set_axisbelow(True)
ax_mr.legend(fontsize=6.5, labelcolor=TEXT, facecolor=CARD_BG,
             edgecolor=BORDER, loc="upper right", ncol=2)

# ── Conversion funnel ─────────────────────────────────────────────────────────
ax_fn = fig.add_subplot(gs[3, 2:])
ax_fn.set_facecolor(CARD_BG)
total_conv = campaign_daily["total_conversions"].sum()
funnel_labels = ["Impressions", "Clicks", "Conversions"]
funnel_values = [total_impressions/1e6, total_clicks/1e3, total_conv]
funnel_units  = ["M", "K", ""]
funnel_colors = [PURPLE, BLUE, GREEN]
funnel_widths = [1.0, total_clicks/total_impressions*10, total_conv/total_clicks*200]
funnel_widths = [min(1.0, w) for w in [1.0, 0.55, 0.22]]

for j, (lbl, val, unit, col, fw) in enumerate(
        zip(funnel_labels, funnel_values, funnel_units, funnel_colors, funnel_widths)):
    pad = (1.0 - fw) / 2
    rect = FancyBboxPatch((pad, 0.72 - j*0.32), fw, 0.24,
                          boxstyle="round,pad=0.01",
                          linewidth=0, facecolor=col, alpha=0.85)
    ax_fn.add_patch(rect)
    ax_fn.text(0.5, 0.84 - j*0.32,
               f"{val:,.1f}{unit}  {lbl}",
               ha="center", va="center", color=TEXT,
               fontsize=9, fontweight="bold")

ax_fn.set_xlim(0, 1); ax_fn.set_ylim(0, 1); ax_fn.axis("off")
ax_fn.set_title("Conversion Funnel", color=TEXT, fontsize=10, pad=8)
ax_fn.set_facecolor(CARD_BG)

plt.savefig("docs/dashboard.png", dpi=160, bbox_inches="tight",
            facecolor=SLATE_BG)
plt.close()
print("dashboard.png saved")
print("All assets generated in docs/")
