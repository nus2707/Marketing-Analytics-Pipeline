"""
Generates synthetic Google Ads / Meta Ads benchmark data.

Benchmark ranges based on publicly available industry averages:
  - Google Search CTR: 2–5%,   CPC: $1–4
  - Google Display CTR: 0.1–0.5%, CPC: $0.50–2
  - Meta Feed CTR: 0.9–1.5%,   CPC: $0.50–1.50
  - Meta Stories CTR: 0.5–1.0%, CPC: $0.30–1.00
"""
from __future__ import annotations
import uuid
import random
import json
from datetime import date, timedelta
from typing import Iterator

from ads_pipeline.schemas import RawAdEvent

random.seed(42)

CAMPAIGNS = [
    {"id": "C001", "name": "Summer Sale 2024",     "type": "conversion",  "advertiser": "RetailCo",    "budget": 5000.0},
    {"id": "C002", "name": "Brand Awareness Q3",   "type": "awareness",   "advertiser": "RetailCo",    "budget": 3000.0},
    {"id": "C003", "name": "Retargeting – Cart",   "type": "retargeting", "advertiser": "RetailCo",    "budget": 2000.0},
    {"id": "C004", "name": "Product Launch Alpha", "type": "conversion",  "advertiser": "TechStartup", "budget": 8000.0},
    {"id": "C005", "name": "Holiday Promo 2024",   "type": "conversion",  "advertiser": "TechStartup", "budget": 6000.0},
]

CHANNELS = [
    {"name": "Google Search",  "platform": "Google", "type": "search",
     "ctr_range": (0.02, 0.05),   "cpc_range": (1.0, 4.0),  "cvr_range": (0.02, 0.08), "roas_range": (2.0, 5.0)},
    {"name": "Google Display", "platform": "Google", "type": "display",
     "ctr_range": (0.001, 0.005), "cpc_range": (0.5, 2.0),  "cvr_range": (0.005, 0.02),"roas_range": (1.0, 3.0)},
    {"name": "Meta Feed",      "platform": "Meta",   "type": "social_feed",
     "ctr_range": (0.009, 0.015), "cpc_range": (0.5, 1.5),  "cvr_range": (0.01, 0.04), "roas_range": (1.5, 4.0)},
    {"name": "Meta Stories",   "platform": "Meta",   "type": "social_story",
     "ctr_range": (0.005, 0.01),  "cpc_range": (0.3, 1.0),  "cvr_range": (0.008, 0.03),"roas_range": (1.2, 3.5)},
]


def generate_events(
    start_date: date = date(2024, 1, 1),
    end_date: date   = date(2024, 6, 30),
) -> Iterator[dict]:
    """Yield one dict per (campaign × channel × day)."""
    current = start_date
    while current <= end_date:
        for campaign in CAMPAIGNS:
            active_channels = random.sample(CHANNELS, k=random.randint(2, 3))
            for ch in active_channels:
                weekend_boost = 1.2 if current.weekday() >= 5 else 1.0
                impressions = int(random.randint(5_000, 50_000) * weekend_boost)
                ctr = random.uniform(*ch["ctr_range"])
                clicks = max(1, int(impressions * ctr))
                cvr = random.uniform(*ch["cvr_range"])
                conversions = max(0, int(clicks * cvr))
                cpc = random.uniform(*ch["cpc_range"])
                spend = round(clicks * cpc, 2)
                roas = random.uniform(*ch["roas_range"])
                revenue = round(spend * roas, 2)

                event = RawAdEvent(
                    event_id=str(uuid.uuid4()),
                    timestamp=f"{current.isoformat()}T00:00:00Z",
                    campaign_id=campaign["id"],
                    campaign_name=campaign["name"],
                    campaign_type=campaign["type"],
                    advertiser=campaign["advertiser"],
                    platform=ch["platform"],
                    channel=ch["name"],
                    date=current.isoformat(),
                    impressions=impressions,
                    clicks=clicks,
                    conversions=conversions,
                    spend_usd=spend,
                    revenue_usd=revenue,
                )
                # compatible with pydantic v1 (.dict) and v2 (.model_dump)
                yield event.model_dump() if hasattr(event, "model_dump") else event.dict()
        current += timedelta(days=1)


if __name__ == "__main__":
    events = list(generate_events())
    print(f"Generated {len(events):,} ad events")
    print(json.dumps(events[0], indent=2))
