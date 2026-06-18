"""
Pydantic models for raw Kafka events and dataclasses for the star-schema tables.
"""
from __future__ import annotations
from dataclasses import dataclass
from pydantic import BaseModel, Field


class RawAdEvent(BaseModel):
    event_id: str
    timestamp: str                  # ISO-8601
    campaign_id: str
    campaign_name: str
    campaign_type: str              # awareness | conversion | retargeting
    advertiser: str
    platform: str                   # Google | Meta
    channel: str                    # Google Search | Google Display | Meta Feed | Meta Stories
    date: str                       # YYYY-MM-DD
    impressions: int = Field(ge=0)
    clicks: int       = Field(ge=0)
    conversions: int  = Field(ge=0)
    spend_usd: float  = Field(ge=0)
    revenue_usd: float = Field(ge=0)


@dataclass
class DimCampaign:
    campaign_key: int
    campaign_id: str
    campaign_name: str
    campaign_type: str
    advertiser: str
    daily_budget_usd: float


@dataclass
class DimDate:
    date_key: int
    full_date: str
    year: int
    quarter: int
    month: int
    week: int
    day: int
    day_of_week: str
    is_weekend: bool


@dataclass
class DimChannel:
    channel_key: int
    channel_name: str
    platform: str
    channel_type: str               # search | display | social_feed | social_story


@dataclass
class FactAdImpression:
    impression_id: str
    campaign_key: int
    date_key: int
    channel_key: int
    impressions: int
    clicks: int
    conversions: int
    spend_usd: float
    revenue_usd: float
    ctr: float
    cpc: float
    roas: float
