from pydantic import BaseModel
from typing import List, Dict, Optional
from datetime import datetime
from enum import Enum

class HealthStatus(str, Enum):
    HEALTHY = "healthy"
    AT_RISK = "at_risk"
    CRITICAL = "critical"

class RecommendationPriority(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class CustomerUsage(BaseModel):
    customer_id: str
    total_logins: int
    avg_session_duration: float
    feature_adoption_count: int
    last_activity_date: datetime
    usage_trend: str  # "increasing", "stable", "decreasing"

class CustomerCRM(BaseModel):
    customer_id: str
    company_name: str
    account_value: float
    last_contact_date: datetime
    contact_sentiment: str
    contract_end_date: datetime
    csm_name: str

class CustomerSupport(BaseModel):
    customer_id: str
    open_tickets: int
    avg_resolution_time: float
    recent_sentiment: str
    escalated_issues: int

class Recommendation(BaseModel):
    action: str
    priority: RecommendationPriority
    reasoning: str
    timeline: str

class CustomerHealthScore(BaseModel):
    customer_id: str
    company_name: str
    overall_score: int  # 0-100
    health_status: HealthStatus
    usage_score: int
    relationship_score: int
    support_score: int
    recommendations: List[Recommendation]
    reasoning: str
    last_updated: datetime = datetime.now()