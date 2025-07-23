"""
CrewAI agents for customer health analysis with AI-powered recommendations
"""

from crewai import Agent, Task, Crew
from crewai.tools import BaseTool
from typing import Dict, List, Optional, Any
import os
import openai
from datetime import datetime, timedelta
import json
from pydantic import BaseModel, Field
from models.customer_health import (
    CustomerHealthScore, HealthStatus, Recommendation, RecommendationPriority
)

class HealthScoringTool(BaseTool):
    name: str = "health_score_calculator"
    description: str = "Calculate customer health scores from collected data"
    
    def _run(self, customer_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate comprehensive health score from customer data"""
        try:
            # Initialize scores
            usage_score = 0
            relationship_score = 0
            support_score = 100  # Start with perfect support score
            
            # Calculate usage score (0-100)
            if "usage_data" in customer_data:
                usage = customer_data["usage_data"]
                
                # Login frequency (40 points max)
                total_logins = usage.get("total_logins", 0)
                if total_logins >= 50:
                    usage_score += 40
                elif total_logins >= 25:
                    usage_score += 30
                elif total_logins >= 10:
                    usage_score += 20
                else:
                    usage_score += 10
                
                # Session duration (30 points max)
                avg_session = usage.get("avg_session_duration", 0)
                if avg_session >= 45:
                    usage_score += 30
                elif avg_session >= 25:
                    usage_score += 20
                elif avg_session >= 15:
                    usage_score += 10
                
                # Feature adoption (20 points max)
                features_used = usage.get("features_used", 0)
                if features_used >= 5:
                    usage_score += 20
                elif features_used >= 3:
                    usage_score += 15
                elif features_used >= 2:
                    usage_score += 10
                
                # Activity trend (10 points max)
                trend = usage.get("trend", "stable")
                if trend == "increasing":
                    usage_score += 10
                elif trend == "stable":
                    usage_score += 5
            
            # Calculate relationship score (0-100)
            if "relationship_data" in customer_data:
                relationship = customer_data["relationship_data"]
                
                # Contact recency (40 points max)
                last_contact = relationship.get("last_contact_date")
                if last_contact:
                    try:
                        last_contact_date = datetime.fromisoformat(last_contact.replace('Z', '+00:00'))
                        days_since_contact = (datetime.now() - last_contact_date.replace(tzinfo=None)).days
                        
                        if days_since_contact <= 7:
                            relationship_score += 40
                        elif days_since_contact <= 14:
                            relationship_score += 30
                        elif days_since_contact <= 30:
                            relationship_score += 20
                        else:
                            relationship_score += 10
                    except:
                        relationship_score += 20  # Default if date parsing fails
                
                # Engagement quality (40 points max)
                engagement_score = relationship.get("engagement_score", 0)
                emails_responded = relationship.get("emails_responded", 0)
                meetings_attended = relationship.get("meetings_attended", 0)
                
                if engagement_score > 80 or (emails_responded > 5 and meetings_attended > 2):
                    relationship_score += 40
                elif engagement_score > 60 or (emails_responded > 3 and meetings_attended > 1):
                    relationship_score += 30
                elif engagement_score > 40 or emails_responded > 1:
                    relationship_score += 20
                else:
                    relationship_score += 10
                
                # Contract health (20 points max)
                contract_value = relationship.get("contract_value", 0)
                renewal_probability = relationship.get("renewal_probability", 0.5)
                
                if contract_value > 100000 and renewal_probability > 0.8:
                    relationship_score += 20
                elif contract_value > 50000 and renewal_probability > 0.6:
                    relationship_score += 15
                elif renewal_probability > 0.4:
                    relationship_score += 10
                else:
                    relationship_score += 5
            
            # Calculate support score (start at 100, deduct for issues)
            if "support_data" in customer_data:
                support = customer_data["support_data"]
                
                # Open tickets penalty
                open_tickets = support.get("open_tickets", 0)
                support_score -= min(open_tickets * 15, 50)  # Max 50 point penalty
                
                # Resolution time penalty
                avg_resolution = support.get("avg_resolution_hours", 0)
                if avg_resolution > 72:
                    support_score -= 20
                elif avg_resolution > 48:
                    support_score -= 10
                
                # Satisfaction penalty
                satisfaction = support.get("satisfaction_score", 5)  # Out of 5
                if satisfaction < 3:
                    support_score -= 30
                elif satisfaction < 4:
                    support_score -= 15
                
                # Escalations penalty
                escalations = support.get("escalations", 0)
                support_score -= min(escalations * 10, 30)  # Max 30 point penalty
            
            # Ensure scores are within bounds
            usage_score = max(0, min(100, usage_score))
            relationship_score = max(0, min(100, relationship_score))
            support_score = max(0, min(100, support_score))
            
            # Calculate overall score (weighted average)
            overall_score = int(usage_score * 0.4 + relationship_score * 0.3 + support_score * 0.3)
            
            # Determine health status
            if overall_score >= 80:
                health_status = "healthy"
            elif overall_score >= 60:
                health_status = "at_risk"
            else:
                health_status = "critical"
            
            return {
                "overall_score": overall_score,
                "usage_score": usage_score,
                "relationship_score": relationship_score,
                "support_score": support_score,
                "health_status": health_status,
                "calculation_details": {
                    "usage_factors": customer_data.get("usage_data", {}),
                    "relationship_factors": customer_data.get("relationship_data", {}),
                    "support_factors": customer_data.get("support_data", {})
                }
            }
            
        except Exception as e:
            return {"error": f"Health scoring error: {str(e)}"}

class AIRecommendationTool(BaseTool):
    name: str = "ai_recommendation_generator"
    description: str = "Generate AI-powered customer success recommendations"
    
    def _run(self, customer_data: Dict[str, Any], health_scores: Dict[str, Any]) -> Dict[str, Any]:
        """Generate personalized recommendations using OpenAI"""
        try:
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                return {"error": "OpenAI API key not configured"}
            
            client = openai.OpenAI(api_key=api_key)
            
            # Create detailed prompt with customer context
            prompt = f"""
            As a Customer Success expert, analyze this customer data and provide 3 specific, actionable recommendations:

            CUSTOMER PROFILE:
            - Overall Health Score: {health_scores.get('overall_score', 0)}/100 ({health_scores.get('health_status', 'unknown')})
            - Usage Score: {health_scores.get('usage_score', 0)}/100
            - Relationship Score: {health_scores.get('relationship_score', 0)}/100
            - Support Score: {health_scores.get('support_score', 0)}/100

            DETAILED DATA:
            Usage Data: {json.dumps(customer_data.get('usage_data', {}), indent=2)}
            Relationship Data: {json.dumps(customer_data.get('relationship_data', {}), indent=2)}
            Support Data: {json.dumps(customer_data.get('support_data', {}), indent=2)}

            REQUIREMENTS:
            1. Provide exactly 3 recommendations
            2. Each recommendation should be specific and actionable
            3. Include priority level (critical/high/medium/low)
            4. Include realistic timeline
            5. Explain the reasoning behind each recommendation

            FORMAT (use exactly this format):
            1. ACTION: [specific action] | PRIORITY: [critical/high/medium/low] | TIMELINE: [timeframe] | REASONING: [why this helps]
            2. ACTION: [specific action] | PRIORITY: [critical/high/medium/low] | TIMELINE: [timeframe] | REASONING: [why this helps]
            3. ACTION: [specific action] | PRIORITY: [critical/high/medium/low] | TIMELINE: [timeframe] | REASONING: [why this helps]
            """
            
            response = client.chat.completions.create(
                model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
                max_tokens=800,
                messages=[{"role": "user", "content": prompt}]
            )
            
            content = response.choices[0].message.content
            recommendations = []
            
            # Parse the AI response
            lines = content.strip().split('\n')
            for line in lines:
                if '|' in line and 'ACTION:' in line:
                    try:
                        parts = line.split('|')
                        if len(parts) >= 4:
                            action = parts[0].split('ACTION:')[1].strip()
                            priority = parts[1].split('PRIORITY:')[1].strip().lower()
                            timeline = parts[2].split('TIMELINE:')[1].strip()
                            reasoning = parts[3].split('REASONING:')[1].strip()
                            
                            recommendations.append({
                                "action": action,
                                "priority": priority,
                                "timeline": timeline,
                                "reasoning": reasoning
                            })
                    except Exception as parse_error:
                        continue
            
            # Fallback recommendations if parsing failed
            if not recommendations:
                recommendations = self._generate_fallback_recommendations(health_scores)
            
            return {
                "recommendations": recommendations[:3],  # Ensure max 3 recommendations
                "ai_response": content
            }
            
        except Exception as e:
            # Fallback to rule-based recommendations
            return {
                "recommendations": self._generate_fallback_recommendations(health_scores),
                "error": f"AI recommendation error: {str(e)}"
            }
    
    def _generate_fallback_recommendations(self, health_scores: Dict[str, Any]) -> List[Dict[str, str]]:
        """Generate rule-based recommendations as fallback"""
        recommendations = []
        
        usage_score = health_scores.get('usage_score', 0)
        relationship_score = health_scores.get('relationship_score', 0)
        support_score = health_scores.get('support_score', 0)
        
        if usage_score < 60:
            recommendations.append({
                "action": "Schedule product training and onboarding session",
                "priority": "high",
                "timeline": "Within 1 week",
                "reasoning": "Low usage score indicates customer needs better product understanding"
            })
        
        if relationship_score < 60:
            recommendations.append({
                "action": "Increase CSM touchpoints and schedule check-in call",
                "priority": "high", 
                "timeline": "Within 3 days",
                "reasoning": "Poor relationship score requires immediate attention to prevent churn"
            })
        
        if support_score < 60:
            recommendations.append({
                "action": "Review and prioritize resolution of open support tickets",
                "priority": "critical",
                "timeline": "Within 24 hours",
                "reasoning": "Support issues are significantly impacting customer experience"
            })
        
        # Add general recommendations if no specific issues
        if len(recommendations) < 3:
            if health_scores.get('overall_score', 0) >= 80:
                recommendations.append({
                    "action": "Explore upsell opportunities and expansion use cases",
                    "priority": "medium",
                    "timeline": "Within 2 weeks",
                    "reasoning": "Healthy customer is good candidate for account expansion"
                })
            else:
                recommendations.append({
                    "action": "Conduct comprehensive account review and health assessment",
                    "priority": "medium",
                    "timeline": "Within 1 week", 
                    "reasoning": "Regular health assessments help maintain customer satisfaction"
                })
        
        return recommendations[:3]

def create_health_analysis_agents():
    """Create specialized agents for health analysis"""
    
    scoring_agent = Agent(
        role="Customer Health Scoring Specialist",
        goal="Calculate accurate customer health scores from multi-source data",
        backstory="Expert in customer success metrics, data analysis, and health scoring methodologies. Skilled at weighing different data signals appropriately.",
        tools=[HealthScoringTool()],
        verbose=True,
        allow_delegation=False
    )
    
    recommendation_agent = Agent(
        role="AI-Powered Customer Success Strategist", 
        goal="Generate actionable recommendations for improving customer health",
        backstory="Senior customer success manager with deep experience in customer retention strategies. Expert at translating data insights into specific action plans.",
        tools=[AIRecommendationTool()],
        verbose=True,
        allow_delegation=False
    )
    
    analysis_coordinator = Agent(
        role="Customer Health Analysis Coordinator",
        goal="Orchestrate comprehensive customer health analysis workflow",
        backstory="Expert at coordinating complex analysis workflows and ensuring data quality throughout the health assessment process.",
        verbose=True,
        allow_delegation=True
    )
    
    return {
        "scoring": scoring_agent,
        "recommendations": recommendation_agent,
        "coordinator": analysis_coordinator
    }

def create_health_analysis_crew(customer_data: Dict[str, Any], customer_info: Dict[str, str]):
    """Create a crew for comprehensive health analysis"""
    
    agents = create_health_analysis_agents()
    
    # Task 1: Calculate health scores
    scoring_task = Task(
        description=f"""
        Calculate comprehensive health scores for customer: {customer_info.get('name', 'Unknown')}
        
        Using the collected customer data:
        {json.dumps(customer_data, indent=2)}
        
        Requirements:
        1. Calculate usage score (0-100) based on product engagement
        2. Calculate relationship score (0-100) based on communication and satisfaction  
        3. Calculate support score (0-100) based on ticket history and resolution
        4. Determine overall weighted health score
        5. Classify health status (healthy/at_risk/critical)
        6. Provide detailed breakdown of scoring factors
        
        Account for any missing data gracefully and document assumptions.
        """,
        agent=agents["scoring"],
        expected_output="Detailed health scores with calculation breakdown and status classification"
    )
    
    # Task 2: Generate AI recommendations
    recommendation_task = Task(
        description=f"""
        Generate personalized customer success recommendations for: {customer_info.get('name', 'Unknown')}
        
        Based on the health scoring results and customer context:
        - Customer: {customer_info.get('name', 'Unknown')} ({customer_info.get('email', 'Unknown')})
        - Company: {customer_info.get('company', 'Unknown')}
        - Account Value: {customer_info.get('account_value', 'Unknown')}
        
        Requirements:
        1. Generate exactly 3 specific, actionable recommendations
        2. Prioritize recommendations (critical/high/medium/low)
        3. Provide realistic timelines for each action
        4. Explain the reasoning behind each recommendation
        5. Consider the customer's industry, size, and value
        6. Focus on the most impactful improvements first
        
        Recommendations should be immediately actionable by a CSM or sales rep.
        """,
        agent=agents["recommendations"],
        expected_output="3 prioritized recommendations with actions, timelines, and reasoning"
    )
    
    return Crew(
        agents=list(agents.values()),
        tasks=[scoring_task, recommendation_task],
        verbose=False,
        process="sequential"
    )