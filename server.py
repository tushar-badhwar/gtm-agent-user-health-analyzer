#!/usr/bin/env python3
"""
Customer Health Analyzer MCP Server - Stable Version with Enhanced Error Handling

An MCP server that provides AI-powered customer health analysis tools for GTM teams.
Analyzes customer usage, CRM, and support data to provide health scores and actionable recommendations.
"""

import asyncio
import json
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.server.lowlevel.server import NotificationOptions
from mcp.types import (
    CallToolRequest,
    CallToolResult,
    Tool,
    TextContent,
)
from pydantic import AnyUrl
import pandas as pd
from dotenv import load_dotenv

# Import our models and orchestrator
from models.customer_health import (
    CustomerHealthScore, HealthStatus, CustomerUsage, 
    CustomerCRM, CustomerSupport, Recommendation, RecommendationPriority
)
from orchestrator import CustomerHealthOrchestrator

# Import Airtable discovery tool
try:
    from airtable_discovery import AirtableDiscoveryTool
except ImportError as e:
    print(f"⚠️ Airtable discovery not available: {e}", file=sys.stderr)
    AirtableDiscoveryTool = None

# Load environment variables
load_dotenv()

# Suppress ALL CrewAI output immediately
os.environ["CREWAI_DISABLE_TELEMETRY"] = "true"
os.environ["CREWAI_VERBOSE"] = "false" 
os.environ["OPENAI_LOG_LEVEL"] = "ERROR"
os.environ["LANGCHAIN_VERBOSE"] = "false"

# The CustomerHealthAnalyzer class has been replaced by CustomerHealthOrchestrator
# which uses CrewAI agents for dynamic data integration and health analysis

# Initialize the orchestrator (replaces the simple analyzer)
orchestrator = CustomerHealthOrchestrator()

# Create MCP server
server = Server("customer-health-analyzer")

@server.list_tools()
async def handle_list_tools() -> list[Tool]:
    """List available customer health analysis tools"""
    print("🔧 Creating tools list...", file=sys.stderr)
    
    try:
        tools = [
            Tool(
                name="set_data_source",
                description="Select which data source to use for customer health analysis",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "data_source": {
                            "type": "string",
                            "enum": ["static", "airtable", "hubspot", "zapier"],
                            "description": "Data source to use: 'static' for sample data (5 customers), 'airtable' for Airtable database, 'hubspot' for HubSpot CRM (coming soon), 'zapier' for Zapier integration (coming soon)"
                        }
                    },
                    "required": ["data_source"],
                    "additionalProperties": False,
                }
            ),
            Tool(
                name="get_data_source_status",
                description="Show current data source configuration and available options",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                }
            ),
            Tool(
                name="analyze_customer_health",
                description="Analyze customer health scores for all customers or a specific customer from the currently selected data source",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "customer_id": {
                            "type": "string",
                            "description": "Optional specific customer ID to analyze (e.g., 'CUST001')"
                        },
                        "format": {
                            "type": "string",
                            "enum": ["detailed", "summary"],
                            "description": "Output format - 'detailed' for full analysis or 'summary' for overview only"
                        }
                    },
                    "additionalProperties": False
                }
            ),
            Tool(
                name="list_customers",
                description="List all available customers in the currently selected data source",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                }
            ),
            Tool(
                name="get_customer_details",
                description="Get detailed information about a specific customer",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "customer_id": {
                            "type": "string",
                            "description": "Customer ID to get details for (e.g., 'CUST001')"
                        }
                    },
                    "required": ["customer_id"],
                    "additionalProperties": False,
                }
            ),
            Tool(
                name="get_recommendations",
                description="Get AI-powered recommendations for improving customer health",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "customer_id": {
                            "type": "string",
                            "description": "Customer ID to get recommendations for (e.g., 'CUST001')"
                        }
                    },
                    "required": ["customer_id"],
                    "additionalProperties": False,
                }
            ),
            Tool(
                name="discover_airtable_bases",
                description="Discover all accessible Airtable bases for the configured API token",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                }
            ),
            Tool(
                name="discover_airtable_schema",
                description="Discover complete schema (tables and fields) for a specific Airtable base",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "base_id": {
                            "type": "string",
                            "description": "Airtable base ID to analyze (e.g., 'appXXXXXXXXXXXXXX')"
                        },
                        "format": {
                            "type": "string",
                            "enum": ["summary", "detailed", "json"],
                            "description": "Output format - 'summary' for overview, 'detailed' for full report, 'json' for machine-readable format"
                        }
                    },
                    "required": ["base_id"],
                    "additionalProperties": False,
                }
            ),
            Tool(
                name="find_airtable_customer_tables",
                description="Find tables in an Airtable base that likely contain customer data",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "base_id": {
                            "type": "string",
                            "description": "Airtable base ID to analyze (e.g., 'appXXXXXXXXXXXXXX')"
                        }
                    },
                    "required": ["base_id"],
                    "additionalProperties": False,
                }
            ),
            Tool(
                name="connect_to_airtable_base",
                description="Connect to a specific Airtable base - all subsequent Airtable operations will use this base",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "base_id": {
                            "type": "string",
                            "description": "Airtable base ID to connect to (e.g., 'appXXXXXXXXXXXXXX')"
                        }
                    },
                    "required": ["base_id"],
                    "additionalProperties": False,
                }
            ),
            Tool(
                name="get_current_airtable_base",
                description="Show information about the currently connected Airtable base",
                inputSchema={
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                }
            )
        ]
        
        print(f"✅ Created {len(tools)} tools", file=sys.stderr)
        for i, tool in enumerate(tools):
            print(f"  Tool {i+1}: {tool.name}", file=sys.stderr)
        return tools
        
    except Exception as e:
        print(f"❌ Error creating tools: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        # Return minimal tools on error
        return []

async def handle_call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Handle tool execution requests with comprehensive error handling"""
    
    print(f"🔧 Tool name: {name}", file=sys.stderr)
    print(f"🔧 Arguments: {arguments}", file=sys.stderr)
    
    try:
        # Add input validation
        if not name:
            return [TextContent(type="text", text="❌ Error: Tool name is required")]
        
        if not isinstance(arguments, dict):
            return [TextContent(type="text", text="❌ Error: Arguments must be a dictionary")]

        if name == "set_data_source":
            try:
                data_source = arguments.get("data_source")
                if not data_source:
                    return [TextContent(type="text", text="❌ Error: data_source parameter is required")]
                
                # Update orchestrator data source configuration
                result = orchestrator.set_data_source(data_source)
                
                if result.get("success"):
                    return [TextContent(type="text", text=f"✅ Data source set to: {data_source}\n\n{result.get('message', '')}\n\n🔍 Use list_customers to see available customers from this data source.")]
                else:
                    return [TextContent(type="text", text=f"❌ Failed to set data source to {data_source}: {result.get('error', 'Unknown error')}")]
            except Exception as e:
                print(f"❌ Error in set_data_source: {str(e)}", file=sys.stderr)
                return [TextContent(type="text", text=f"❌ Error setting data source: {str(e)}")]
                
        elif name == "get_data_source_status":
            try:
                # Get current data source configuration
                status = orchestrator.get_current_data_source()
                
                result = f"""📊 Data Source Configuration
{'='*40}

🔧 Current Data Source: {status['current_data_source']}
📁 Using Static Data: {status['use_static_data']}

📋 Available Data Sources:
• static: Sample database (5 demo customers) ✅ Always available
• airtable: Airtable database {'✅ Configured' if status['configuration']['airtable_configured'] else '❌ Not configured'}
• hubspot: HubSpot CRM {'✅ Configured' if status['configuration']['hubspot_configured'] else '❌ Not configured'} (Coming soon)
• zapier: Zapier integration {'✅ Configured' if status['configuration']['zapier_configured'] else '❌ Not configured'} (Coming soon)

💡 Use set_data_source tool to switch between available data sources.
📖 See AIRTABLE_SETUP.md for Airtable configuration instructions."""

                return [TextContent(type="text", text=result)]
            except Exception as e:
                print(f"❌ Error in get_data_source_status: {str(e)}", file=sys.stderr)
                return [TextContent(type="text", text=f"❌ Error getting data source status: {str(e)}")]
                
        elif name == "analyze_customer_health":
            try:
                customer_id = arguments.get("customer_id")
                format_type = arguments.get("format", "detailed")
                
                print(f"🔧 Starting health analysis for: {'all customers' if not customer_id else customer_id}", file=sys.stderr)
                
                # Run analysis using CrewAI orchestrator with timeout protection
                health_scores = await asyncio.wait_for(
                    orchestrator.analyze_customer_health(
                        customer_identifier=customer_id if customer_id else "all",
                        identifier_type="id" if customer_id else "all"
                    ),
                    timeout=120.0  # 2 minute timeout
                )
                
                print(f"🔧 Health analysis completed successfully", file=sys.stderr)
                
            except asyncio.TimeoutError:
                print(f"⏰ Health analysis timed out after 2 minutes", file=sys.stderr)
                return [TextContent(type="text", text="❌ Analysis timed out. Please try again or contact support.")]
            except Exception as analysis_error:
                print(f"❌ Health analysis failed: {str(analysis_error)}", file=sys.stderr)
                import traceback
                traceback.print_exc(file=sys.stderr)
                return [TextContent(type="text", text=f"❌ Analysis failed: {str(analysis_error)}")]
            
            if not health_scores:
                return [TextContent(type="text", text="No customer data found or analysis failed.")]
            
            if format_type == "summary":
                # Return summary only
                try:
                    summary = orchestrator.generate_summary_report(health_scores)
                    return [TextContent(type="text", text=summary)]
                except Exception as e:
                    return [TextContent(type="text", text=f"❌ Error generating summary: {str(e)}")]
            else:
                # Return detailed analysis
                try:
                    results = []
                    
                    # Add summary first
                    summary = orchestrator.generate_summary_report(health_scores)
                    results.append(f"📊 {summary}\n" + "="*50 + "\n")
                    
                    # Add detailed customer analysis
                    for score in sorted(health_scores, key=lambda x: x.overall_score):
                        status_emoji = {"healthy": "🟢", "at_risk": "🟡", "critical": "🔴"}
                        emoji = status_emoji.get(score.health_status.value, "⚪")
                        
                        result = f"""
{emoji} {score.company_name} (ID: {score.customer_id})
Health Score: {score.overall_score}/100 - {score.health_status.value.title()}

Detailed Scores:
• Usage: {score.usage_score}/100
• Relationship: {score.relationship_score}/100  
• Support: {score.support_score}/100

Reasoning: {score.reasoning}

🎯 Recommended Actions:"""
                        
                        for i, rec in enumerate(score.recommendations, 1):
                            priority_emoji = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "🟢"}
                            p_emoji = priority_emoji.get(rec.priority.value, "⚪")
                            result += f"""
{i}. {rec.action}
   Priority: {p_emoji} {rec.priority.value.upper()}
   Timeline: {rec.timeline}
   Reasoning: {rec.reasoning}"""
                        
                        results.append(result)
                    
                    return [TextContent(type="text", text="\n".join(results))]
                except Exception as e:
                    return [TextContent(type="text", text=f"❌ Error formatting detailed results: {str(e)}")]
        
        elif name == "list_customers":
            try:
                # Use the orchestrator's consistent data source routing
                print("🔧 Using orchestrator's data source routing for consistency...", file=sys.stderr)
                
                # Determine data sources using orchestrator logic (same as analyze_customer_health)
                if orchestrator.use_static_data or orchestrator.current_data_source == "static":
                    data_sources = ["static"]
                else:
                    data_sources = [orchestrator.current_data_source]
                
                print(f"🔧 Using data sources: {data_sources}", file=sys.stderr)
                
                # Use orchestrator's _collect_customer_data method for consistency
                customer_data = await orchestrator._collect_customer_data("all", data_sources)
                
                if "error" in customer_data:
                    return [TextContent(type="text", text=f"❌ Error collecting customer data: {customer_data['error']}")]
                
                # Handle different data formats from orchestrator
                if "customers" in customer_data and isinstance(customer_data["customers"], list):
                    # Airtable format - list of customer objects
                    customers_list = customer_data["customers"]
                    data_source_name = "Airtable"
                    
                    result = f"📋 Available Customers ({data_source_name}):\n" + "="*50 + "\n"
                    
                    for customer in customers_list:
                        name = customer.get("name", "Unknown Customer")
                        email = customer.get("email", "No email")
                        account_value = customer.get("account_value", 0)
                        customer_type = customer.get("customer_type", "Regular")
                        
                        # Format account value
                        if isinstance(account_value, (int, float)):
                            account_str = f"${account_value:,.0f}"
                        else:
                            account_str = f"${0:,.0f}"
                        
                        result += f"• {name} ({email})\n"
                        result += f"  Type: {customer_type} | Value: {account_str}\n\n"
                    
                    result += f"Total customers found: {len(customers_list)}"
                    
                elif "usage_data" in customer_data and isinstance(customer_data["usage_data"], list):
                    # Static format - separate data arrays
                    usage_data = customer_data.get("usage_data", [])
                    crm_data = customer_data.get("relationship_data", [])
                    data_source_name = "Static Data"
                    
                    # Get unique customers from all data sources
                    customers = set()
                    for data in [usage_data, crm_data]:
                        if data and isinstance(data, list):
                            customers.update([item["customer_id"] for item in data])
                    
                    if not customers:
                        return [TextContent(type="text", text="No customers found in dataset.")]
                    
                    # Create customer details mapping
                    crm_dict = {c["customer_id"]: c for c in crm_data} if crm_data else {}
                    
                    result = f"📋 Available Customers ({data_source_name}):\n" + "="*50 + "\n"
                    
                    for customer_id in sorted(customers):
                        crm = crm_dict.get(customer_id)
                        if crm:
                            result += f"• {customer_id}: {crm['company_name']} (${crm.get('account_value', 0):,.0f})\n"
                        else:
                            result += f"• {customer_id}: Unknown Company\n"
                    
                    result += f"\nTotal customers found: {len(customers)}"
                
                else:
                    return [TextContent(type="text", text="❌ Unexpected data format from orchestrator")]
                
                return [TextContent(type="text", text=result)]
            
            except Exception as e:
                print(f"❌ Error in list_customers: {str(e)}", file=sys.stderr)
                import traceback
                traceback.print_exc(file=sys.stderr)
                return [TextContent(type="text", text=f"❌ Error listing customers: {str(e)}")]
        
        elif name == "get_customer_details":
            try:
                customer_id = arguments.get("customer_id")
                if not customer_id:
                    return [TextContent(type="text", text="❌ Error: customer_id parameter is required")]
                
                print(f"🔧 Getting details for customer: {customer_id}", file=sys.stderr)
                
                # Use dynamic data collection with timeout
                customer_data = await asyncio.wait_for(
                    orchestrator._collect_customer_data(
                        customer_id, 
                        ["static"] if orchestrator.use_static_data or orchestrator.current_data_source == "static" else [orchestrator.current_data_source]
                    ),
                    timeout=60.0  # 1 minute timeout
                )
                
                if "error" in customer_data:
                    return [TextContent(type="text", text=f"No data found for customer {customer_id}: {customer_data['error']}")]
                
                usage_data = customer_data.get("usage_data")
                crm_data = customer_data.get("relationship_data") 
                support_data = customer_data.get("support_data")
                
                result = f"📊 Customer Details: {customer_id}\n" + "="*30 + "\n"
                
                # Usage details
                if usage_data:
                    result += f"""
📈 Usage Data:
• Total Logins: {usage_data.get('total_logins', 'N/A')}
• Avg Session Duration: {usage_data.get('avg_session_duration', 'N/A')} minutes
• Features Adopted: {usage_data.get('feature_adoption_count', 'N/A')}
• Usage Trend: {usage_data.get('usage_trend', 'N/A')}
• Last Activity: {usage_data.get('last_activity_date', 'N/A')}
"""
                
                # CRM details
                if crm_data:
                    result += f"""
🤝 CRM/Relationship Data:
• Account Value: ${crm_data.get('account_value', 0):,.0f}
• Last Contact: {crm_data.get('last_contact_date', 'N/A')}
• Contact Sentiment: {crm_data.get('contact_sentiment', 'N/A')}
• Contract Ends: {crm_data.get('contract_end_date', 'N/A')}
• CSM: {crm_data.get('csm_name', 'N/A')}
"""
                
                # Support details
                if support_data:
                    result += f"""
🎧 Support Data:
• Open Tickets: {support_data.get('open_tickets', 'N/A')}
• Avg Resolution Time: {support_data.get('avg_resolution_time', 'N/A')} hours
• Recent Sentiment: {support_data.get('recent_sentiment', 'N/A')}
• Escalated Issues: {support_data.get('escalated_issues', 'N/A')}
"""
                
                return [TextContent(type="text", text=result)]
                
            except asyncio.TimeoutError:
                print(f"⏰ Customer details timed out for {customer_id}", file=sys.stderr)
                return [TextContent(type="text", text="❌ Request timed out. Please try again.")]
            except Exception as details_error:
                print(f"❌ Error getting customer details: {str(details_error)}", file=sys.stderr)
                return [TextContent(type="text", text=f"❌ Error getting customer details: {str(details_error)}")]
        
        elif name == "get_recommendations":
            try:
                customer_id = arguments.get("customer_id")
                if not customer_id:
                    return [TextContent(type="text", text="❌ Error: customer_id parameter is required")]
                
                print(f"🔧 Getting recommendations for customer: {customer_id}", file=sys.stderr)
                
                # Run analysis for this customer using orchestrator with timeout
                health_scores = await asyncio.wait_for(
                    orchestrator.analyze_customer_health(customer_id, "id"),
                    timeout=60.0  # 1 minute timeout
                )
                
                if not health_scores:
                    return [TextContent(type="text", text=f"No data found for customer {customer_id}")]
                
                score = health_scores[0]
                
                result = f"🎯 Recommendations for {score.company_name} ({customer_id})\n"
                result += f"Current Health Score: {score.overall_score}/100 - {score.health_status.value.title()}\n"
                result += "="*50 + "\n"
                
                for i, rec in enumerate(score.recommendations, 1):
                    priority_emoji = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "🟢"}
                    p_emoji = priority_emoji.get(rec.priority.value, "⚪")
                    
                    result += f"""
{i}. {rec.action}
   Priority: {p_emoji} {rec.priority.value.upper()}
   Timeline: {rec.timeline}
   Reasoning: {rec.reasoning}
"""
                
                return [TextContent(type="text", text=result)]
                
            except asyncio.TimeoutError:
                print(f"⏰ Recommendations timed out for {customer_id}", file=sys.stderr)
                return [TextContent(type="text", text="❌ Request timed out. Please try again.")]
            except Exception as rec_error:
                print(f"❌ Error getting recommendations: {str(rec_error)}", file=sys.stderr)
                return [TextContent(type="text", text=f"❌ Error getting recommendations: {str(rec_error)}")]
        
        elif name == "discover_airtable_bases":
            try:
                if not AirtableDiscoveryTool:
                    return [TextContent(type="text", text="❌ Airtable discovery tool not available. Install pyairtable: pip install pyairtable")]
                
                print("🔍 Discovering all accessible Airtable bases...", file=sys.stderr)
                
                # Check if API key is configured
                api_key = os.getenv("AIRTABLE_API_KEY")
                if not api_key:
                    return [TextContent(type="text", text="❌ AIRTABLE_API_KEY not configured. Please set your Personal Access Token in environment variables.")]
                
                discovery_tool = AirtableDiscoveryTool(api_key)
                bases = discovery_tool.discover_all_bases()
                
                if not bases:
                    return [TextContent(type="text", text="❌ No accessible bases found. Check your API token permissions.")]
                
                result = f"🔍 Discovered Airtable Bases\n{'='*30}\n\n"
                result += f"Total accessible bases: {len(bases)}\n\n"
                
                for i, base in enumerate(bases, 1):
                    result += f"{i}. **{base.name}**\n"
                    result += f"   • Base ID: `{base.id}`\n"
                    result += f"   • Permission Level: {base.permission_level}\n\n"
                
                result += "💡 Use `discover_airtable_schema` with a Base ID to analyze specific base structure."
                
                return [TextContent(type="text", text=result)]
                
            except Exception as e:
                print(f"❌ Error discovering Airtable bases: {str(e)}", file=sys.stderr)
                return [TextContent(type="text", text=f"❌ Error discovering bases: {str(e)}")]
        
        elif name == "discover_airtable_schema":
            try:
                if not AirtableDiscoveryTool:
                    return [TextContent(type="text", text="❌ Airtable discovery tool not available. Install pyairtable: pip install pyairtable")]
                
                base_id = arguments.get("base_id")
                if not base_id:
                    return [TextContent(type="text", text="❌ Error: base_id parameter is required")]
                
                format_type = arguments.get("format", "detailed")
                
                print(f"🔍 Discovering schema for base: {base_id}", file=sys.stderr)
                
                # Check if API key is configured
                api_key = os.getenv("AIRTABLE_API_KEY")
                if not api_key:
                    return [TextContent(type="text", text="❌ AIRTABLE_API_KEY not configured. Please set your Personal Access Token in environment variables.")]
                
                discovery_tool = AirtableDiscoveryTool(api_key)
                
                if format_type == "json":
                    # Return JSON format
                    schema_data = discovery_tool.export_schema_json(base_id)
                    if not schema_data:
                        return [TextContent(type="text", text=f"❌ Could not discover schema for base {base_id}")]
                    
                    import json
                    return [TextContent(type="text", text=f"```json\n{json.dumps(schema_data, indent=2)}\n```")]
                
                elif format_type == "summary":
                    # Return brief summary
                    base_info = discovery_tool.discover_base_schema(base_id)
                    if not base_info:
                        return [TextContent(type="text", text=f"❌ Could not discover schema for base {base_id}")]
                    
                    result = f"📊 Base Schema Summary\n{'='*25}\n\n"
                    result += f"**Base:** {base_info.name} (`{base_info.id}`)\n"
                    result += f"**Tables:** {len(base_info.tables)}\n\n"
                    
                    # Find customer tables
                    customer_tables = discovery_tool.find_customer_tables(base_id)
                    if customer_tables:
                        result += "🎯 **Recommended Customer Tables:**\n"
                        for table, score in customer_tables[:3]:
                            result += f"• {table.name} (confidence: {score:.1f}%)\n"
                    
                    result += f"\n📋 **All Tables:**\n"
                    for table in base_info.tables:
                        result += f"• {table.name} ({len(table.fields)} fields)\n"
                    
                    return [TextContent(type="text", text=result)]
                
                else:
                    # Return detailed report
                    report = discovery_tool.generate_discovery_report(base_id)
                    if not report or report.startswith("❌"):
                        return [TextContent(type="text", text=f"❌ Could not generate discovery report for base {base_id}")]
                    
                    return [TextContent(type="text", text=report)]
                
            except Exception as e:
                print(f"❌ Error discovering Airtable schema: {str(e)}", file=sys.stderr)
                return [TextContent(type="text", text=f"❌ Error discovering schema: {str(e)}")]
        
        elif name == "find_airtable_customer_tables":
            try:
                if not AirtableDiscoveryTool:
                    return [TextContent(type="text", text="❌ Airtable discovery tool not available. Install pyairtable: pip install pyairtable")]
                
                base_id = arguments.get("base_id")
                if not base_id:
                    return [TextContent(type="text", text="❌ Error: base_id parameter is required")]
                
                print(f"🔍 Finding customer tables in base: {base_id}", file=sys.stderr)
                
                # Check if API key is configured
                api_key = os.getenv("AIRTABLE_API_KEY")
                if not api_key:
                    return [TextContent(type="text", text="❌ AIRTABLE_API_KEY not configured. Please set your Personal Access Token in environment variables.")]
                
                discovery_tool = AirtableDiscoveryTool(api_key)
                customer_tables = discovery_tool.find_customer_tables(base_id)
                
                if not customer_tables:
                    return [TextContent(type="text", text=f"❌ No customer tables found in base {base_id}")]
                
                result = f"🎯 Customer Tables in Base {base_id}\n{'='*40}\n\n"
                
                for i, (table, confidence) in enumerate(customer_tables, 1):
                    confidence_emoji = "🟢" if confidence >= 80 else "🟡" if confidence >= 60 else "🟠" if confidence >= 40 else "🔴"
                    
                    result += f"{i}. {confidence_emoji} **{table.name}** (Confidence: {confidence:.1f}%)\n"
                    result += f"   • Fields: {len(table.fields)}\n"
                    result += f"   • Primary Field: {table.primary_field or 'Unknown'}\n"
                    
                    # Show key customer-related fields
                    customer_fields = []
                    for field in table.fields[:5]:  # Show first 5 fields
                        if any(keyword in field.name.lower() for keyword in ['email', 'name', 'customer', 'company']):
                            customer_fields.append(f"{field.name} ({field.field_type})")
                    
                    if customer_fields:
                        result += f"   • Key Fields: {', '.join(customer_fields)}\n"
                    
                    result += "\n"
                
                result += "💡 **Confidence Score Guide:**\n"
                result += "• 🟢 80-100%: Highly likely customer table\n"
                result += "• 🟡 60-79%: Probably customer table\n"
                result += "• 🟠 40-59%: Possibly customer table\n"
                result += "• 🔴 0-39%: Unlikely customer table\n\n"
                result += "Use `set_data_source airtable` then `list_customers` to test the recommended table."
                
                return [TextContent(type="text", text=result)]
                
            except Exception as e:
                print(f"❌ Error finding customer tables: {str(e)}", file=sys.stderr)
                return [TextContent(type="text", text=f"❌ Error finding customer tables: {str(e)}")]
        
        elif name == "connect_to_airtable_base":
            try:
                base_id = arguments.get("base_id")
                if not base_id:
                    return [TextContent(type="text", text="❌ Error: base_id parameter is required")]
                
                print(f"🔗 Connecting to Airtable base: {base_id}", file=sys.stderr)
                
                # Use orchestrator to connect to the base
                result = orchestrator.connect_to_airtable_base(base_id)
                
                if result.get("success"):
                    return [TextContent(type="text", text=result.get("message", "Successfully connected to Airtable base!"))]
                else:
                    return [TextContent(type="text", text=f"❌ Failed to connect to base: {result.get('error', 'Unknown error')}")]
                
            except Exception as e:
                print(f"❌ Error connecting to Airtable base: {str(e)}", file=sys.stderr)
                return [TextContent(type="text", text=f"❌ Error connecting to base: {str(e)}")]
        
        elif name == "get_current_airtable_base":
            try:
                print("📊 Getting current Airtable base info...", file=sys.stderr)
                
                # Get current base info from orchestrator
                base_info = orchestrator.get_current_airtable_base()
                
                if not base_info.get("connected"):
                    result = "📊 **Airtable Base Status**\n" + "="*25 + "\n\n"
                    result += "❌ **Not Connected**\n\n"
                    result += "No Airtable base is currently connected.\n\n"
                    result += "**To get started:**\n"
                    result += "1. Use `discover_airtable_bases` to see available bases\n"
                    result += "2. Use `connect_to_airtable_base` with a base ID to connect\n"
                    result += "3. Then use `list_customers`, `analyze_customer_health`, etc.\n\n"
                    if orchestrator.active_airtable_base_id:
                        result += f"💡 Default base from .env: `{orchestrator.active_airtable_base_id}`"
                else:
                    result = "📊 **Current Airtable Base**\n" + "="*25 + "\n\n"
                    result += f"✅ **Connected to Base**\n\n"
                    result += f"• **Base ID:** `{base_info['base_id']}`\n"
                    
                    if "base_name" in base_info:
                        result += f"• **Name:** {base_info['base_name']}\n"
                        result += f"• **Tables:** {base_info['table_count']}\n"
                        result += f"• **Permission Level:** {base_info['permission_level']}\n"
                    
                    result += f"• **Active Data Source:** {'✅ Yes' if base_info['is_active_source'] else '❌ No (use set_data_source airtable)'}\n\n"
                    
                    result += "**Available Operations:**\n"
                    result += "• `list_customers` - See customers in this base\n"
                    result += "• `analyze_customer_health` - Analyze customer health\n"
                    result += "• `get_customer_details` - Get customer details\n"
                    result += "• `connect_to_airtable_base` - Switch to different base"
                
                return [TextContent(type="text", text=result)]
                
            except Exception as e:
                print(f"❌ Error getting current Airtable base: {str(e)}", file=sys.stderr)
                return [TextContent(type="text", text=f"❌ Error getting base info: {str(e)}")]
        
        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]
    
    except Exception as e:
        print(f"❌ Critical error executing tool {name}: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return [TextContent(type="text", text=f"❌ Critical error executing tool {name}: {str(e)}")]

@server.call_tool()
async def handle_call_tool_decorator(name: str, arguments: dict) -> list[TextContent]:
    """Decorated version of the tool handler for proper MCP registration"""
    return await handle_call_tool(name, arguments)

async def main():
    """Main entry point for the MCP server with enhanced stability"""
    # Add debug output
    print("🚀 Customer Health Analyzer MCP Server starting...", file=sys.stderr)
    print(f"🐍 Python version: {sys.version}", file=sys.stderr)
    print(f"📁 Working directory: {os.getcwd()}", file=sys.stderr)
    
    # Suppress CrewAI and other verbose output to prevent JSON-RPC corruption
    import logging
    os.environ["CREWAI_DISABLE_TELEMETRY"] = "true"
    os.environ["OPENAI_LOG_LEVEL"] = "ERROR"
    
    logging.getLogger("crewai").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    
    # Set up basic logging for debugging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stderr)
    
    try:
        # Run the server using stdio transport with improved error handling
        async with stdio_server() as (read_stream, write_stream):
            print("🔧 Server capabilities initialized", file=sys.stderr)
            print("📡 Starting MCP server communication...", file=sys.stderr)
            
            # Add connection monitoring
            print("💡 Server is ready to handle requests from Claude Desktop", file=sys.stderr)
            
            await server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="customer-health-analyzer",
                    server_version="1.0.0",
                    capabilities=server.get_capabilities(
                        notification_options=NotificationOptions(),
                        experimental_capabilities={}
                    ),
                ),
            )
            
    except KeyboardInterrupt:
        print("🔴 Server stopped by user (Ctrl+C)", file=sys.stderr)
    except Exception as e:
        print(f"🔴 Server crashed with error: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        # Don't exit, just log the error and continue
        print("🔄 Server attempting graceful recovery...", file=sys.stderr)

if __name__ == "__main__":
    asyncio.run(main())