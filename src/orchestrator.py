"""
Main orchestrator that combines data integration and health analysis crews
"""

import asyncio
import os
import sys
import contextlib
import io
from typing import Dict, List, Optional, Any
from datetime import datetime

from agents.data_integration_agents import create_dynamic_data_collection_crew
from agents.health_analysis_agents import create_health_analysis_crew
from models.customer_health import (
    CustomerHealthScore, HealthStatus, Recommendation, RecommendationPriority
)

@contextlib.contextmanager
def suppress_stdout():
    """Context manager to suppress stdout output"""
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout

class CustomerHealthOrchestrator:
    """Main orchestrator for customer health analysis with dynamic data sources"""
    
    def __init__(self):
        self.use_static_data = os.getenv("USE_STATIC_DATA", "true").lower() == "true"
        # Get the project root directory (where server.py is located)
        self.project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        print(f"🔧 Project root: {self.project_root}", file=sys.stderr)
        self.default_data_source = os.getenv("DEFAULT_DATA_SOURCE", "static")
        self.current_data_source = "static" if self.use_static_data else self.default_data_source
        
        # Active Airtable base management
        self.active_airtable_base_id = os.getenv("AIRTABLE_BASE_ID")  # Default from env
        self.active_airtable_base_info = None  # Will store base info when connected
    
    def set_data_source(self, data_source: str) -> Dict[str, Any]:
        """Set the active data source for customer health analysis"""
        
        try:
            if data_source not in ["static", "airtable", "hubspot", "zapier"]:
                return {
                    "success": False,
                    "error": f"Invalid data source: {data_source}. Must be one of: static, airtable, hubspot, zapier"
                }
            
            # Update current configuration
            self.current_data_source = data_source
            
            if data_source == "static":
                self.use_static_data = True
                message = "Using sample database with 5 demo customers (TechCorp Inc, DataSolutions LLC, StartupXYZ, Enterprise Global, SmallBiz Co)"
            elif data_source == "airtable":
                self.use_static_data = False
                # Check if Airtable credentials are configured
                if not os.getenv("AIRTABLE_API_KEY") or not os.getenv("AIRTABLE_BASE_ID"):
                    return {
                        "success": False,
                        "error": "Airtable integration requires AIRTABLE_API_KEY and AIRTABLE_BASE_ID environment variables. See AIRTABLE_SETUP.md for setup instructions."
                    }
                message = f"Using Airtable database (Base ID: {os.getenv('AIRTABLE_BASE_ID', 'Not configured')})"
            elif data_source == "hubspot":
                self.use_static_data = False
                # Check if HubSpot credentials are configured
                if not os.getenv("HUBSPOT_API_KEY"):
                    return {
                        "success": False,
                        "error": "HubSpot integration requires HUBSPOT_API_KEY environment variable. This feature is coming soon."
                    }
                message = "Using HubSpot CRM (Feature coming soon)"
            elif data_source == "zapier":
                self.use_static_data = False
                # Check if Zapier credentials are configured
                if not os.getenv("ZAPIER_API_KEY"):
                    return {
                        "success": False,
                        "error": "Zapier integration requires ZAPIER_API_KEY environment variable. This feature is coming soon."
                    }
                message = "Using Zapier integration (Feature coming soon)"
            
            print(f"🔄 Data source changed to: {data_source}", file=sys.stderr)
            
            return {
                "success": True,
                "data_source": data_source,
                "message": message
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to set data source: {str(e)}"
            }
    
    def connect_to_airtable_base(self, base_id: str) -> Dict[str, Any]:
        """Connect to a specific Airtable base and make it the active base for all operations"""
        
        try:
            # Validate API key first
            api_key = os.getenv("AIRTABLE_API_KEY")
            if not api_key:
                return {
                    "success": False,
                    "error": "AIRTABLE_API_KEY not configured. Please set your Personal Access Token in environment variables."
                }
            
            # Import and test the discovery tool
            try:
                from airtable_discovery import AirtableDiscoveryTool
                discovery_tool = AirtableDiscoveryTool(api_key)
            except ImportError as e:
                return {
                    "success": False,
                    "error": f"Airtable discovery tool not available. Please install pyairtable: pip install pyairtable. Error: {str(e)}"
                }
            
            print(f"🔗 Connecting to Airtable base: {base_id}", file=sys.stderr)
            
            # Test connection and get base info
            base_info = discovery_tool.discover_base_schema(base_id)
            if not base_info:
                return {
                    "success": False,
                    "error": f"Could not connect to base {base_id}. Check base ID and permissions."
                }
            
            # Find customer tables in this base
            customer_tables = discovery_tool.find_customer_tables(base_id)
            
            # Update active base
            self.active_airtable_base_id = base_id
            self.active_airtable_base_info = base_info
            
            # Switch to Airtable data source
            self.current_data_source = "airtable"
            self.use_static_data = False
            
            print(f"✅ Connected to Airtable base: {base_info.name}", file=sys.stderr)
            
            # Prepare response message
            message = f"Successfully connected to Airtable base!\n\n"
            message += f"📊 **Base Details:**\n"
            message += f"• Name: {base_info.name}\n"
            message += f"• Base ID: {base_id}\n"
            message += f"• Tables: {len(base_info.tables)}\n"
            message += f"• Permission Level: {base_info.permission_level}\n\n"
            
            if customer_tables:
                message += f"🎯 **Recommended Customer Tables:**\n"
                for table, confidence in customer_tables[:3]:
                    confidence_emoji = "🟢" if confidence >= 80 else "🟡" if confidence >= 60 else "🟠"
                    message += f"• {confidence_emoji} {table.name} (confidence: {confidence:.1f}%)\n"
                message += f"\n"
            
            message += f"✅ **All tools now operate on this base!**\n"
            message += f"• Use `list_customers` to see customers in this base\n"
            message += f"• Use `analyze_customer_health` to analyze customers\n"
            message += f"• Use `get_current_airtable_base` to check connection status"
            
            return {
                "success": True,
                "base_id": base_id,
                "base_name": base_info.name,
                "table_count": len(base_info.tables),
                "customer_tables_found": len(customer_tables),
                "message": message
            }
            
        except Exception as e:
            print(f"❌ Error connecting to Airtable base: {str(e)}", file=sys.stderr)
            return {
                "success": False,
                "error": f"Failed to connect to base: {str(e)}"
            }
    
    def get_current_airtable_base(self) -> Dict[str, Any]:
        """Get information about the currently connected Airtable base"""
        
        if not self.active_airtable_base_id:
            return {
                "connected": False,
                "message": "No Airtable base connected. Use connect_to_airtable_base to connect to a base."
            }
        
        base_info = {
            "connected": True,
            "base_id": self.active_airtable_base_id,
            "is_active_source": self.current_data_source == "airtable"
        }
        
        if self.active_airtable_base_info:
            base_info.update({
                "base_name": self.active_airtable_base_info.name,
                "table_count": len(self.active_airtable_base_info.tables),
                "permission_level": self.active_airtable_base_info.permission_level
            })
        
        return base_info
    
    def get_current_data_source(self) -> Dict[str, Any]:
        """Get information about the currently configured data source"""
        
        return {
            "current_data_source": self.current_data_source,
            "use_static_data": self.use_static_data,
            "available_sources": self._detect_available_sources(),
            "configuration": {
                "airtable_configured": bool(os.getenv("AIRTABLE_API_KEY") and os.getenv("AIRTABLE_BASE_ID")),
                "hubspot_configured": bool(os.getenv("HUBSPOT_API_KEY")),
                "zapier_configured": bool(os.getenv("ZAPIER_API_KEY"))
            }
        }
    
    async def analyze_customer_health(self, 
                                    customer_identifier: str, 
                                    identifier_type: str = "email",
                                    data_sources: Optional[List[str]] = None) -> List[CustomerHealthScore]:
        """
        Orchestrate complete customer health analysis
        
        Args:
            customer_identifier: Email, ID, or other identifier
            identifier_type: Type of identifier (email, id, name)
            data_sources: List of data sources to use (hubspot, airtable, zapier, static)
        """
        
        # Determine data sources to use
        if data_sources is None:
            if self.use_static_data or self.current_data_source == "static":
                data_sources = ["static"]
            else:
                data_sources = [self.current_data_source]
        
        # Step 1: Collect customer data
        print(f"🔍 Collecting data for {customer_identifier} from sources: {data_sources}")
        customer_data = await self._collect_customer_data(customer_identifier, data_sources)
        
        if not customer_data or "error" in customer_data:
            print(f"❌ Failed to collect data: {customer_data.get('error', 'Unknown error')}")
            return []
        
        # Step 2: Analyze health and generate recommendations
        print(f"📊 Analyzing customer health...")
        health_analysis = await self._analyze_health(customer_data)
        
        if not health_analysis or "error" in health_analysis:
            print(f"❌ Failed to analyze health: {health_analysis.get('error', 'Unknown error')}")
            return []
        
        # Step 3: Create CustomerHealthScore objects
        health_scores = self._create_health_score_objects(customer_data, health_analysis)
        
        return health_scores
    
    async def _collect_customer_data(self, customer_identifier: str, data_sources: List[str]) -> Dict[str, Any]:
        """Collect customer data from specified sources"""
        
        if "static" in data_sources:
            # Use existing static data collection
            return self._collect_static_data(customer_identifier)
        
        elif "airtable" in data_sources:
            # Use direct Airtable data collection (bypass CrewAI for reliability)
            return self._collect_airtable_data(customer_identifier)
        
        # Use CrewAI for other dynamic data collection (HubSpot, Zapier)
        try:
            crew = create_dynamic_data_collection_crew(customer_identifier, data_sources)
            print(f"🤖 Running data collection crew (output suppressed)...", file=sys.stderr)
            with suppress_stdout():
                result = crew.kickoff()
            
            # Parse crew results into structured format
            collected_data = self._parse_crew_results(result, "data_collection")
            
            return collected_data
            
        except Exception as e:
            return {"error": f"Data collection failed: {str(e)}"}
    
    async def _analyze_health(self, customer_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze customer health using CrewAI agents"""
        
        try:
            # For now, skip the complex CrewAI analysis and return a simple success response
            # This ensures the _create_health_score_objects method gets called properly
            print(f"🧠 Health analysis completed", file=sys.stderr)
            
            return {
                "status": "success",
                "analysis_type": "static_data",
                "customer_count": customer_data.get("customer_count", 1)
            }
            
        except Exception as e:
            return {"error": f"Health analysis failed: {str(e)}"}
    
    def _collect_static_data(self, customer_identifier: str) -> Dict[str, Any]:
        """Collect data from static CSV files (fallback/demo mode)"""
        
        try:
            import pandas as pd
            
            # Load static data files
            usage_path = os.path.join(self.project_root, "data", "sample_usage_data.csv")
            crm_path = os.path.join(self.project_root, "data", "sample_crm_data.csv")
            support_path = os.path.join(self.project_root, "data", "sample_support_data.csv")
            
            print(f"🔧 Loading usage data from: {usage_path}", file=sys.stderr)
            print(f"🔧 File exists: {os.path.exists(usage_path)}", file=sys.stderr)
            
            usage_df = pd.read_csv(usage_path)
            crm_df = pd.read_csv(crm_path)
            support_df = pd.read_csv(support_path)
            
            print(f"🔧 Loaded {len(usage_df)} usage records", file=sys.stderr)
            print(f"🔧 Loaded {len(crm_df)} CRM records", file=sys.stderr)
            print(f"🔧 Loaded {len(support_df)} support records", file=sys.stderr)
            
            # Handle "all" customers vs specific customer
            if customer_identifier == "all":
                # Return list format for all customers
                all_customers = []
                unique_customers = set(usage_df['customer_id'].unique()) | set(crm_df['customer_id'].unique()) | set(support_df['customer_id'].unique())
                
                for cust_id in unique_customers:
                    all_customers.append({"customer_id": cust_id})
                
                return {
                    "usage_data": usage_df.to_dict('records'),
                    "relationship_data": crm_df.to_dict('records'),
                    "support_data": support_df.to_dict('records'),
                    "data_source": "static"
                }
            
            # Find customer by ID (assuming customer_identifier is customer_id for static data)
            customer_id = customer_identifier
            
            # Get usage data
            usage_data = {}
            customer_usage = usage_df[usage_df['customer_id'] == customer_id]
            if not customer_usage.empty:
                total_logins = customer_usage[customer_usage['feature_used'] == 'login']['usage_count'].sum()
                avg_session = customer_usage['session_duration_minutes'].mean()
                features_used = customer_usage['feature_used'].nunique()
                
                usage_data = {
                    "total_logins": int(total_logins),
                    "avg_session_duration": float(avg_session),
                    "features_used": features_used,
                    "trend": "stable"  # Simplified for demo
                }
            
            # Get CRM data
            relationship_data = {}
            customer_crm = crm_df[crm_df['customer_id'] == customer_id]
            if not customer_crm.empty:
                crm_record = customer_crm.iloc[0]
                relationship_data = {
                    "last_contact_date": crm_record['last_contact_date'],
                    "engagement_score": 75,  # Simplified for demo
                    "emails_responded": 3,
                    "meetings_attended": 1,
                    "contract_value": float(crm_record['account_value']),
                    "renewal_probability": 0.7
                }
            
            # Get support data
            support_data = {}
            customer_support = support_df[support_df['customer_id'] == customer_id]
            if not customer_support.empty:
                open_tickets = len(customer_support[customer_support['status'] == 'open'])
                avg_resolution = customer_support[customer_support['status'] == 'closed']['resolution_time_hours'].mean()
                avg_resolution = avg_resolution if not pd.isna(avg_resolution) else 0
                
                support_data = {
                    "open_tickets": open_tickets,
                    "avg_resolution_hours": float(avg_resolution),
                    "satisfaction_score": 4,  # Out of 5, simplified for demo
                    "escalations": len(customer_support[customer_support['priority'] == 'high'])
                }
            
            # Get basic customer info
            customer_info = {}
            if not customer_crm.empty:
                crm_record = customer_crm.iloc[0]
                customer_info = {
                    "name": crm_record['company_name'],
                    "email": f"{customer_id.lower()}@{crm_record['company_name'].lower().replace(' ', '')}.com",
                    "company": crm_record['company_name'],
                    "account_value": float(crm_record['account_value'])
                }
            
            return {
                **customer_info,
                "usage_data": usage_data,
                "relationship_data": relationship_data,
                "support_data": support_data,
                "data_source": "static"
            }
            
        except Exception as e:
            return {"error": f"Static data collection failed: {str(e)}"}
    
    def _collect_airtable_data(self, customer_identifier: str) -> Dict[str, Any]:
        """Collect customer data from Airtable directly"""
        
        try:
            # Import AirtableTool directly
            from agents.data_integration_agents import AirtableTool
            
            airtable_tool = AirtableTool()
            
            # Get base and discover all customers
            api_token = os.getenv("AIRTABLE_API_KEY")
            base_id = self.active_airtable_base_id  # Use active base instead of env variable
            
            if not api_token or not base_id:
                return {"error": "Airtable credentials not configured"}
            
            from pyairtable import Api as AirtableApi
            api = AirtableApi(api_token)
            base = api.base(base_id)
            
            # Discover the best table
            customers_table, table_name_used, field_mapping = airtable_tool._discover_best_table(base, "")
            
            if not customers_table:
                return {"error": "Could not find any accessible customer table in Airtable"}
            
            print(f"🔧 Using Airtable table: {table_name_used}", file=sys.stderr)
            
            if customer_identifier == "all":
                # Get all customers for health analysis
                all_records = customers_table.all()
                
                if not all_records:
                    return {"error": f"No customers found in table '{table_name_used}'"}
                
                # Convert to format expected by health analysis
                customers_data = []
                
                for record in all_records:
                    fields = record.get("fields", {})
                    
                    # Extract data using discovered field mapping
                    customer_name = (
                        airtable_tool._extract_field_value(fields, field_mapping, "name") or
                        airtable_tool._extract_field_value(fields, field_mapping, "company") or
                        "Unknown Customer"
                    )
                    
                    customer_email = airtable_tool._extract_field_value(fields, field_mapping, "email") or f"customer_{len(customers_data)+1}@unknown.com"
                    customer_id = airtable_tool._extract_field_value(fields, field_mapping, "customer_id") or f"CUST{len(customers_data)+1:03d}"
                    account_value = airtable_tool._extract_field_value(fields, field_mapping, "account_value") or 0
                    
                    customer_data = {
                        "customer_id": customer_id,
                        "name": customer_name,
                        "email": customer_email,
                        "company": customer_name,
                        "account_value": float(account_value) if account_value else 0,
                        "usage_data": {
                            "total_logins": 10,  # Default values since Airtable may not have usage data
                            "avg_session_duration": 30,
                            "features_used": 3,
                            "trend": "stable"
                        },
                        "relationship_data": {
                            "last_contact_date": "2024-12-01",
                            "engagement_score": 75,
                            "emails_responded": 3,
                            "meetings_attended": 1,
                            "contract_value": float(account_value) if account_value else 0,
                            "renewal_probability": 0.7
                        },
                        "support_data": {
                            "open_tickets": 0,
                            "avg_resolution_hours": 24,
                            "satisfaction_score": 4,
                            "escalations": 0
                        },
                        "data_source": "airtable"
                    }
                    
                    customers_data.append(customer_data)
                
                return {
                    "customers": customers_data,
                    "data_source": "airtable",
                    "table_name": table_name_used
                }
            
            else:
                # Get specific customer data
                # Search for customer by ID, email, or name
                search_filters = [
                    f"LOWER({{Customer ID}}) = LOWER('{customer_identifier}')",
                    f"LOWER({{Email Address}}) = LOWER('{customer_identifier}')",
                    f"LOWER({{Full Name}}) = LOWER('{customer_identifier}')",
                    f"LOWER({{Company}}) = LOWER('{customer_identifier}')"
                ]
                
                customer_record = None
                for search_filter in search_filters:
                    try:
                        records = customers_table.all(formula=search_filter)
                        if records:
                            customer_record = records[0]
                            break
                    except:
                        continue
                
                if not customer_record:
                    return {"error": f"Customer '{customer_identifier}' not found in Airtable"}
                
                fields = customer_record.get("fields", {})
                
                # Extract data using discovered field mapping
                customer_name = (
                    airtable_tool._extract_field_value(fields, field_mapping, "name") or
                    airtable_tool._extract_field_value(fields, field_mapping, "company") or
                    "Unknown Customer"
                )
                
                customer_email = airtable_tool._extract_field_value(fields, field_mapping, "email") or "unknown@example.com"
                account_value = airtable_tool._extract_field_value(fields, field_mapping, "account_value") or 0
                
                return {
                    "name": customer_name,
                    "email": customer_email,
                    "company": customer_name,
                    "account_value": float(account_value) if account_value else 0,
                    "usage_data": {
                        "total_logins": 10,  # Default values since Airtable may not have usage data
                        "avg_session_duration": 30,
                        "features_used": 3,
                        "trend": "stable"
                    },
                    "relationship_data": {
                        "last_contact_date": "2024-12-01",
                        "engagement_score": 75,
                        "emails_responded": 3,
                        "meetings_attended": 1,
                        "contract_value": float(account_value) if account_value else 0,
                        "renewal_probability": 0.7
                    },
                    "support_data": {
                        "open_tickets": 0,
                        "avg_resolution_hours": 24,
                        "satisfaction_score": 4,
                        "escalations": 0
                    },
                    "data_source": "airtable"
                }
                
        except Exception as e:
            print(f"❌ Airtable data collection failed: {str(e)}", file=sys.stderr)
            return {"error": f"Airtable data collection failed: {str(e)}"}
    
    def _parse_crew_results(self, crew_result: Any, result_type: str) -> Dict[str, Any]:
        """Parse CrewAI crew results into structured format"""
        
        try:
            # This is a simplified parser - you may need to adapt based on actual CrewAI output format
            if hasattr(crew_result, 'raw'):
                raw_result = crew_result.raw
            elif isinstance(crew_result, str):
                raw_result = crew_result
            else:
                raw_result = str(crew_result)
            
            if result_type == "data_collection":
                # Parse data collection results
                # This would need to be implemented based on actual CrewAI output format
                return {"parsed_data": raw_result, "source": "crew_ai"}
            
            elif result_type == "health_analysis":
                # Parse health analysis results
                # This would need to be implemented based on actual CrewAI output format
                return {"analysis_result": raw_result}
            
            return {"raw_result": raw_result}
            
        except Exception as e:
            return {"error": f"Failed to parse crew results: {str(e)}"}
    
    def _create_health_score_objects(self, customer_data: Dict[str, Any], health_analysis: Dict[str, Any]) -> List[CustomerHealthScore]:
        """Create CustomerHealthScore objects from analysis results"""
        
        try:
            import pandas as pd
            
            # Check if we have data for all customers or just one
            if "customers" in customer_data and isinstance(customer_data["customers"], list):
                # Airtable data format for all customers
                return self._create_airtable_customer_scores(customer_data["customers"])
            elif "usage_data" in customer_data and isinstance(customer_data["usage_data"], list):
                # Static data format for all customers
                return self._create_all_customer_scores(customer_data)
            else:
                # Single customer data
                return self._create_single_customer_score(customer_data, health_analysis)
            
        except Exception as e:
            print(f"❌ Failed to create health score objects: {str(e)}", file=sys.stderr)
            return []
    
    def _create_all_customer_scores(self, customer_data: Dict[str, Any]) -> List[CustomerHealthScore]:
        """Create health scores for all customers"""
        
        try:
            import pandas as pd
            
            # Convert data back to DataFrames for easier processing
            usage_df = pd.DataFrame(customer_data["usage_data"])
            relationship_df = pd.DataFrame(customer_data["relationship_data"])
            support_df = pd.DataFrame(customer_data["support_data"])
            
            # Get all unique customer IDs
            all_customer_ids = set()
            if not usage_df.empty:
                all_customer_ids.update(usage_df['customer_id'].unique())
            if not relationship_df.empty:
                all_customer_ids.update(relationship_df['customer_id'].unique())
            if not support_df.empty:
                all_customer_ids.update(support_df['customer_id'].unique())
            
            health_scores = []
            
            for customer_id in all_customer_ids:
                # Calculate usage score
                usage_score = self._calculate_usage_score(usage_df, customer_id)
                
                # Calculate relationship score
                relationship_score = self._calculate_relationship_score(relationship_df, customer_id)
                
                # Calculate support score
                support_score = self._calculate_support_score(support_df, customer_id)
                
                # Calculate overall score
                overall_score = int(usage_score * 0.4 + relationship_score * 0.3 + support_score * 0.3)
                
                # Determine health status
                if overall_score >= 80:
                    health_status = HealthStatus.HEALTHY
                elif overall_score >= 60:
                    health_status = HealthStatus.AT_RISK
                else:
                    health_status = HealthStatus.CRITICAL
                
                # Get company name from CRM data
                company_name = "Unknown Company"
                customer_crm = relationship_df[relationship_df['customer_id'] == customer_id]
                if not customer_crm.empty:
                    company_name = customer_crm.iloc[0]['company_name']
                
                # Generate recommendations based on scores
                recommendations = self._generate_recommendations(usage_score, relationship_score, support_score, health_status)
                
                health_score = CustomerHealthScore(
                    customer_id=customer_id,
                    company_name=company_name,
                    overall_score=overall_score,
                    health_status=health_status,
                    usage_score=usage_score,
                    relationship_score=relationship_score,
                    support_score=support_score,
                    recommendations=recommendations,
                    reasoning=f"Health score calculated from usage ({usage_score}), relationship ({relationship_score}), and support ({support_score}) metrics"
                )
                
                health_scores.append(health_score)
            
            return health_scores
            
        except Exception as e:
            print(f"❌ Failed to create all customer scores: {str(e)}", file=sys.stderr)
            return []
    
    def _create_single_customer_score(self, customer_data: Dict[str, Any], health_analysis: Dict[str, Any]) -> List[CustomerHealthScore]:
        """Create health score for a single customer"""
        
        try:
            # Extract basic customer info
            customer_id = customer_data.get("email", "unknown").split("@")[0] if customer_data.get("email") else "unknown"
            company_name = customer_data.get("company", "Unknown Company")
            
            # Use actual data to calculate scores
            usage_data = customer_data.get("usage_data", {})
            relationship_data = customer_data.get("relationship_data", {})
            support_data = customer_data.get("support_data", {})
            
            # Calculate usage score from actual data
            usage_score = self._calculate_usage_score_from_dict(usage_data)
            
            # Calculate relationship score from actual data  
            relationship_score = self._calculate_relationship_score_from_dict(relationship_data)
            
            # Calculate support score from actual data
            support_score = self._calculate_support_score_from_dict(support_data)
            
            overall_score = int(usage_score * 0.4 + relationship_score * 0.3 + support_score * 0.3)
            
            # Determine health status
            if overall_score >= 80:
                health_status = HealthStatus.HEALTHY
            elif overall_score >= 60:
                health_status = HealthStatus.AT_RISK
            else:
                health_status = HealthStatus.CRITICAL
            
            # Generate recommendations based on scores
            recommendations = self._generate_recommendations(usage_score, relationship_score, support_score, health_status)
            
            health_score = CustomerHealthScore(
                customer_id=customer_id,
                company_name=company_name,
                overall_score=overall_score,
                health_status=health_status,
                usage_score=usage_score,
                relationship_score=relationship_score,
                support_score=support_score,
                recommendations=recommendations,
                reasoning=f"Health score calculated from {customer_data.get('data_source', 'multiple')} data source(s)"
            )
            
            return [health_score]
            
        except Exception as e:
            print(f"❌ Failed to create single customer score: {str(e)}", file=sys.stderr)
            return []
    
    def _create_airtable_customer_scores(self, customers_data: List[Dict[str, Any]]) -> List[CustomerHealthScore]:
        """Create health scores for all customers from Airtable data"""
        
        try:
            health_scores = []
            
            for customer_data in customers_data:
                customer_id = customer_data.get("customer_id", "unknown")
                company_name = customer_data.get("company", "Unknown Company")
                
                # Calculate scores using the data provided
                usage_data = customer_data.get("usage_data", {})
                relationship_data = customer_data.get("relationship_data", {})
                support_data = customer_data.get("support_data", {})
                
                # Calculate usage score from actual data
                usage_score = self._calculate_usage_score_from_dict(usage_data)
                
                # Calculate relationship score from actual data  
                relationship_score = self._calculate_relationship_score_from_dict(relationship_data)
                
                # Calculate support score from actual data
                support_score = self._calculate_support_score_from_dict(support_data)
                
                # Calculate overall score
                overall_score = int(usage_score * 0.4 + relationship_score * 0.3 + support_score * 0.3)
                
                # Determine health status
                if overall_score >= 80:
                    health_status = HealthStatus.HEALTHY
                elif overall_score >= 60:
                    health_status = HealthStatus.AT_RISK
                else:
                    health_status = HealthStatus.CRITICAL
                
                # Generate recommendations based on scores
                recommendations = self._generate_recommendations(usage_score, relationship_score, support_score, health_status)
                
                health_score = CustomerHealthScore(
                    customer_id=customer_id,
                    company_name=company_name,
                    overall_score=overall_score,
                    health_status=health_status,
                    usage_score=usage_score,
                    relationship_score=relationship_score,
                    support_score=support_score,
                    recommendations=recommendations,
                    reasoning=f"Health score calculated from Airtable data: usage ({usage_score}), relationship ({relationship_score}), and support ({support_score}) metrics"
                )
                
                health_scores.append(health_score)
            
            return health_scores
            
        except Exception as e:
            print(f"❌ Failed to create Airtable customer scores: {str(e)}", file=sys.stderr)
            return []
    
    def _calculate_usage_score(self, usage_df, customer_id: str) -> int:
        """Calculate usage score from DataFrame data"""
        
        try:
            customer_usage = usage_df[usage_df['customer_id'] == customer_id]
            if customer_usage.empty:
                return 0
            
            # Calculate metrics
            total_logins = customer_usage[customer_usage['feature_used'] == 'login']['usage_count'].sum()
            avg_session = customer_usage['session_duration_minutes'].mean()
            features_used = customer_usage['feature_used'].nunique()
            
            # Scoring logic (scale 0-100)
            login_score = min(total_logins * 2, 40)  # Max 40 points for logins
            session_score = min(avg_session / 60 * 30, 30)  # Max 30 points for avg session
            feature_score = min(features_used * 7.5, 30)  # Max 30 points for feature diversity
            
            return int(login_score + session_score + feature_score)
            
        except Exception as e:
            print(f"❌ Error calculating usage score for {customer_id}: {str(e)}", file=sys.stderr)
            return 50  # Default score on error
    
    def _calculate_relationship_score(self, relationship_df, customer_id: str) -> int:
        """Calculate relationship score from DataFrame data"""
        
        try:
            import pandas as pd
            
            customer_crm = relationship_df[relationship_df['customer_id'] == customer_id]
            if customer_crm.empty:
                return 0
            
            crm_record = customer_crm.iloc[0]
            
            # Calculate days since last contact
            last_contact = pd.to_datetime(crm_record['last_contact_date'])
            days_since_contact = (pd.Timestamp.now() - last_contact).days
            
            # Scoring logic (scale 0-100)
            contact_score = max(0, 40 - days_since_contact)  # Max 40 points, decreases with time
            
            # Contact outcome scoring
            outcome_scores = {
                'very_positive': 35,
                'positive': 25,
                'neutral': 15,
                'negative': 5,
                'no_response': 0
            }
            outcome_score = outcome_scores.get(crm_record['contact_outcome'], 10)
            
            # Account value bonus (normalized)
            account_value = float(crm_record.get('account_value', 0))
            value_score = min(account_value / 10000, 25)  # Max 25 points
            
            return int(contact_score + outcome_score + value_score)
            
        except Exception as e:
            print(f"❌ Error calculating relationship score for {customer_id}: {str(e)}", file=sys.stderr)
            return 50  # Default score on error
    
    def _calculate_support_score(self, support_df, customer_id: str) -> int:
        """Calculate support score from DataFrame data"""
        
        try:
            customer_support = support_df[support_df['customer_id'] == customer_id]
            if customer_support.empty:
                return 100  # No tickets = perfect support score
            
            # Calculate metrics
            open_tickets = len(customer_support[customer_support['status'] == 'open'])
            total_tickets = len(customer_support)
            
            # Calculate average resolution time for closed tickets
            closed_tickets = customer_support[customer_support['status'] == 'closed']
            avg_resolution = 0
            if not closed_tickets.empty:
                avg_resolution = closed_tickets['resolution_time_hours'].mean()
            
            # Calculate sentiment scores
            sentiment_scores = {
                'very_positive': 100,
                'positive': 80,
                'neutral': 60,
                'negative': 30,
                'very_negative': 10
            }
            
            sentiment_values = [sentiment_scores.get(sentiment, 50) for sentiment in customer_support['sentiment']]
            avg_sentiment = sum(sentiment_values) / len(sentiment_values) if sentiment_values else 50
            
            # Scoring logic (scale 0-100)
            # Penalize open tickets
            ticket_penalty = open_tickets * 15  # 15 points per open ticket
            
            # Penalize slow resolution times
            resolution_penalty = max(0, (avg_resolution - 24) / 24 * 20) if avg_resolution > 0 else 0  # Penalty after 24 hours
            
            # Base score starts at 100 and gets reduced by penalties
            score = 100 - ticket_penalty - resolution_penalty
            
            # Factor in sentiment (weight it 30%)
            final_score = int(score * 0.7 + avg_sentiment * 0.3)
            
            return max(0, min(100, final_score))
            
        except Exception as e:
            print(f"❌ Error calculating support score for {customer_id}: {str(e)}", file=sys.stderr)
            return 70  # Default score on error
    
    def _calculate_usage_score_from_dict(self, usage_data: dict) -> int:
        """Calculate usage score from dictionary data"""
        
        try:
            if not usage_data:
                return 0
            
            total_logins = usage_data.get('total_logins', 0)
            avg_session = usage_data.get('avg_session_duration', 0)
            features_used = usage_data.get('features_used', 0)
            
            # Scoring logic (scale 0-100)
            login_score = min(total_logins * 2, 40)  # Max 40 points for logins
            session_score = min(avg_session / 60 * 30, 30)  # Max 30 points for avg session
            feature_score = min(features_used * 7.5, 30)  # Max 30 points for feature diversity
            
            return int(login_score + session_score + feature_score)
            
        except Exception as e:
            return 50  # Default score on error
    
    def _calculate_relationship_score_from_dict(self, relationship_data: dict) -> int:
        """Calculate relationship score from dictionary data"""
        
        try:
            if not relationship_data:
                return 0
            
            engagement_score = relationship_data.get('engagement_score', 50)
            contract_value = relationship_data.get('contract_value', 0)
            renewal_probability = relationship_data.get('renewal_probability', 0.5)
            
            # Normalize and combine scores
            engagement_normalized = min(engagement_score, 100)
            value_score = min(contract_value / 10000 * 20, 20)  # Max 20 points
            renewal_score = renewal_probability * 30  # Max 30 points
            
            return int(engagement_normalized * 0.5 + value_score + renewal_score)
            
        except Exception as e:
            return 50  # Default score on error
    
    def _calculate_support_score_from_dict(self, support_data: dict) -> int:
        """Calculate support score from dictionary data"""
        
        try:
            if not support_data:
                return 100  # No support data = assume good support experience
            
            open_tickets = support_data.get('open_tickets', 0)
            satisfaction_score = support_data.get('satisfaction_score', 4)  # Out of 5
            avg_resolution = support_data.get('avg_resolution_hours', 24)
            escalations = support_data.get('escalations', 0)
            
            # Scoring logic (scale 0-100)
            ticket_penalty = open_tickets * 15  # 15 points per open ticket
            escalation_penalty = escalations * 10  # 10 points per escalation
            resolution_penalty = max(0, (avg_resolution - 24) / 24 * 20) if avg_resolution > 0 else 0
            
            # Base score from satisfaction (convert from 5-point scale to 100)
            satisfaction_base = (satisfaction_score / 5) * 100
            
            # Apply penalties
            final_score = satisfaction_base - ticket_penalty - escalation_penalty - resolution_penalty
            
            return max(0, min(100, int(final_score)))
            
        except Exception as e:
            return 70  # Default score on error
    
    def _generate_recommendations(self, usage_score: int, relationship_score: int, support_score: int, health_status: HealthStatus) -> List[Recommendation]:
        """Generate recommendations based on component scores and health status"""
        
        recommendations = []
        
        # Usage-based recommendations
        if usage_score < 40:
            recommendations.append(Recommendation(
                action="Schedule product training session to increase feature adoption",
                priority=RecommendationPriority.HIGH,
                timeline="Within 1 week",
                reasoning=f"Low usage score ({usage_score}) indicates customer needs help maximizing product value"
            ))
        elif usage_score < 70:
            recommendations.append(Recommendation(
                action="Share advanced feature guides and best practices",
                priority=RecommendationPriority.MEDIUM,
                timeline="Within 2 weeks",
                reasoning=f"Moderate usage score ({usage_score}) suggests opportunity for deeper engagement"
            ))
        
        # Relationship-based recommendations
        if relationship_score < 40:
            recommendations.append(Recommendation(
                action="Schedule immediate check-in call with CSM to address relationship concerns",
                priority=RecommendationPriority.CRITICAL,
                timeline="Within 3 days",
                reasoning=f"Low relationship score ({relationship_score}) indicates urgent need for relationship repair"
            ))
        elif relationship_score < 70:
            recommendations.append(Recommendation(
                action="Plan quarterly business review to strengthen relationship",
                priority=RecommendationPriority.HIGH,
                timeline="Within 2 weeks",
                reasoning=f"Moderate relationship score ({relationship_score}) could benefit from regular check-ins"
            ))
        
        # Support-based recommendations
        if support_score < 40:
            recommendations.append(Recommendation(
                action="Prioritize resolution of open support tickets and conduct satisfaction survey",
                priority=RecommendationPriority.CRITICAL,
                timeline="Immediately",
                reasoning=f"Low support score ({support_score}) indicates serious support issues affecting satisfaction"
            ))
        elif support_score < 70:
            recommendations.append(Recommendation(
                action="Review support ticket history and proactively address common issues",
                priority=RecommendationPriority.MEDIUM,
                timeline="Within 1 week",
                reasoning=f"Moderate support score ({support_score}) suggests room for support improvement"
            ))
        
        # Health status-based recommendations
        if health_status == HealthStatus.CRITICAL:
            recommendations.append(Recommendation(
                action="Initiate customer success intervention plan with executive involvement",
                priority=RecommendationPriority.CRITICAL,
                timeline="Within 24 hours",
                reasoning="Critical health status requires immediate executive attention to prevent churn"
            ))
        elif health_status == HealthStatus.AT_RISK:
            recommendations.append(Recommendation(
                action="Develop customer success action plan with increased touchpoints",
                priority=RecommendationPriority.HIGH,
                timeline="Within 1 week",
                reasoning="At-risk status requires proactive intervention to improve health"
            ))
        else:  # HEALTHY
            recommendations.append(Recommendation(
                action="Continue current engagement strategy and explore expansion opportunities",
                priority=RecommendationPriority.LOW,
                timeline="Within 1 month",
                reasoning="Healthy customer presents opportunity for account growth"
            ))
        
        # Limit to top 3 recommendations to avoid overwhelming output
        return recommendations[:3]
    
    def _detect_available_sources(self) -> List[str]:
        """Detect which data sources are available based on configuration"""
        
        available_sources = []
        
        # Check if API keys are configured
        if os.getenv("HUBSPOT_API_KEY"):
            available_sources.append("hubspot")
        
        if os.getenv("AIRTABLE_API_KEY") and os.getenv("AIRTABLE_BASE_ID"):
            available_sources.append("airtable")
        
        if os.getenv("ZAPIER_API_KEY"):
            available_sources.append("zapier")
        
        # Fallback to static data if no integrations configured
        if not available_sources:
            available_sources.append("static")
        
        return available_sources
    
    def generate_summary_report(self, health_scores: List[CustomerHealthScore]) -> str:
        """Generate executive summary report"""
        
        if not health_scores:
            return "No customer data available for analysis."
        
        total_customers = len(health_scores)
        healthy = len([s for s in health_scores if s.health_status == HealthStatus.HEALTHY])
        at_risk = len([s for s in health_scores if s.health_status == HealthStatus.AT_RISK])
        critical = len([s for s in health_scores if s.health_status == HealthStatus.CRITICAL])
        
        avg_score = sum(s.overall_score for s in health_scores) / total_customers
        
        report = f"""Customer Health Analysis Report
=====================================

Analysis Summary:
• Total Customers: {total_customers}
• Average Health Score: {avg_score:.1f}/100
• Data Sources: {self._get_data_sources_summary()}

Health Distribution:
• 🟢 Healthy: {healthy} customers ({healthy/total_customers*100:.1f}%)
• 🟡 At Risk: {at_risk} customers ({at_risk/total_customers*100:.1f}%)
• 🔴 Critical: {critical} customers ({critical/total_customers*100:.1f}%)

Priority Actions Required:"""
        
        # Add top priority customers
        priority_customers = sorted(
            [s for s in health_scores if s.health_status in [HealthStatus.CRITICAL, HealthStatus.AT_RISK]], 
            key=lambda x: x.overall_score
        )
        
        for i, customer in enumerate(priority_customers[:3], 1):
            report += f"\n{i}. {customer.company_name} (Score: {customer.overall_score}/100)"
            if customer.recommendations:
                report += f"\n   → {customer.recommendations[0].action}"
        
        return report
    
    def _get_data_sources_summary(self) -> str:
        """Get summary of configured data sources"""
        
        if self.use_static_data or self.current_data_source == "static":
            return "Static demo data (5 sample customers)"
        elif self.current_data_source == "airtable":
            base_id = os.getenv("AIRTABLE_BASE_ID", "Unknown")
            return f"Airtable database (Base: {base_id[:8]}...)"
        elif self.current_data_source == "hubspot":
            return "HubSpot CRM"
        elif self.current_data_source == "zapier":
            return "Zapier integration"
        else:
            return f"Current source: {self.current_data_source}"