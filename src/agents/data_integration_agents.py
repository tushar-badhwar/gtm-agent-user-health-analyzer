"""
CrewAI agents for dynamic data integration with HubSpot, Zapier, and Airtable
"""

from crewai import Agent, Task, Crew
from crewai.tools import BaseTool
from typing import Dict, List, Any
import os
import sys
import requests

# Import API clients
try:
    from hubspot import HubSpot
except ImportError:
    HubSpot = None

try:
    from pyairtable import Api as AirtableApi
except ImportError:
    AirtableApi = None

# Custom tools for each integration
class HubSpotTool(BaseTool):
    name: str = "hubspot_data_collector"
    description: str = "Collect customer data from HubSpot CRM"
    
    def _run(self, customer_email: str) -> Dict[str, Any]:
        """Collect customer data from HubSpot"""
        api_key = os.getenv("HUBSPOT_API_KEY")
        if not api_key or not HubSpot:
            return {"error": "HubSpot API key not configured or library not installed"}
        
        try:
            client = HubSpot(access_token=api_key)
            
            # Search for contact by email
            search_request = {
                "filterGroups": [
                    {
                        "filters": [
                            {
                                "propertyName": "email",
                                "operator": "EQ",
                                "value": customer_email
                            }
                        ]
                    }
                ],
                "properties": [
                    "email", "firstname", "lastname", "company", "phone",
                    "lifecyclestage", "hs_lead_status", "createdate", "lastmodifieddate",
                    "num_associated_deals", "total_revenue", "hs_analytics_num_page_views"
                ]
            }
            
            contacts_response = client.crm.contacts.search_api.do_search(search_request=search_request)
            
            if not contacts_response.results:
                return {"error": f"No contact found for email: {customer_email}"}
            
            contact = contacts_response.results[0]
            
            # Get associated company data
            company_data = {}
            if contact.properties.get("company"):
                try:
                    company_response = client.crm.companies.basic_api.get_by_id(
                        company_id=contact.properties["company"],
                        properties=["name", "industry", "numberofemployees", "annualrevenue", "createdate"]
                    )
                    company_data = company_response.properties
                except:
                    pass
            
            # Get recent activity/engagement data
            try:
                # Get recent emails
                email_response = client.crm.objects.emails.search_api.do_search(
                    search_request={
                        "filterGroups": [
                            {
                                "filters": [
                                    {
                                        "propertyName": "hs_email_to_email",
                                        "operator": "EQ", 
                                        "value": customer_email
                                    }
                                ]
                            }
                        ],
                        "sorts": [{"propertyName": "hs_createdate", "direction": "DESCENDING"}],
                        "limit": 10
                    }
                )
                recent_emails = len(email_response.results) if email_response.results else 0
            except:
                recent_emails = 0
            
            return {
                "source": "hubspot",
                "customer_data": {
                    "contact_id": contact.id,
                    "email": contact.properties.get("email"),
                    "name": f"{contact.properties.get('firstname', '')} {contact.properties.get('lastname', '')}".strip(),
                    "company": contact.properties.get("company"),
                    "phone": contact.properties.get("phone"),
                    "lifecycle_stage": contact.properties.get("lifecyclestage"),
                    "lead_status": contact.properties.get("hs_lead_status"),
                    "created_date": contact.properties.get("createdate"),
                    "last_modified": contact.properties.get("lastmodifieddate"),
                    "total_revenue": contact.properties.get("total_revenue"),
                    "page_views": contact.properties.get("hs_analytics_num_page_views"),
                    "recent_emails": recent_emails,
                    "company_data": company_data
                }
            }
            
        except Exception as e:
            return {"error": f"HubSpot API error: {str(e)}"}

class AirtableTool(BaseTool):
    name: str = "airtable_data_collector"
    description: str = "Collect customer data from Airtable"
    
    def _discover_best_table(self, base, customer_email):
        """Discover the best table for customer data using Airtable API"""
        try:
            # Get base schema using the API
            print("🔍 Discovering available tables in base...", file=sys.stderr)
            
            # Use the API to get base metadata (this requires proper permissions)
            try:
                # Try to get base schema if available
                # Note: This may require enterprise features, so we'll fall back to probing
                base_info = base.get()
                if hasattr(base_info, 'tables'):
                    table_names = [table.name for table in base_info.tables]
                    print(f"✅ Found tables via API: {table_names}", file=sys.stderr)
                else:
                    table_names = []
            except:
                table_names = []
            
            # If API metadata isn't available, use intelligent probing
            if not table_names:
                print("🔍 API metadata not available, using intelligent table discovery...", file=sys.stderr)
                table_names = self._probe_for_tables(base)
            
            # Evaluate each table for customer data quality
            best_table = None
            best_table_name = None
            best_field_mapping = {}
            best_score = 0
            
            for table_name in table_names:
                try:
                    print(f"📊 Analyzing table: '{table_name}'", file=sys.stderr)
                    table = base.table(table_name)
                    
                    # Get sample records to analyze
                    sample_records = table.all(max_records=10)  # More samples for better analysis
                    if not sample_records:
                        print(f"   ⚠️ Table '{table_name}' is empty", file=sys.stderr)
                        continue
                    
                    # Analyze this table's suitability for customer data
                    field_mapping = self._discover_schema(table, sample_records)
                    score = self._score_table_for_customers(field_mapping, sample_records, customer_email)
                    
                    print(f"   📈 Table '{table_name}' score: {score}/100", file=sys.stderr)
                    
                    if score > best_score:
                        best_score = score
                        best_table = table
                        best_table_name = table_name
                        best_field_mapping = field_mapping
                        
                except Exception as e:
                    print(f"   ❌ Could not analyze table '{table_name}': {str(e)[:50]}...", file=sys.stderr)
                    continue
            
            if best_table:
                print(f"✅ Selected best table: '{best_table_name}' (score: {best_score}/100)", file=sys.stderr)
                return best_table, best_table_name, best_field_mapping
            else:
                return None, None, {}
                
        except Exception as e:
            print(f"❌ Table discovery failed: {str(e)}", file=sys.stderr)
            return None, None, {}
    
    def _probe_for_tables(self, base):
        """Intelligent probing to discover table names"""
        # Common patterns for customer tables
        potential_names = [
            # Direct customer tables
            "Customers", "Customer", "Clients", "Client", "Contacts", "Contact",
            # Generic tables (often default names)
            "Table 1", "Table1", "Main Table", "Main", "Sheet1", "Sheet 1",
            # Business-specific
            "Accounts", "Account", "Users", "User", "Members", "Member",
            "Leads", "Lead", "Prospects", "Prospect", "People", "Person",
            # Alternative formats
            "Customer Data", "Client Data", "Contact List", "Customer List",
            "CRM", "Database", "Records", "Entries"
        ]
        
        discovered_tables = []
        
        for table_name in potential_names:
            try:
                table = base.table(table_name)
                # Test if we can actually access this table
                _ = table.all(max_records=1)  # Test access
                discovered_tables.append(table_name)
                print(f"   ✅ Found table: '{table_name}'", file=sys.stderr)
            except:
                continue
        
        if not discovered_tables:
            # Last resort: try some common variations
            print("🔍 Trying systematic table name variations...", file=sys.stderr)
            for i in range(1, 6):  # Try Table 1-5
                try:
                    table_name = f"Table {i}"
                    table = base.table(table_name)
                    _ = table.all(max_records=1)  # Test access
                    discovered_tables.append(table_name)
                    print(f"   ✅ Found table: '{table_name}'", file=sys.stderr)
                except:
                    continue
        
        print(f"🏁 Discovered {len(discovered_tables)} accessible tables", file=sys.stderr)
        return discovered_tables
    
    def _score_table_for_customers(self, field_mapping, sample_records, target_email):
        """Score a table's suitability for customer data (0-100)"""
        score = 0
        
        # Essential fields scoring
        if field_mapping.get("email"):
            score += 40  # Email field is critical
        if field_mapping.get("name"):
            score += 20  # Name field is important
        if field_mapping.get("customer_id"):
            score += 15  # ID field is valuable
            
        # Nice-to-have fields
        if field_mapping.get("company"):
            score += 5
        if field_mapping.get("phone"):
            score += 5
        if field_mapping.get("account_value"):
            score += 10
        if field_mapping.get("customer_type"):
            score += 5
        
        # Check if target customer might be in this table
        if target_email and sample_records:
            for record in sample_records:
                fields = record.get("fields", {})
                for field_value in fields.values():
                    if isinstance(field_value, str) and target_email.lower() in field_value.lower():
                        score += 20  # Bonus for containing our target customer
                        break
        
        # Penalty for tables with too few records (might be config tables)
        if len(sample_records) < 3:
            score -= 10
            
        return max(0, min(100, score))  # Clamp between 0-100

    def _discover_schema(self, _, sample_records):
        """Discover and map table schema to our expected fields"""
        if not sample_records:
            return {}
            
        # Analyze all field names across sample records
        all_fields = set()
        for record in sample_records:
            all_fields.update(record.get("fields", {}).keys())
        
        # Smart field mapping based on common patterns
        field_mapping = {
            "email": self._find_field_by_patterns(all_fields, [
                "email_address", "email", "e-mail", "e_mail", "contact_email",
                "customer_email", "user_email", "primary_email"
            ]),
            "name": self._find_field_by_patterns(all_fields, [
                "name", "full_name", "customer_name", "client_name", "contact_name",
                "first_name", "last_name", "display_name", "person_name"
            ]),
            "company": self._find_field_by_patterns(all_fields, [
                "company", "company_name", "organization", "org", "business",
                "account", "account_name", "client", "customer_company"
            ]),
            "account_value": self._find_field_by_patterns(all_fields, [
                "account_value", "value", "revenue", "contract_value", "deal_value",
                "ticket_size", "ticket size", "annual_revenue", "mrr", "arr", "amount", "price",
                "deal_amount", "contract_amount", "purchase_amount", "order_value"
            ]),
            "customer_id": self._find_field_by_patterns(all_fields, [
                "id", "customer_id", "client_id", "account_id", "user_id",
                "contact_id", "record_id", "reference"
            ]),
            "phone": self._find_field_by_patterns(all_fields, [
                "phone", "phone_number", "telephone", "mobile", "cell",
                "contact_phone", "primary_phone"
            ]),
            "created_date": self._find_field_by_patterns(all_fields, [
                "created", "created_date", "date_created", "signup_date",
                "registration_date", "start_date", "onboarding_date"
            ]),
            "last_contact": self._find_field_by_patterns(all_fields, [
                "last_contact", "last_contact_date", "last_interaction",
                "last_touch", "last_activity", "recent_contact"
            ]),
            "engagement_score": self._find_field_by_patterns(all_fields, [
                "engagement", "engagement_score", "customer_engagement",
                "engagement_rating", "activity_score", "involvement_score"
            ]),
            "customer_type": self._find_field_by_patterns(all_fields, [
                "type", "customer_type", "client_type", "tier", "segment",
                "category", "classification", "status"
            ]),
            "sentiment": self._find_field_by_patterns(all_fields, [
                "sentiment", "email_sentiment", "mood", "satisfaction",
                "feedback", "rating", "score"
            ]),
            "last_purchase": self._find_field_by_patterns(all_fields, [
                "last_purchase", "last_order", "recent_purchase", "latest_order",
                "last_transaction", "purchase_date"
            ])
        }
        
        print(f"🔍 Discovered field mapping:", file=sys.stderr)
        for key, value in field_mapping.items():
            if value:
                print(f"  • {key} → {value}", file=sys.stderr)
        
        return field_mapping
    
    def _find_field_by_patterns(self, field_names, patterns):
        """Find the best matching field name based on patterns"""
        field_names_lower = [f.lower() for f in field_names]
        
        # Exact match first
        for pattern in patterns:
            if pattern.lower() in field_names_lower:
                idx = field_names_lower.index(pattern.lower())
                return list(field_names)[idx]
        
        # Partial match
        for pattern in patterns:
            for field_name in field_names:
                if pattern.lower() in field_name.lower():
                    return field_name
        
        return None
    
    def _extract_field_value(self, fields, field_mapping, key):
        """Extract field value using the discovered mapping"""
        mapped_field = field_mapping.get(key)
        if not mapped_field:
            return None
            
        value = fields.get(mapped_field)
        
        # Handle computed fields (objects with 'value' property)
        if isinstance(value, dict) and 'value' in value:
            return value['value']
        
        return value

    def _run(self, customer_email: str) -> Dict[str, Any]:
        """Collect customer data from Airtable using Personal Access Token (PAT)"""
        # Get Airtable Personal Access Token (PAT) and Base ID
        api_token = os.getenv("AIRTABLE_API_KEY")  # This should be a PAT token
        base_id = os.getenv("AIRTABLE_BASE_ID")
        
        if not api_token or not base_id:
            return {"error": "Airtable Personal Access Token (PAT) or Base ID not configured. Check AIRTABLE_API_KEY and AIRTABLE_BASE_ID in .env file"}
            
        if not AirtableApi:
            return {"error": "pyairtable library not installed. Run: pip install pyairtable"}
        
        if not api_token.startswith("pat"):
            return {"error": "AIRTABLE_API_KEY should be a Personal Access Token starting with 'pat'. Old API keys are deprecated."}
        
        try:
            # Initialize Airtable API with Personal Access Token
            api = AirtableApi(api_token)
            base = api.base(base_id)
            
            print(f"🔍 Searching Airtable for customer: {customer_email}", file=sys.stderr)
            
            # Discover available tables using Airtable API
            customers_table, table_name_used, field_mapping = self._discover_best_table(base, customer_email)
            
            if not customers_table:
                return {"error": "Could not find any accessible table with customer data. Please check your Airtable permissions and base structure."}
            
            # Search for customer using discovered email field
            records = []
            email_field = field_mapping.get("email")
            
            if email_field:
                try:
                    formula = f"LOWER({{{email_field}}}) = LOWER('{customer_email}')"
                    records = customers_table.all(formula=formula)
                    if records:
                        print(f"✅ Found customer using discovered email field: {email_field}", file=sys.stderr)
                except Exception as e:
                    print(f"⚠️ Search by email failed: {str(e)}", file=sys.stderr)
            
            # Fallback: search by customer ID field if email search failed
            if not records:
                customer_id_field = field_mapping.get("customer_id")
                if customer_id_field:
                    try:
                        formula = f"{{{customer_id_field}}} = '{customer_email}'"
                        records = customers_table.all(formula=formula)
                        if records:
                            print(f"✅ Found customer using customer ID field: {customer_id_field}", file=sys.stderr)
                    except:
                        pass
            
            # Last resort: search through all records for the email in any text field
            if not records:
                print(f"🔍 Performing broad search across all fields...", file=sys.stderr)
                all_records = customers_table.all(max_records=100)  # Limit to avoid timeout
                for record in all_records:
                    fields = record.get("fields", {})
                    for field_value in fields.values():
                        if isinstance(field_value, str) and customer_email.lower() in field_value.lower():
                            records = [record]
                            print(f"✅ Found customer through broad search", file=sys.stderr)
                            break
                    if records:
                        break
                
            if not records:
                return {"error": f"No customer found for: {customer_email} in table '{table_name_used}'"}
            
            customer = records[0]
            fields = customer["fields"]
            
            # Extract data using discovered field mapping
            customer_name = (
                self._extract_field_value(fields, field_mapping, "name") or
                self._extract_field_value(fields, field_mapping, "company") or
                "Unknown Customer"
            )
            
            customer_email_found = (
                self._extract_field_value(fields, field_mapping, "email") or
                customer_email
            )
            
            account_value = self._extract_field_value(fields, field_mapping, "account_value") or 0
            
            print(f"✅ Found customer: {customer_name}", file=sys.stderr)
            
            # Get usage data if available
            usage_data = {}
            try:
                usage_table = base.table("Usage")
                usage_formula = f"LOWER({{Customer Email}}) = LOWER('{customer_email}')"
                usage_records = usage_table.all(formula=usage_formula)
                
                if usage_records:
                    print(f"📊 Found {len(usage_records)} usage records", file=sys.stderr)
                    # Aggregate usage data
                    total_logins = 0
                    total_session_time = 0
                    features_used = set()
                    
                    for record in usage_records:
                        fields_usage = record["fields"]
                        feature = fields_usage.get("Feature Used", "")
                        if feature == "login":
                            total_logins += fields_usage.get("Usage Count", 0)
                        
                        total_session_time += fields_usage.get("Session Duration", 0)
                        if feature:
                            features_used.add(feature)
                    
                    avg_session = total_session_time / len(usage_records) if usage_records else 0
                    usage_data = {
                        "total_logins": total_logins,
                        "avg_session_duration": avg_session,
                        "features_used": len(features_used),
                        "trend": "stable"  # Could be calculated from dates
                    }
            except Exception as e:
                print(f"⚠️ Could not fetch usage data: {str(e)}", file=sys.stderr)
            
            # Get support data if available  
            support_data = {}
            try:
                support_table = base.table("Support")
                support_formula = f"LOWER({{Customer Email}}) = LOWER('{customer_email}')"
                support_records = support_table.all(formula=support_formula)
                
                if support_records:
                    print(f"🎧 Found {len(support_records)} support records", file=sys.stderr)
                    open_tickets = sum(1 for r in support_records if r["fields"].get("Status") == "open")
                    resolution_times = [r["fields"].get("Resolution Time Hours", 0) for r in support_records if r["fields"].get("Status") == "closed"]
                    avg_resolution = sum(resolution_times) / len(resolution_times) if resolution_times else 0
                    escalations = sum(1 for r in support_records if r["fields"].get("Priority") in ["high", "critical"])
                    
                    support_data = {
                        "open_tickets": open_tickets,
                        "avg_resolution_hours": avg_resolution,
                        "satisfaction_score": 4,  # Could be from support rating field
                        "escalations": escalations
                    }
            except Exception as e:
                print(f"⚠️ Could not fetch support data: {str(e)}", file=sys.stderr)
            
            # Extract values using dynamic field mapping
            engagement_score_raw = self._extract_field_value(fields, field_mapping, "engagement_score")
            engagement_score_value = float(engagement_score_raw) if engagement_score_raw else 75
                
            # Map sentiment to contact outcome
            sentiment_raw = self._extract_field_value(fields, field_mapping, "sentiment")
            sentiment_value = str(sentiment_raw).lower() if sentiment_raw else "neutral"
            contact_outcome = "positive" if "positive" in sentiment_value else "negative" if "negative" in sentiment_value else "neutral"
            
            # Get other relationship data
            last_contact_date = self._extract_field_value(fields, field_mapping, "last_contact") or self._extract_field_value(fields, field_mapping, "last_purchase") or ""
            customer_type = self._extract_field_value(fields, field_mapping, "customer_type") or "Regular"
            phone = self._extract_field_value(fields, field_mapping, "phone") or ""
            created_date = self._extract_field_value(fields, field_mapping, "created_date") or ""
            
            # Calculate days since last purchase if we have a last purchase date
            days_since_last_purchase = 0
            if last_contact_date and isinstance(last_contact_date, str):
                try:
                    from datetime import datetime
                    if last_contact_date:
                        # Try to parse various date formats
                        for date_format in ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"]:
                            try:
                                last_date = datetime.strptime(last_contact_date.split("T")[0], date_format)
                                days_since_last_purchase = (datetime.now() - last_date).days
                                break
                            except:
                                continue
                except:
                    days_since_last_purchase = 0
            
            customer_info = {
                "name": customer_name,
                "email": customer_email_found,
                "company": self._extract_field_value(fields, field_mapping, "company") or customer_name,
                "account_value": float(account_value) if account_value else 0
            }
            
            return {
                **customer_info,
                "usage_data": usage_data,
                "relationship_data": {
                    "last_contact_date": last_contact_date,
                    "engagement_score": engagement_score_value,
                    "emails_responded": 3,   # Could be calculated from your data
                    "meetings_attended": 1,  # Could be tracked in Airtable
                    "contract_value": float(account_value) if account_value else 0,
                    "renewal_probability": 0.8 if customer_type == "VIP" else 0.6,
                    "contact_outcome": contact_outcome,
                    "csm_name": "Auto-assigned",  # Could be added to your Airtable
                    "contract_end_date": "",  # Could be calculated from last purchase + period
                    "days_since_last_purchase": days_since_last_purchase,
                    "customer_type": customer_type,
                    "phone": phone,
                    "created_date": created_date
                },
                "support_data": support_data,
                "data_source": "airtable",
                "schema_info": {
                    "table_name": table_name_used,
                    "discovered_fields": list(field_mapping.keys()),
                    "mapped_fields": {k: v for k, v in field_mapping.items() if v}
                }
            }
            
        except Exception as e:
            return {"error": f"Airtable API error: {str(e)}"}

class ZapierTool(BaseTool):
    name: str = "zapier_data_collector"
    description: str = "Collect customer data via Zapier webhooks/API"
    
    def _run(self, customer_email: str) -> Dict[str, Any]:
        """Collect customer data through Zapier integration"""
        api_key = os.getenv("ZAPIER_API_KEY")
        
        if not api_key:
            return {"error": "Zapier API key not configured"}
        
        try:
            # This would typically involve:
            # 1. Triggering a Zapier webhook with customer email
            # 2. Zapier collects data from connected apps
            # 3. Returns aggregated data
            
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            }
            
            # Example webhook URL - replace with your actual Zapier webhook
            webhook_url = "https://hooks.zapier.com/hooks/catch/your-webhook-id/"
            
            payload = {
                "customer_email": customer_email,
                "action": "get_customer_data"
            }
            
            response = requests.post(webhook_url, json=payload, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                return {
                    "source": "zapier",
                    "customer_data": data
                }
            else:
                return {"error": f"Zapier webhook error: {response.status_code}"}
                
        except Exception as e:
            return {"error": f"Zapier integration error: {str(e)}"}

# CrewAI Agents
def create_data_integration_agents():
    """Create specialized agents for data integration"""
    
    hubspot_agent = Agent(
        role="HubSpot Integration Specialist",
        goal="Collect comprehensive customer data from HubSpot CRM",
        backstory="Expert at navigating HubSpot's API, understanding contact lifecycles, and extracting meaningful customer engagement data.",
        tools=[HubSpotTool()],
        verbose=True,
        allow_delegation=False
    )
    
    airtable_agent = Agent(
        role="Airtable Data Analyst",
        goal="Extract and analyze customer data from Airtable databases",
        backstory="Specialist in Airtable schema analysis and data extraction, skilled at working with custom field configurations.",
        tools=[AirtableTool()],
        verbose=True,
        allow_delegation=False
    )
    
    zapier_agent = Agent(
        role="Zapier Integration Coordinator",
        goal="Coordinate data collection across multiple platforms via Zapier",
        backstory="Expert at orchestrating complex data flows through Zapier integrations and webhook management.",
        tools=[ZapierTool()],
        verbose=True,
        allow_delegation=False
    )
    
    data_synthesis_agent = Agent(
        role="Data Synthesis Specialist",
        goal="Combine and standardize customer data from multiple sources",
        backstory="Expert at data normalization, conflict resolution, and creating unified customer profiles from disparate sources.",
        verbose=True,
        allow_delegation=False
    )
    
    return {
        "hubspot": hubspot_agent,
        "airtable": airtable_agent, 
        "zapier": zapier_agent,
        "synthesis": data_synthesis_agent
    }

def create_dynamic_data_collection_crew(customer_email: str, data_sources: List[str]):
    """Create a crew for dynamic data collection based on available sources"""
    
    agents = create_data_integration_agents()
    tasks = []
    
    # Create collection tasks for each available data source
    for source in data_sources:
        if source in agents:
            task = Task(
                description=f"""
                Collect customer data for {customer_email} from {source.title()}:
                
                1. Connect to {source.title()} API with proper authentication
                2. Search for customer using email: {customer_email}
                3. Extract all relevant customer information including:
                   - Contact details and company information
                   - Engagement history and activity data
                   - Account value and contract information
                   - Recent interactions and communications
                4. Handle any API errors or rate limits gracefully
                5. Return structured data with source attribution
                
                Focus on data quality and completeness.
                """,
                agent=agents[source],
                expected_output=f"Structured customer data from {source.title()} with metadata"
            )
            tasks.append(task)
    
    # Add synthesis task to combine all data
    if len(tasks) > 1:
        synthesis_task = Task(
            description=f"""
            Synthesize customer data for {customer_email} from multiple sources:
            
            1. Combine data from all successful source collections
            2. Resolve any conflicts between data sources (use most recent/reliable)
            3. Fill gaps where possible (e.g., missing contact info from one source)
            4. Create a unified customer profile with confidence scores
            5. Flag any data quality issues or inconsistencies
            6. Map all fields to our standard customer health schema
            
            Prioritize data quality and provide source attribution for each field.
            """,
            agent=agents["synthesis"],
            expected_output="Unified customer profile with source attribution and quality metrics"
        )
        tasks.append(synthesis_task)
    
    return Crew(
        agents=list(agents.values()),
        tasks=tasks,
        verbose=False,
        process="sequential"
    )