"""
Enhanced Airtable Discovery Tool
Automatically discovers all accessible bases and their schemas
"""

import os
import sys
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import json

try:
    from pyairtable import Api as AirtableApi
    from pyairtable.api.types import BaseSchema, TableSchema
except ImportError:
    AirtableApi = None
    BaseSchema = None
    TableSchema = None


@dataclass
class FieldInfo:
    """Information about a single field in a table"""
    name: str
    field_type: str
    description: Optional[str] = None
    options: Optional[Dict[str, Any]] = None
    sample_values: List[Any] = None


@dataclass
class TableInfo:
    """Information about a single table in a base"""
    name: str
    id: str
    description: Optional[str] = None
    fields: List[FieldInfo] = None
    record_count: int = 0
    primary_field: Optional[str] = None


@dataclass
class BaseInfo:
    """Information about a single Airtable base"""
    name: str
    id: str
    permission_level: str
    tables: List[TableInfo] = None


class AirtableDiscoveryTool:
    """Enhanced tool for discovering Airtable bases and schemas"""
    
    def __init__(self, api_token: Optional[str] = None):
        """Initialize with Airtable Personal Access Token"""
        self.api_token = api_token or os.getenv("AIRTABLE_API_KEY")
        
        if not self.api_token:
            raise ValueError("Airtable API token required. Set AIRTABLE_API_KEY environment variable.")
        
        # Import pyairtable
        try:
            if AirtableApi is None:
                from pyairtable import Api as DynamicAirtableApi
                self.api = DynamicAirtableApi(self.api_token)
            else:
                self.api = AirtableApi(self.api_token)
        except ImportError:
            raise ImportError("pyairtable library required. Run: pip install pyairtable")
        
        if not self.api_token.startswith("pat"):
            print("⚠️ Warning: Consider using Personal Access Token (PAT) starting with 'pat' for better security", file=sys.stderr)
    
    def discover_all_bases(self) -> List[BaseInfo]:
        """
        Discover all accessible Airtable bases
        Note: This requires appropriate permissions on the PAT token
        """
        try:
            print("🔍 Discovering all accessible Airtable bases...", file=sys.stderr)
            
            # Get all bases accessible to this token
            # Note: This requires the token to have base schema read permissions
            bases_response = self.api.bases()
            
            discovered_bases = []
            
            for base_data in bases_response:
                # Handle different response formats
                if hasattr(base_data, 'name'):
                    # Base object with attributes
                    base_info = BaseInfo(
                        name=getattr(base_data, 'name', 'Unknown Base'),
                        id=getattr(base_data, 'id', ''),
                        permission_level=getattr(base_data, 'permission_level', 'unknown')
                    )
                else:
                    # Dictionary format
                    base_info = BaseInfo(
                        name=base_data.get("name", "Unknown Base"),
                        id=base_data.get("id", ""),
                        permission_level=base_data.get("permissionLevel", "unknown")
                    )
                discovered_bases.append(base_info)
                print(f"  ✅ Found base: {base_info.name} ({base_info.id})", file=sys.stderr)
            
            print(f"🏁 Discovered {len(discovered_bases)} accessible bases", file=sys.stderr)
            return discovered_bases
            
        except Exception as e:
            print(f"❌ Error discovering bases: {str(e)}", file=sys.stderr)
            print("💡 Tip: Ensure your PAT token has 'schema.bases:read' scope", file=sys.stderr)
            return []
    
    def discover_base_schema(self, base_id: str) -> Optional[BaseInfo]:
        """
        Discover complete schema for a specific base including all tables and fields
        """
        try:
            print(f"🔍 Discovering schema for base: {base_id}", file=sys.stderr)
            
            base = self.api.base(base_id)
            
            # Try to get base schema using metadata API
            try:
                base_schema = base.schema()
                return self._parse_base_schema(base_schema, base_id)
            except Exception as schema_error:
                print(f"⚠️ Schema API failed: {str(schema_error)}", file=sys.stderr)
                print("🔄 Falling back to manual discovery...", file=sys.stderr)
                return self._discover_base_manually(base, base_id)
                
        except Exception as e:
            print(f"❌ Error discovering base schema: {str(e)}", file=sys.stderr)
            return None
    
    def _parse_base_schema(self, schema: BaseSchema, base_id: str) -> BaseInfo:
        """Parse official base schema from Airtable API"""
        print("✅ Using official schema API", file=sys.stderr)
        
        base_info = BaseInfo(
            name=getattr(schema, 'name', 'Unknown Base'),
            id=base_id,
            permission_level="read",
            tables=[]
        )
        
        for table_schema in schema.tables:
            table_info = TableInfo(
                name=table_schema.name,
                id=table_schema.id,
                description=getattr(table_schema, 'description', None),
                fields=[],
                primary_field=None
            )
            
            # Parse fields
            for field in table_schema.fields:
                field_info = FieldInfo(
                    name=field.name,
                    field_type=field.type,
                    description=getattr(field, 'description', None),
                    options=getattr(field, 'options', None)
                )
                
                table_info.fields.append(field_info)
                
                # Identify primary field
                if getattr(field, 'primary', False):
                    table_info.primary_field = field.name
            
            # Get record count (requires additional API call)
            try:
                records = self.api.base(base_id).table(table_schema.name).all(max_records=1)
                # Note: This doesn't give actual count, just verifies table is accessible
                table_info.record_count = -1  # Unknown count
            except:
                table_info.record_count = 0
            
            base_info.tables.append(table_info)
            print(f"  📊 Table: {table_info.name} ({len(table_info.fields)} fields)", file=sys.stderr)
        
        return base_info
    
    def _discover_base_manually(self, base, base_id: str) -> BaseInfo:
        """Manually discover base schema by probing tables"""
        print("🔍 Manual discovery mode - probing for tables...", file=sys.stderr)
        
        base_info = BaseInfo(
            name="Unknown Base",
            id=base_id,
            permission_level="limited",
            tables=[]
        )
        
        # Use the existing table discovery logic from AirtableTool
        discovered_tables = self._probe_for_tables(base)
        
        for table_name in discovered_tables:
            try:
                table = base.table(table_name)
                table_info = self._analyze_table_structure(table, table_name)
                base_info.tables.append(table_info)
                print(f"  📊 Table: {table_info.name} ({len(table_info.fields)} fields)", file=sys.stderr)
            except Exception as e:
                print(f"  ❌ Could not analyze table {table_name}: {str(e)}", file=sys.stderr)
        
        return base_info
    
    def _probe_for_tables(self, base) -> List[str]:
        """Probe for table names using common patterns"""
        potential_names = [
            # Customer/Client tables
            "Customers", "Customer", "Clients", "Client", "Contacts", "Contact",
            "Accounts", "Account", "Users", "User", "Members", "Member",
            "Leads", "Lead", "Prospects", "Prospect", "People", "Person",
            
            # Generic table names
            "Table 1", "Table1", "Table 2", "Table2", "Table 3", "Table3",
            "Main Table", "Main", "Sheet1", "Sheet 1", "Data", "Records",
            
            # Business data tables
            "Orders", "Order", "Purchases", "Purchase", "Transactions", "Transaction",
            "Products", "Product", "Services", "Service", "Inventory",
            "Sales", "Revenue", "Deals", "Deal", "Opportunities", "Opportunity",
            
            # Support/Operations
            "Support", "Tickets", "Ticket", "Issues", "Issue", "Cases", "Case",
            "Tasks", "Task", "Projects", "Project", "Activities", "Activity",
            
            # Analytics/Metrics
            "Usage", "Analytics", "Metrics", "Stats", "Reports", "Report",
            "Events", "Event", "Logs", "Log", "Sessions", "Session"
        ]
        
        discovered_tables = []
        
        for table_name in potential_names:
            try:
                table = base.table(table_name)
                # Test access by trying to get one record
                _ = table.all(max_records=1)
                discovered_tables.append(table_name)
                print(f"    ✅ Found table: '{table_name}'", file=sys.stderr)
            except:
                continue
        
        return discovered_tables
    
    def _analyze_table_structure(self, table, table_name: str) -> TableInfo:
        """Analyze table structure by examining sample records"""
        try:
            # Get sample records to analyze structure
            sample_records = table.all(max_records=10)
            
            table_info = TableInfo(
                name=table_name,
                id="unknown",
                fields=[],
                record_count=len(sample_records)
            )
            
            if not sample_records:
                return table_info
            
            # Analyze field structure from sample records
            all_fields = set()
            field_types = {}
            sample_values = {}
            
            for record in sample_records:
                fields = record.get("fields", {})
                all_fields.update(fields.keys())
                
                for field_name, field_value in fields.items():
                    # Collect sample values
                    if field_name not in sample_values:
                        sample_values[field_name] = []
                    if len(sample_values[field_name]) < 3 and field_value is not None:
                        sample_values[field_name].append(field_value)
                    
                    # Infer field type
                    if field_name not in field_types:
                        field_types[field_name] = self._infer_field_type(field_value)
            
            # Create field info objects
            for field_name in sorted(all_fields):
                field_info = FieldInfo(
                    name=field_name,
                    field_type=field_types.get(field_name, "unknown"),
                    sample_values=sample_values.get(field_name, [])
                )
                table_info.fields.append(field_info)
            
            # Try to identify primary field (usually first field or one containing "name", "title", "id")
            if table_info.fields:
                for field in table_info.fields:
                    if any(keyword in field.name.lower() for keyword in ["name", "title", "primary"]):
                        table_info.primary_field = field.name
                        break
                if not table_info.primary_field:
                    table_info.primary_field = table_info.fields[0].name
            
            return table_info
            
        except Exception as e:
            print(f"❌ Error analyzing table {table_name}: {str(e)}", file=sys.stderr)
            return TableInfo(name=table_name, id="unknown", fields=[])
    
    def _infer_field_type(self, value: Any) -> str:
        """Infer Airtable field type from sample value"""
        if value is None:
            return "unknown"
        
        if isinstance(value, str):
            # Check for special string patterns
            if "@" in value and "." in value:
                return "email"
            elif value.startswith("http"):
                return "url"
            elif len(value) > 100:
                return "longText"
            else:
                return "singleLineText"
        
        elif isinstance(value, (int, float)):
            return "number"
        
        elif isinstance(value, bool):
            return "checkbox"
        
        elif isinstance(value, list):
            if value and isinstance(value[0], dict):
                return "linkedRecord"
            else:
                return "multipleSelect"
        
        elif isinstance(value, dict):
            if "url" in value and "filename" in value:
                return "attachment"
            else:
                return "formula"
        
        else:
            return "unknown"
    
    def find_customer_tables(self, base_id: str) -> List[Tuple[TableInfo, float]]:
        """
        Find tables that likely contain customer data and score their suitability
        Returns list of (TableInfo, confidence_score) tuples
        """
        base_info = self.discover_base_schema(base_id)
        if not base_info or not base_info.tables:
            return []
        
        customer_tables = []
        
        for table in base_info.tables:
            score = self._score_table_for_customer_data(table)
            if score > 0:
                customer_tables.append((table, score))
        
        # Sort by confidence score (highest first)
        customer_tables.sort(key=lambda x: x[1], reverse=True)
        
        return customer_tables
    
    def _score_table_for_customer_data(self, table: TableInfo) -> float:
        """Score a table's likelihood of containing customer data (0-100)"""
        score = 0.0
        
        # Table name patterns
        customer_keywords = [
            "customer", "client", "contact", "account", "user", "member",
            "lead", "prospect", "people", "person"
        ]
        
        table_name_lower = table.name.lower()
        for keyword in customer_keywords:
            if keyword in table_name_lower:
                score += 30
                break
        
        # Field analysis
        email_fields = 0
        name_fields = 0
        company_fields = 0
        value_fields = 0
        
        for field in table.fields:
            field_name_lower = field.name.lower()
            
            # Email fields
            if field.field_type == "email" or any(keyword in field_name_lower for keyword in ["email", "e-mail"]):
                email_fields += 1
                score += 25
            
            # Name fields
            if any(keyword in field_name_lower for keyword in ["name", "first", "last", "full"]):
                name_fields += 1
                score += 10
            
            # Company fields
            if any(keyword in field_name_lower for keyword in ["company", "organization", "business"]):
                company_fields += 1
                score += 10
            
            # Value/Revenue fields
            if any(keyword in field_name_lower for keyword in ["value", "revenue", "amount", "price"]):
                value_fields += 1
                score += 5
        
        # Bonus for having multiple customer-indicating fields
        if email_fields > 0 and name_fields > 0:
            score += 20
        
        # Penalty for very few fields (likely config tables)
        if len(table.fields) < 3:
            score -= 20
        
        return max(0, min(100, score))
    
    def generate_discovery_report(self, base_id: str) -> str:
        """Generate a comprehensive discovery report for a base"""
        base_info = self.discover_base_schema(base_id)
        
        if not base_info:
            return f"❌ Could not discover schema for base {base_id}"
        
        report = f"""
🔍 Airtable Base Discovery Report
================================

Base Information:
• Name: {base_info.name}
• ID: {base_info.id}
• Permission Level: {base_info.permission_level}
• Tables Found: {len(base_info.tables)}

"""
        
        # Customer table analysis
        customer_tables = self.find_customer_tables(base_id)
        if customer_tables:
            report += "🎯 Recommended Customer Tables:\n"
            for table, score in customer_tables[:3]:  # Top 3
                report += f"• {table.name} (confidence: {score:.1f}%)\n"
            report += "\n"
        
        # Detailed table information
        report += "📊 Table Details:\n"
        for table in base_info.tables:
            report += f"\n📋 Table: {table.name}\n"
            report += f"   • Fields: {len(table.fields)}\n"
            report += f"   • Primary Field: {table.primary_field or 'Unknown'}\n"
            report += f"   • Records: {table.record_count if table.record_count >= 0 else 'Unknown'}\n"
            
            if table.fields:
                report += "   • Key Fields:\n"
                for field in table.fields[:10]:  # Limit to first 10 fields
                    sample_str = ""
                    if field.sample_values:
                        sample_str = f" (e.g., {field.sample_values[0]})"
                    report += f"     - {field.name}: {field.field_type}{sample_str}\n"
                
                if len(table.fields) > 10:
                    report += f"     ... and {len(table.fields) - 10} more fields\n"
        
        return report
    
    def export_schema_json(self, base_id: str, output_path: Optional[str] = None) -> Dict[str, Any]:
        """Export base schema as JSON for further analysis"""
        base_info = self.discover_base_schema(base_id)
        
        if not base_info:
            return {}
        
        # Convert to JSON-serializable format
        schema_data = {
            "base": {
                "name": base_info.name,
                "id": base_info.id,
                "permission_level": base_info.permission_level,
                "discovery_timestamp": "2024-12-07"  # Could use actual timestamp
            },
            "tables": []
        }
        
        for table in base_info.tables:
            table_data = {
                "name": table.name,
                "id": table.id,
                "description": table.description,
                "record_count": table.record_count,
                "primary_field": table.primary_field,
                "fields": []
            }
            
            for field in table.fields:
                field_data = {
                    "name": field.name,
                    "type": field.field_type,
                    "description": field.description,
                    "options": field.options,
                    "sample_values": field.sample_values
                }
                table_data["fields"].append(field_data)
            
            schema_data["tables"].append(table_data)
        
        # Save to file if path provided
        if output_path:
            with open(output_path, 'w') as f:
                json.dump(schema_data, f, indent=2)
            print(f"✅ Schema exported to: {output_path}", file=sys.stderr)
        
        return schema_data


# Convenience functions for quick discovery
def discover_all_airtable_bases(api_token: Optional[str] = None) -> List[BaseInfo]:
    """Quick function to discover all accessible bases"""
    tool = AirtableDiscoveryTool(api_token)
    return tool.discover_all_bases()


def discover_base_schema(base_id: str, api_token: Optional[str] = None) -> Optional[BaseInfo]:
    """Quick function to discover schema for a specific base"""
    tool = AirtableDiscoveryTool(api_token)
    return tool.discover_base_schema(base_id)


def find_customer_tables(base_id: str, api_token: Optional[str] = None) -> List[Tuple[TableInfo, float]]:
    """Quick function to find likely customer tables in a base"""
    tool = AirtableDiscoveryTool(api_token)
    return tool.find_customer_tables(base_id)


if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python airtable_discovery.py <base_id>")
        sys.exit(1)
    
    base_id = sys.argv[1]
    
    try:
        tool = AirtableDiscoveryTool()
        
        # Generate discovery report
        report = tool.generate_discovery_report(base_id)
        print(report)
        
        # Export schema
        schema_file = f"airtable_schema_{base_id}.json"
        tool.export_schema_json(base_id, schema_file)
        
    except Exception as e:
        print(f"❌ Discovery failed: {str(e)}", file=sys.stderr)
        sys.exit(1)