# Dynamic Airtable Schema Discovery

The Customer Health Analyzer now includes **intelligent schema discovery** that automatically adapts to any Airtable database structure without requiring hardcoded field mappings.

## 🚀 Key Features

### ✅ **Intelligent Table Discovery**
- **API-First Approach**: Attempts to get base metadata via Airtable API when available
- **Smart Probing**: Systematically tests 20+ common table name patterns
- **Quality Scoring**: Analyzes each table and scores it 0-100 based on customer data suitability
- **Best Match Selection**: Automatically selects the highest-scoring table for customer data

### ✅ **Intelligent Field Mapping**  
- Automatically maps your field names to our health analysis schema
- Uses pattern matching with prioritized field name lists
- Handles both exact matches and partial matches

### ✅ **Flexible Data Types**
- Handles computed fields (objects with `value` property)
- Converts data types automatically (strings to numbers, etc.)
- Gracefully handles missing or null fields

### ✅ **Multiple Search Strategies**
1. **Primary search**: Uses discovered email field with exact matching
2. **Fallback search**: Uses customer ID field if email search fails  
3. **Broad search**: Searches all text fields for the email if other methods fail

## 🧠 Field Mapping Intelligence

The tool automatically maps your Airtable fields to these logical categories:

| Logical Field | Your Field Names (Examples) |
|---------------|----------------------------|
| **email** | Email Address, Email, contact_email, customer_email |
| **name** | Full Name, name, customer_name, contact_name, first_name |
| **company** | Company Name, company, organization, account_name |
| **account_value** | Ticket Size, account_value, revenue, deal_value, contract_value |
| **customer_id** | Customer ID, id, client_id, account_id, user_id |
| **phone** | Phone Number, phone, telephone, mobile, contact_phone |
| **engagement_score** | Customer Engagement Score, engagement, activity_score |
| **customer_type** | Customer Type, type, tier, segment, classification |
| **sentiment** | Email Sentiment Analysis, sentiment, satisfaction, feedback |
| **created_date** | Created Date, signup_date, registration_date, start_date |
| **last_contact** | Last Contact Date, last_interaction, last_activity |

## 📊 Schema Discovery Output

When processing a customer, you'll see detailed schema discovery logs:

```
🔍 Discovered field mapping:
  • email → Email Address
  • name → Full Name  
  • account_value → Ticket Size
  • customer_id → Customer ID
  • phone → Phone Number
  • engagement_score → Customer Engagement Score
  • customer_type → Customer Type
  • sentiment → Email Sentiment Analysis
```

## 🔧 Testing Your Schema

Use the provided test scripts to validate schema discovery:

```bash
# Test with your real customer data
python3 test_schema_discovery.py

# Test basic functionality
python3 test_real_customer.py
```

## 🏗️ How It Works

1. **Table Discovery**: 
   - Attempts API metadata retrieval first
   - Falls back to intelligent probing of 20+ common table names
   - Scores each table (0-100) based on customer data quality
   - Selects the best table automatically
   
2. **Schema Analysis**: Gets 10 sample records to analyze field structure comprehensively

3. **Smart Field Mapping**: Maps your field names to logical categories using intelligent patterns with priority ordering

4. **Multi-Strategy Customer Search**: 
   - Primary: Uses discovered email field with exact matching
   - Fallback: Uses customer ID field if email search fails  
   - Broad: Searches all text fields for the email if other methods fail

5. **Data Extraction**: Uses discovered mappings to extract and format data with type conversion

## 🎯 Benefits

- **Zero Configuration**: Works with any Airtable schema out of the box
- **Intelligent**: Learns your field naming conventions automatically  
- **Flexible**: Handles various data types and computed fields
- **Robust**: Multiple fallback strategies for finding customers
- **Transparent**: Shows exactly which fields were mapped and how

## 🔄 Adding New Field Patterns

To support additional field naming patterns, simply update the pattern lists in `src/agents/data_integration_agents.py`:

```python
"email": self._find_field_by_patterns(all_fields, [
    "email_address", "email", "e-mail", "e_mail", "contact_email",
    "customer_email", "user_email", "primary_email",
    "your_custom_email_field"  # Add your patterns here
]),
```

The system will automatically use your custom patterns for future schema discovery.