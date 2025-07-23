# Airtable Integration Setup Guide

## Prerequisites

1. **Airtable Account** with a base containing customer data
2. **Airtable API Key** (Personal Access Token)
3. **Base ID** from your Airtable base

## Step 1: Get Your Airtable Credentials

### Get Personal Access Token (PAT)
1. Go to https://airtable.com/create/tokens
2. Click **"Create new token"**
3. Configure your token:
   - **Name**: "Customer Health Analyzer"
   - **Scopes**: Select these permissions:
     - `data.records:read` - Read records from tables
     - `schema.bases:read` - Read base schema information
   - **Access**: Choose "Add a base" and select your customer data base
4. Click **"Create token"**
5. Copy the token (starts with `pat...`) - **Save this immediately, you won't see it again!**

### Get Base ID
1. Go to your Airtable base
2. Click **Help** → **API Documentation** 
3. Copy the Base ID from the URL or documentation (starts with `app...`)

**Alternative method for Base ID:**
1. Open your base in Airtable
2. Look at the URL: `https://airtable.com/app123456789abcdef/...`
3. The Base ID is the part after `/app` (e.g., `app123456789abcdef`)

## Step 2: Required Airtable Schema

Create these tables in your Airtable base:

### Table 1: "Customers"
| Field Name | Type | Description |
|------------|------|-------------|
| Customer ID | Single line text | Unique identifier (e.g., CUST001) |
| Email | Email | Customer email address |
| Company Name | Single line text | Company name |
| Account Value | Currency | Contract/account value |
| Contract End Date | Date | When contract expires |
| CSM Name | Single line text | Customer Success Manager |
| Last Contact Date | Date | Last touchpoint |
| Contact Outcome | Single select | positive, negative, neutral, no_response |

### Table 2: "Usage" (Optional)
| Field Name | Type | Description |
|------------|------|-------------|
| Customer Email | Email | Links to customer |
| Date | Date | Usage date |
| Feature Used | Single line text | Feature name (login, dashboard, reports) |
| Session Duration | Number | Minutes |
| Usage Count | Number | Number of times used |

### Table 3: "Support" (Optional)
| Field Name | Type | Description |
|------------|------|-------------|
| Customer Email | Email | Links to customer |
| Ticket ID | Single line text | Support ticket identifier |
| Status | Single select | open, closed, pending |
| Priority | Single select | low, medium, high, critical |
| Resolution Time Hours | Number | Time to resolve |
| Created Date | Date | When ticket was created |

## Step 3: Update Configuration

Add to your `.env` file:
```
# Airtable Configuration - Use Personal Access Token (PAT)
AIRTABLE_API_KEY=pat_1234567890abcdef.1234567890abcdef123456  # Your PAT token
AIRTABLE_BASE_ID=app123456789abcdef                            # Your Base ID

# Switch to Airtable as data source
USE_STATIC_DATA=false
DEFAULT_DATA_SOURCE=airtable
```

**Important Notes:**
- The token starts with `pat_` followed by a long string
- Keep your token secure - don't share it or commit it to version control
- If you lose the token, you'll need to create a new one

## Step 4: Test the Integration

Use these Claude Desktop prompts:
1. **"List all available customers"** - Should show Airtable customers
2. **"Analyze customer health for [customer_email]"** - Replace with real email
3. **"Show me recommendations for [company_name]"**

## Sample Data for Testing

If you want to test with sample data, create these records:

### Customers Table:
| Customer ID | Email | Company Name | Account Value | Contract End Date | CSM Name | Last Contact Date | Contact Outcome |
|-------------|-------|--------------|---------------|-------------------|----------|-------------------|-----------------|
| CUST001 | john@techcorp.com | TechCorp Inc | $50,000 | 2025-03-15 | Sarah Johnson | 2024-11-28 | positive |
| CUST002 | mary@datasolutions.com | DataSolutions LLC | $120,000 | 2025-06-30 | Michael Chen | 2024-11-30 | positive |

### Usage Table:
| Customer Email | Date | Feature Used | Session Duration | Usage Count |
|----------------|------|--------------|------------------|-------------|
| john@techcorp.com | 2024-12-01 | login | 45 | 15 |
| john@techcorp.com | 2024-12-01 | dashboard | 30 | 12 |
| mary@datasolutions.com | 2024-12-01 | login | 60 | 20 |

## Troubleshooting

**"Airtable API key not configured":**
- Check your `.env` file has the correct `AIRTABLE_API_KEY`
- Ensure the token has the right scopes

**"No customer found":**
- Check the email address matches exactly
- Verify the "Customers" table name is correct
- Check the "Email" field name matches

**"Base ID not found":**
- Verify `AIRTABLE_BASE_ID` in `.env`
- Check the base ID is correct (starts with `app`)

## Next Steps

Once Airtable integration works:
1. **Import your real customer data** into Airtable
2. **Customize field mappings** if your schema differs
3. **Add more tables** for additional data sources
4. **Set up automations** in Airtable to update data