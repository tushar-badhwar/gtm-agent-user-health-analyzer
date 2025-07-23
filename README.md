# Customer Health Analyzer MCP Server

An MCP (Model Context Protocol) server that provides AI-powered customer health analysis tools for GTM (Go-To-Market) teams. Analyzes customer usage, CRM, and support data to provide health scores and actionable recommendations.

## Features

- **AI-Powered Analysis**: Uses CrewAI agents with OpenAI GPT for intelligent customer health scoring
- **Comprehensive Scoring**: Analyzes usage, relationship, and support metrics
- **Actionable Recommendations**: Provides specific next steps with priorities and timelines
- **MCP Integration**: Works seamlessly with Claude Desktop and other MCP clients
- **Real-time Analysis**: Process customer data on-demand through MCP tools
- **Dynamic Data Sources**: Connects to HubSpot, Airtable, Zapier, or static data

## Architecture

### CrewAI Multi-Agent System
- **Data Integration Agents**: HubSpot, Airtable, Zapier specialists
- **Health Analysis Agent**: Calculates comprehensive health scores
- **AI Recommendation Agent**: Generates personalized action plans
- **Orchestrator**: Coordinates workflow and data synthesis

### Data Sources (Dynamic)
- **HubSpot CRM**: Contact data, deals, engagement metrics
- **Airtable**: Custom customer databases and usage tracking
- **Zapier**: Multi-platform data aggregation via webhooks
- **Static Data**: CSV files for demo/development

### Scoring Model
- **Usage Score (40%)**: Login frequency, session duration, feature adoption, usage trends
- **Relationship Score (30%)**: Contact recency, sentiment, contract status
- **Support Score (30%)**: Open tickets, resolution times, sentiment, escalations

### Health Status
- **Healthy** (80-100): Customer is engaged and satisfied
- **At Risk** (60-79): Customer needs attention to prevent churn
- **Critical** (0-59): Customer requires immediate intervention

## Quick Start

### 1. Setup Environment

```bash
# Navigate to project directory
cd gtm-agent-user-health-analyzer

# Install dependencies
pip install -r requirements.txt

# Setup environment variables
cp .env.example .env
# Edit .env and add your API keys
```

### 2. Configure MCP Client

Add this server to your MCP client configuration (e.g., Claude Desktop):

#### For Windows with WSL:
```json
{
  "mcpServers": {
    "customer-health-analyzer": {
      "command": "C:\\Windows\\System32\\wsl.exe",
      "args": ["python3", "/home/tbadhwar/gtm-agent-user-health-analyzer/server.py"],
      "env": {
        "OPENAI_API_KEY": "your_openai_api_key_here",
        "OPENAI_MODEL": "gpt-4o-mini",
        "USE_STATIC_DATA": "true",
        "DEFAULT_DATA_SOURCE": "static",
        "AIRTABLE_API_KEY": "your_airtable_api_key_here",
        "AIRTABLE_BASE_ID": "your_airtable_base_id_here"
      }
    }
  }
}
```

#### For Linux/macOS:
```json
{
  "mcpServers": {
    "customer-health-analyzer": {
      "command": "python3",
      "args": ["/path/to/gtm-agent-user-health-analyzer/server.py"],
      "env": {
        "OPENAI_API_KEY": "your_openai_api_key_here",
        "OPENAI_MODEL": "gpt-4o-mini",
        "USE_STATIC_DATA": "true",
        "DEFAULT_DATA_SOURCE": "static",
        "AIRTABLE_API_KEY": "your_airtable_api_key_here",
        "AIRTABLE_BASE_ID": "your_airtable_base_id_here"
      }
    }
  }
}
```

### 3. Test the Server

```bash
# Test the server directly
python server.py

# Or install and run
pip install -e .
customer-health-analyzer-mcp
```

## Available MCP Tools

### 1. `set_data_source`
**NEW**: Select which data source to use for customer health analysis.

**Parameters:**
- `data_source` (required): "static", "airtable", "hubspot", or "zapier"

**Example:**
```
Use the set_data_source tool with data_source "airtable" to switch to your Airtable database
```

### 2. `get_data_source_status`
**NEW**: Show current data source configuration and available options.

**Example:**
```
Use the get_data_source_status tool to see which data sources are configured
```

### 3. `analyze_customer_health`
Analyze customer health scores for all customers or a specific customer from the currently selected data source.

**Parameters:**
- `customer_id` (optional): Specific customer ID to analyze (e.g., 'CUST001')
- `format` (optional): Output format - 'detailed' or 'summary' (default: 'detailed')

**Example:**
```
Use the analyze_customer_health tool to get a complete health analysis for all customers
```

### 4. `list_customers`
List all available customers in the currently selected data source.

**Example:**
```
Use the list_customers tool to see what customers are available for analysis
```

### 5. `get_customer_details`
Get detailed information about a specific customer.

**Parameters:**
- `customer_id` (required): Customer ID to get details for

**Example:**
```
Use the get_customer_details tool with customer_id "CUST001" to see detailed customer information
```

### 6. `get_recommendations`
Get AI-powered recommendations for improving customer health.

**Parameters:**
- `customer_id` (required): Customer ID to get recommendations for

**Example:**
```
Use the get_recommendations tool with customer_id "CUST003" to get specific action items for this at-risk customer
```

## Sample Data

The server includes sample data for demonstration:

- **5 customers** with different health profiles:
  - CUST001: TechCorp Inc (Healthy)
  - CUST002: DataSolutions LLC (Healthy) 
  - CUST003: StartupXYZ (At Risk)
  - CUST004: Enterprise Global (Healthy)
  - CUST005: SmallBiz Co (Critical)

- **Usage data**: Login patterns, feature adoption, session durations
- **CRM data**: Contact history, sentiment, contract details  
- **Support data**: Tickets, resolution times, sentiments

## Configuration

### Environment Variables

```bash
# OpenAI Configuration
OPENAI_API_KEY=your_openai_api_key_here
OPENAI_MODEL=gpt-4o-mini

# CRM Integration API Keys
HUBSPOT_API_KEY=your_hubspot_api_key_here
AIRTABLE_API_KEY=your_airtable_api_key_here
AIRTABLE_BASE_ID=your_airtable_base_id_here
ZAPIER_API_KEY=your_zapier_api_key_here

# Data Source Configuration
USE_STATIC_DATA=true  # Set to false to use real CRM integrations
DEFAULT_DATA_SOURCE=static  # static, hubspot, airtable, zapier
```

### Data Source Modes

**Static Mode (Default)**: Uses CSV demo data for testing
**Dynamic Mode**: Connects to real CRM systems via CrewAI agents

To switch to dynamic mode:
1. Add your CRM API keys to .env
2. Set `USE_STATIC_DATA=false`
3. Configure your preferred `DEFAULT_DATA_SOURCE`

## Example Usage with Claude Desktop

Once configured, you can ask Claude to:

1. **"Switch to my Airtable database and show me all customers"**
   - Uses `set_data_source` tool to switch to "airtable"
   - Uses `list_customers` tool to show all Airtable customers

2. **"What data sources do I have configured?"**
   - Uses `get_data_source_status` tool
   - Shows current source and available options

3. **"Analyze the health of all our customers"**
   - Uses `analyze_customer_health` tool
   - Returns comprehensive health scores and recommendations

4. **"Show me details for customer CUST003"**
   - Uses `get_customer_details` tool
   - Returns usage, CRM, and support data

5. **"What should we do to help our at-risk customers?"**
   - Uses `analyze_customer_health` with summary format
   - Provides prioritized action items

6. **"Give me specific recommendations for StartupXYZ"**
   - Uses `get_recommendations` tool
   - Returns AI-generated action plan

## Example Output

```
Customer Health Summary Report
================================

Total Customers Analyzed: 4
Average Health Score: 73.5/100

Health Status Distribution:
- Healthy: 2 customers (50.0%)
- At Risk: 1 customers (25.0%)
- Critical: 1 customers (25.0%)

StartupXYZ (ID: CUST003)
Health Score: 45/100 - At Risk

Detailed Scores:
- Usage: 30/100
- Relationship: 25/100  
- Support: 70/100

Recommended Actions:
1. Schedule product training session
   Priority: HIGH
   Timeline: Within 1 week
   Reasoning: Low usage indicates need for better product understanding

2. Schedule check-in call with CSM
   Priority: HIGH
   Timeline: Within 3 days
   Reasoning: Poor relationship score indicates need for immediate outreach
```

## Project Structure

```
gtm-agent-user-health-analyzer/
├── server.py                        # Main MCP server
├── src/
│   ├── orchestrator.py              # CrewAI workflow orchestrator
│   ├── agents/
│   │   ├── data_integration_agents.py   # HubSpot, Airtable, Zapier agents
│   │   └── health_analysis_agents.py    # Health scoring and recommendation agents
│   └── models/
│       └── customer_health.py       # Pydantic models
├── data/
│   ├── sample_usage_data.csv        # Sample usage metrics
│   ├── sample_crm_data.csv          # Sample CRM data
│   └── sample_support_data.csv      # Sample support data
├── requirements.txt                 # Python dependencies
├── pyproject.toml                  # Package configuration
├── mcp.json                        # MCP server configuration
├── .env.example                    # Environment template
└── README.md                       # This file
```

## Development

### Adding New Data Sources

To integrate with real CRM/support systems:

1. **Extend the `CustomerHealthAnalyzer` class** in `server.py`
2. **Add new collection methods** for your data sources
3. **Update scoring algorithms** as needed
4. **Add new MCP tools** for specific integrations

### Customizing Scoring

Modify the scoring methods in `server.py`:
- `calculate_usage_score()`
- `calculate_relationship_score()`
- `calculate_support_score()`

### Testing

```bash
# Install in development mode
pip install -e .

# Run the server
python server.py

# Test with MCP client or direct JSON-RPC calls
```

## Integration Roadmap

### Phase 2: Real Data Sources
- HubSpot CRM integration
- Salesforce integration  
- Support system APIs (Zendesk, Intercom)
- Product analytics (Mixpanel, Amplitude)

### Phase 3: Advanced Features
- Predictive churn modeling
- Historical trend analysis
- Custom scoring models
- Automated alerting via MCP

## Troubleshooting

**Server won't start:**
- Check that all dependencies are installed: `pip install -r requirements.txt`
- Verify OPENAI_API_KEY is set in .env file or Claude Desktop config
- Ensure Python path is correct in MCP configuration
- For WSL: Verify `C:\\Windows\\System32\\wsl.exe` path is correct

**Tools disappear after usage:**
- This indicates server crashed due to unhandled exceptions
- Restart server: Stop (Ctrl+C) and run `python3 server.py` again
- Check server logs for error details

**No customer data:**
- Verify data files exist in `data/` directory
- Check file permissions and format
- Use `get_data_source_status` tool to verify configuration
- Use `set_data_source` tool to switch between static and Airtable

**Data inconsistency between tools:**
- All tools now use consistent data source routing
- Use `set_data_source` to change data source for all tools
- Restart server if switching between data sources

**AI recommendations not working:**
- Verify OPENAI_API_KEY is valid
- Check internet connectivity
- Server will fall back to rule-based recommendations if AI fails

## License

MIT License - Feel free to use and modify for your needs.

---

Built with MCP, CrewAI, OpenAI GPT, and Python.