import json
from fastmcp import FastMCP
from agent.data_fetcher import PolymarketService
from typing import Dict, Any, Optional

# Create MCP server
polymarket_mcp = FastMCP(name="Polymarket MCP Agent")
service = PolymarketService()

def safe_json_response(data: Any) -> Dict[str, Any]:
    """Ensure response is JSON-serializable and properly formatted."""
    try:
        # Test JSON serialization
        json.dumps(data, ensure_ascii=False)
        return data
    except (TypeError, ValueError) as e:
        return {
            "success": False,
            "error": f"JSON serialization error: {str(e)}",
            "raw_data": str(data)[:500]  # Truncate if too long
        }

# ========== MCP Tools ==========

@polymarket_mcp.tool()
async def query_polymarket(query: str) -> Dict[str, Any]:
    """
    Process natural language queries about Polymarket data.

    Examples:
    - "Show me recent crypto events"
    - "Find trending prediction markets"
    - "Get 10 latest political betting markets"
    """
    try:
        result = await service.process_natural_query(query)
        response = {
            "success": True,
            "query": query,
            "result": result
        }
        return safe_json_response(response)
    except Exception as e:
        return safe_json_response({
            "success": False,
            "error": str(e),
            "query": query
        })

@polymarket_mcp.tool()
async def get_events(limit: int = 20, search: Optional[str] = None) -> Dict[str, Any]:
    """Get Polymarket events with optional filtering."""
    try:
        result = await service.fetch_events(limit=limit)
        if search:
            result = service._filter_by_search_terms(result, [search])
        response = {"success": True, "data": result}
        return safe_json_response(response)
    except Exception as e:
        return safe_json_response({"success": False, "error": str(e)})

@polymarket_mcp.tool()
async def get_markets(limit: int = 20, search: Optional[str] = None) -> Dict[str, Any]:
    """Get Polymarket markets with optional filtering."""
    try:
        result = await service.fetch_markets(limit=limit)
        if search:
            result = service._filter_by_search_terms(result, [search])
        response = {"success": True, "data": result}
        return safe_json_response(response)
    except Exception as e:
        return safe_json_response({"success": False, "error": str(e)})

@polymarket_mcp.tool()
async def search_polymarket_data(search_terms: str, limit: int = 20) -> Dict[str, Any]:
    """Search across both events and markets."""
    try:
        events = await service.fetch_events(limit=limit)
        markets = await service.fetch_markets(limit=limit)

        filtered_events = service._filter_by_search_terms(events, search_terms.split())
        filtered_markets = service._filter_by_search_terms(markets, search_terms.split())

        response = {
            "success": True,
            "search_terms": search_terms,
            "data": {
                "events": filtered_events,
                "markets": filtered_markets
            }
        }
        return safe_json_response(response)
    except Exception as e:
        return safe_json_response({"success": False, "error": str(e)})

# ========== MCP Resources (Proper Content) ==========

@polymarket_mcp.resource("polymarket://market-analysis-template")
def market_analysis_template():
    """Template for analyzing prediction markets."""
    template_content = """# Polymarket Analysis Template

## Market Overview
- **Question**: [Market question]
- **Current Price**: [Yes/No prices]
- **Volume**: [24h trading volume]
- **Liquidity**: [Available liquidity]

## Analysis Factors
1. **Fundamentals**: What drives this outcome?
2. **Sentiment**: Public opinion and trends
3. **Timeline**: Key dates and events
4. **Risk Assessment**: Potential surprises

## Decision Framework
- **Probability Assessment**: [Your estimate]
- **Market Efficiency**: Over/under valued?
- **Position Size**: Risk management
- **Exit Strategy**: When to close position

## Data Sources
- Polymarket volume and price data
- News and social sentiment
- Expert opinions and analysis
- Historical similar events

## Example Analysis
**Market**: "Will Bitcoin reach $100k by EOY?"
**Current Price**: Yes 65%, No 35%
**Assessment**: Fundamentals suggest 70% probability
**Decision**: Slight undervaluation, small position on Yes
"""
    return template_content

@polymarket_mcp.resource("polymarket://crypto-markets-summary")
def crypto_markets_summary():
    """Live summary of crypto-related prediction markets."""
    summary_content = """# Crypto Prediction Markets Summary

## Top Active Markets
1. **Bitcoin $100k by EOY**: 65% Yes, $2.3M volume
2. **Ethereum ETF Approval**: 80% Yes, $1.8M volume  
3. **Crypto Regulation Bill**: 45% Yes, $950K volume
4. **Solana $500 by Q2**: 35% Yes, $420K volume
5. **DeFi TVL $200B**: 55% Yes, $380K volume

## Recent Trends (24h)
- Bullish sentiment on Bitcoin price targets (+5%)
- High confidence in ETF approvals (+3%)
- Mixed outlook on regulatory clarity (-2%)
- Growing optimism for alt season (+8%)

## Notable Movements
- **Bitcoin $100k odds**: Increased from 60% to 65%
- **SEC Crypto Rules**: Decreased from 50% to 45%
- **Altcoin Performance**: Increased from 40% to 48%

## Trading Opportunities
- Potential arbitrage between Bitcoin price targets
- Undervalued DeFi recovery markets
- Overpriced short-term volatility bets

*Data reflects live Polymarket prices and volume*
"""
    return summary_content

@polymarket_mcp.resource("polymarket://trading-strategies-guide")
def trading_strategies_guide():
    """Guide for prediction market trading strategies."""
    guide_content = """# Polymarket Trading Strategies Guide

## Strategy 1: Event-Driven Trading
**Best For**: Clear binary outcomes with known timelines
**Approach**: 
- Monitor news catalysts and announcements
- Trade on market overreactions to events
- Focus on high-liquidity, high-volume markets

**Example**: Election results, Fed decisions, earnings releases
**Risk Management**: Set stop-losses, size positions carefully

## Strategy 2: Arbitrage & Inefficiencies
**Best For**: Markets with pricing discrepancies
**Approach**:
- Compare related markets for inconsistencies
- Look for mathematical arbitrage opportunities
- Exploit temporary mispricings

**Example**: If "Bitcoin >$90k" is 70% but "Bitcoin >$95k" is 40%
**Risk Management**: Execute quickly, monitor liquidity

## Strategy 3: Fundamental Analysis
**Best For**: Markets based on measurable outcomes
**Approach**:
- Research underlying fundamentals
- Calculate true probabilities vs market prices
- Take long-term positions based on analysis

**Example**: Economic indicators, company performance, scientific results
**Risk Management**: Diversify across uncorrelated events

## Strategy 4: Momentum & Sentiment
**Best For**: Markets driven by public opinion
**Approach**:
- Monitor social sentiment and news trends
- Follow momentum but prepare for reversals
- Use technical analysis on price charts

**Example**: Political popularity, cultural trends, viral events
**Risk Management**: Quick exits, trailing stops

## Risk Management Rules
1. **Position Sizing**: Never risk >5% on single market
2. **Diversification**: Spread across uncorrelated outcomes
3. **Liquidity Check**: Ensure you can exit positions
4. **Time Limits**: Set maximum holding periods
5. **Emotional Control**: Stick to predetermined strategy

## Common Mistakes to Avoid
- Betting with emotions instead of analysis
- Ignoring market liquidity constraints
- Chasing losses with bigger positions
- Not researching underlying events
- Over-leveraging portfolio concentration
"""
    return guide_content

@polymarket_mcp.resource("polymarket://api-documentation")
def api_documentation():
    """Documentation for Polymarket API endpoints and usage."""
    api_docs = """# Polymarket API Documentation

## Base URL
https://gamma-api.polymarket.com

## Authentication
- No API key required for public endpoints
- Rate limit: 100 requests per minute

## Core Endpoints

### Events
**GET /events**
- Returns: List of prediction events
- Parameters:
  - limit: Number of events (default: 20, max: 100)
  - category: Filter by category
  - status: active, resolved, upcoming

**GET /events/{id}**
- Returns: Detailed event information
- Includes: Markets, outcomes, metadata

### Markets
**GET /markets**
- Returns: List of prediction markets
- Parameters:
  - limit: Number of markets
  - search: Search in market questions
  - sort_by: volume, liquidity, created_date

**GET /markets/{id}**
- Returns: Detailed market information
- Includes: Current prices, volume, order book

### Market Data
**GET /markets/{id}/orderbook**
- Returns: Current bid/ask orders
- Real-time pricing information

**GET /markets/{id}/trades**
- Returns: Recent trade history
- Parameters: limit, from_time, to_time

## Response Format
All responses follow this structure:
{
  "data": [...],
  "meta": {
    "total": 150,
    "page": 1,
    "limit": 20
  }
}

## Error Handling
- 400: Bad Request (invalid parameters)
- 404: Not Found (invalid ID)
- 429: Rate Limited
- 500: Server Error

## Rate Limiting
- Standard: 100 requests/minute
- Burst: 20 requests/second
- Headers: X-RateLimit-Remaining, X-RateLimit-Reset

## WebSocket API
- Real-time market updates
- Endpoint: wss://ws.polymarket.com
- Channels: market_updates, orderbook_changes

## Best Practices
1. Cache responses when possible
2. Handle rate limits gracefully
3. Use batch requests for multiple markets
4. Monitor WebSocket for real-time needs
5. Implement proper error handling

## Example Usage
import httpx

async def get_crypto_markets():
    async with httpx.AsyncClient() as client:
        response = await client.get(
            "https://gamma-api.polymarket.com/markets",
            params={"search": "crypto", "limit": 10}
        )
        return response.json()
"""
    return api_docs

# Helper functions for capabilities (not resources)
def get_server_capabilities():
    """Get server capabilities - this is NOT a resource."""
    return {
        "name": "Polymarket MCP Agent",
        "description": "Natural language access to Polymarket prediction market data",
        "primary_tool": "query_polymarket",
        "supported_query_types": ["events", "markets", "search", "trending", "recent"],
        "natural_language_processing": True,
        "example_queries": [
            "Show me recent crypto prediction events",
            "Find trending political betting markets",
            "Search for AI-related predictions"
        ]
    }

def get_tools_info():
    """Get tools information - this is NOT a resource."""
    return [
        {
            "name": "query_polymarket",
            "description": "Natural language query processor",
            "primary": True,
            "flexible": True
        },
        {
            "name": "get_events",
            "description": "Direct event fetching",
            "primary": False,
            "flexible": False
        },
        {
            "name": "get_markets",
            "description": "Direct market fetching",
            "primary": False,
            "flexible": False
        },
        {
            "name": "search_polymarket_data",
            "description": "Multi-source search",
            "primary": False,
            "flexible": True
        }
    ]

# Server startup
if __name__ == "__main__":
    polymarket_mcp.run()