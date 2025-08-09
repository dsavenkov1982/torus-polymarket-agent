import json
import os
from pathlib import Path
from fastmcp import FastMCP
from agent.data_fetcher import PolymarketService
from typing import Dict, Any, Optional

polymarket_mcp = FastMCP(name="Polymarket MCP Agent")
service = PolymarketService()

def safe_json_response(data: Any) -> Dict[str, Any]:
    try:
        json.dumps(data, ensure_ascii=False)
        return data
    except (TypeError, ValueError) as e:
        return {
            "success": False,
            "error": f"JSON serialization error: {str(e)}",
            "raw_data": str(data)[:500]
        }

def load_resource_file(filename: str) -> str:
    resources_dir = Path(__file__).parent / "resources"
    file_path = resources_dir / filename
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        return f"Resource file not found: {filename}"
    except Exception as e:
        return f"Error loading resource: {str(e)}"

@polymarket_mcp.tool()
async def query_polymarket(query: str) -> Dict[str, Any]:
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

@polymarket_mcp.resource("polymarket://market-analysis-template")
def market_analysis_template():
    return load_resource_file("market-analysis-template.md")

@polymarket_mcp.resource("polymarket://crypto-markets-summary")
def crypto_markets_summary():
    return load_resource_file("crypto-markets-summary.md")

@polymarket_mcp.resource("polymarket://trading-strategies-guide")
def trading_strategies_guide():
    return load_resource_file("trading-strategies-guide.md")

@polymarket_mcp.resource("polymarket://api-documentation")
def api_documentation():
    return load_resource_file("api-documentation.md")

def get_server_capabilities():
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

if __name__ == "__main__":
    polymarket_mcp.run()