# agent/mcp_server.py
"""
Pure MCP server - only MCP protocol, no HTTP endpoints.
"""

from fastmcp import FastMCP
from agent.data_fetcher import PolymarketService
from typing import Dict, Any, Optional

# Pure MCP server
polymarket_mcp = FastMCP(name="Polymarket MCP Agent")
service = PolymarketService()


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
        return {
            "success": True,
            "query": query,
            "result": result
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "query": query
        }


@polymarket_mcp.tool()
async def get_events(limit: int = 20, search: Optional[str] = None) -> Dict[str, Any]:
    """Get Polymarket events with optional filtering."""
    try:
        result = await service.fetch_events(limit=limit)
        if search:
            result = service._filter_by_search_terms(result, [search])
        return {"success": True, "data": result}
    except Exception as e:
        return {"success": False, "error": str(e)}


@polymarket_mcp.tool()
async def get_markets(limit: int = 20, search: Optional[str] = None) -> Dict[str, Any]:
    """Get Polymarket markets with optional filtering."""
    try:
        result = await service.fetch_markets(limit=limit)
        if search:
            result = service._filter_by_search_terms(result, [search])
        return {"success": True, "data": result}
    except Exception as e:
        return {"success": False, "error": str(e)}


@polymarket_mcp.tool()
async def search_polymarket_data(search_terms: str, limit: int = 20) -> Dict[str, Any]:
    """Search across both events and markets."""
    try:
        events = await service.fetch_events(limit=limit)
        markets = await service.fetch_markets(limit=limit)

        filtered_events = service._filter_by_search_terms(events, search_terms.split())
        filtered_markets = service._filter_by_search_terms(markets, search_terms.split())

        return {
            "success": True,
            "search_terms": search_terms,
            "data": {
                "events": filtered_events,
                "markets": filtered_markets
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# ========== MCP Resources ==========

@polymarket_mcp.resource("polymarket://capabilities")
def get_capabilities():
    """Server capabilities for AI agent discovery."""
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


@polymarket_mcp.resource("polymarket://schema")
def get_schema():
    """Data schema for AI agent understanding."""
    return service.get_schema()


@polymarket_mcp.resource("polymarket://tools")
def get_tools_info():
    """Available tools for AI agent discovery."""
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