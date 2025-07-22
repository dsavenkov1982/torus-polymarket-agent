# agent/mcp_endpoints.py

from fastmcp import FastMCP
import httpx

mcp = FastMCP(name="Polymarket MCP Agent")

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"

@mcp.tool()
async def get_events(limit: int = 20):
    """Returns latest Polymarket events (default: 20)."""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{GAMMA_BASE_URL}/events", params={"limit": limit})
        response.raise_for_status()
        return response.json()

@mcp.tool()
async def get_markets(limit: int = 20):
    """Returns latest Polymarket markets (default: 20)."""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{GAMMA_BASE_URL}/markets", params={"limit": limit})
        response.raise_for_status()
        return response.json()

@mcp.resource("polymarket://discovery")
def discovery():
    """Returns metadata on supported Polymarket MCP tools."""
    return [
        {"name": "get_events", "description": get_events.__doc__},
        {"name": "get_markets", "description": get_markets.__doc__},
    ]

if __name__ == "__main__":
    mcp.run()
