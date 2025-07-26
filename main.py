# main.py
"""
Pure MCP server entry point - no HTTP/FastAPI, just MCP protocol.
"""

import asyncio
from agent.mcp_server import polymarket_mcp
from agent.data_fetcher import PolymarketService


async def run_mcp_server():
    """Run the pure MCP server."""
    print("🚀 Starting Polymarket MCP Server (Pure MCP Protocol)")
    print("Available tools:", list(polymarket_mcp._tools.keys()) if hasattr(polymarket_mcp, '_tools') else "Unknown")
    print("Available resources:",
          list(polymarket_mcp._resources.keys()) if hasattr(polymarket_mcp, '_resources') else "Unknown")
    print("Ready for MCP connections...")

    # Keep server running
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\n👋 Shutting down MCP server...")


if __name__ == "__main__":
    asyncio.run(run_mcp_server())