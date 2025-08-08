import logging
import sys
from agent.mcp_server import polymarket_mcp


def setup_logging():
    """Configure logging to file only (not stdout)."""
    logging.basicConfig(
        level=logging.INFO,
        filename='polymarket_mcp.log',
        filemode='a',
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)


def main():
    """Main entry point for the MCP server."""
    logger = setup_logging()

    try:
        logger.info("Starting Polymarket MCP Server")

        # FastMCP.run() manages its own event loop - call it directly
        polymarket_mcp.run()

    except KeyboardInterrupt:
        logger.info("Server stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Server error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()