# agent/clob_api_client.py
import asyncio
import httpx
from loguru import logger
from typing import Dict, List, Any, Optional


class PolymarketCLOBClient:
    """
    Client for Polymarket CLOB API

    Note: The /trades endpoint may require authentication or have rate limits.
    For read-only market data, use the public Gamma API instead.
    """

    GAMMA_API_URL = "https://gamma-api.polymarket.com"
    CLOB_API_URL = "https://clob.polymarket.com"

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize CLOB client

        Args:
            api_key: Optional API key for authenticated endpoints
        """
        headers = {
            "User-Agent": "Polymarket-Indexer/1.0"
        }

        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers=headers,
            follow_redirects=True
        )

    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()

    async def get_markets(self, limit: int = 50, active: bool = True) -> List[Dict[str, Any]]:
        """
        Get markets from Gamma API (public, no auth required)
        """
        try:
            params = {
                "limit": limit,
                "active": str(active).lower()
            }

            response = await self.client.get(
                f"{self.GAMMA_API_URL}/markets",
                params=params
            )
            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error(f"Error fetching markets: {e}")
            return []

    async def get_market_trades_from_gamma(
            self,
            condition_id: str,
            limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get trades for a market from Gamma API

        Note: Gamma API provides market data but may not have detailed trade history.
        For detailed trades, you may need CLOB API authentication or use blockchain data.
        """
        try:
            # Try to get market details which may include recent trades
            response = await self.client.get(
                f"{self.GAMMA_API_URL}/markets/{condition_id}"
            )
            response.raise_for_status()
            market_data = response.json()

            # Gamma API doesn't provide detailed trade history
            # Return empty for now - use blockchain indexing for trades
            logger.warning(f"Gamma API doesn't provide detailed trades for {condition_id}")
            return []

        except Exception as e:
            logger.error(f"Error fetching market data for {condition_id}: {e}")
            return []

    async def get_recent_trades_from_markets(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get recent active markets and extract what trade info we can

        Since CLOB API requires auth, we'll focus on getting market data
        and rely on blockchain indexing for actual trades.
        """
        try:
            # Get active markets
            markets = await self.get_markets(limit=limit, active=True)

            logger.info(f"Retrieved {len(markets)} active markets")

            # For each market, we could try to get orderbook data
            # but detailed trades require either:
            # 1. CLOB API authentication
            # 2. Blockchain indexing (which you're already doing)

            return []  # Return empty since we can't get trades without auth

        except Exception as e:
            logger.error(f"Error fetching recent trades: {e}")
            return []

    async def get_orderbook(self, condition_id: str) -> Optional[Dict[str, Any]]:
        """
        Get current orderbook for a market

        This endpoint may work without authentication but is rate-limited
        """
        try:
            response = await self.client.get(
                f"{self.CLOB_API_URL}/book",
                params={"token_id": condition_id}
            )

            if response.status_code == 401:
                logger.warning(f"Orderbook endpoint requires authentication")
                return None

            response.raise_for_status()
            return response.json()

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                logger.warning(f"Orderbook requires authentication for {condition_id}")
            else:
                logger.error(f"Error fetching orderbook for {condition_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error fetching orderbook for {condition_id}: {e}")
            return None


async def test_clob_api():
    """Test the CLOB API"""
    client = PolymarketCLOBClient()

    try:
        logger.info("Testing Polymarket Gamma API (public endpoints)...")

        # Test markets
        markets = await client.get_markets(limit=5)
        logger.info(f"✓ Found {len(markets)} markets")

        if markets:
            market = markets[0]
            logger.info(f"✓ Sample market: {market.get('question', 'No question')[:80]}")
            logger.info(f"  Volume: ${market.get('volume', 0):,.2f}")

        logger.info("\n" + "=" * 80)
        logger.info("Note: Trade history requires either:")
        logger.info("  1. CLOB API authentication (contact Polymarket for API key)")
        logger.info("  2. Blockchain indexing (which you're already doing!)")
        logger.info("=" * 80)

    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(test_clob_api())