# core/tasks/hybrid_indexer.py
import asyncio
from datetime import datetime
from celery import shared_task
from loguru import logger
from typing import Dict, List, Any

from agent.clob_api_client import PolymarketCLOBClient
from core.tasks.polymarket_sql_indexer import PolymarketSQLIndexer
from core.tasks.blockchain_indexer import PolygonBlockchainIndexer
from settings import settings


class HybridPolymarketIndexer:
    """
    Hybrid indexer that combines:
    1. Blockchain data (for market creation/resolution/trades)
    2. CLOB API data (for market metadata and current state)

    Note: CLOB trade endpoints require authentication.
    We primarily rely on blockchain indexing for trades.
    """

    def __init__(self, settings):
        self.settings = settings
        self.blockchain_indexer = PolygonBlockchainIndexer(settings)

        # Get API key from settings if available
        api_key = getattr(settings, 'POLYMARKET_API_KEY', None)
        self.clob_client = PolymarketCLOBClient(api_key=api_key)
        self.sql_indexer = PolymarketSQLIndexer(settings)

    async def initialize(self):
        await self.sql_indexer.connect()

    async def cleanup(self):
        await self.sql_indexer.close()
        await self.clob_client.close()

    async def index_all_data(self):
        """Index both blockchain and available CLOB data"""
        try:
            logger.info("Starting hybrid indexing (Blockchain + CLOB)")

            # 1. Index blockchain data (markets, resolutions, trades)
            logger.info("Indexing blockchain data...")
            await self.blockchain_indexer.initialize()
            await self.blockchain_indexer.index_blockchain_data()
            await self.blockchain_indexer.cleanup()

            # 2. Enrich with CLOB market metadata (public API)
            logger.info("Enriching market metadata from CLOB...")
            await self._enrich_market_metadata()

            # 3. Update metrics
            logger.info("Updating market metrics...")
            await self._update_all_metrics()

            logger.info("Hybrid indexing completed successfully")

        except Exception as e:
            logger.error(f"Error in hybrid indexing: {e}")
            raise

    async def _enrich_market_metadata(self):
        """
        Enrich market metadata from Gamma API (public, no auth required)

        This updates market questions, descriptions, and other metadata
        """
        try:
            # Get active markets from Gamma API
            markets = await self.clob_client.get_markets(limit=100, active=True)

            logger.info(f"Retrieved {len(markets)} markets from Gamma API")

            enriched_count = 0
            for market in markets:
                try:
                    condition_id = market.get('condition_id') or market.get('conditionId')
                    if not condition_id:
                        continue

                    # Extract metadata
                    metadata = {
                        'question': market.get('question'),
                        'description': market.get('description'),
                        'category': market.get('category'),
                        'end_date': market.get('end_date_iso'),
                        'image_url': market.get('image'),
                        'volume': market.get('volume'),
                        'liquidity': market.get('liquidity'),
                    }

                    # Update condition in database
                    await self.sql_indexer.pool.execute("""
                        UPDATE conditions SET
                            question = COALESCE($1, question),
                            description = COALESCE($2, description),
                            category = COALESCE($3, category),
                            end_date = COALESCE($4::timestamp, end_date),
                            image_url = COALESCE($5, image_url),
                            updated_at = NOW()
                        WHERE condition_id = $6
                    """,
                                                        metadata['question'],
                                                        metadata['description'],
                                                        metadata['category'],
                                                        metadata['end_date'],
                                                        metadata['image_url'],
                                                        condition_id
                                                        )

                    enriched_count += 1

                except Exception as e:
                    logger.warning(f"Failed to enrich market {market.get('question', 'unknown')}: {e}")
                    continue

            logger.info(f"Enriched metadata for {enriched_count} markets")

        except Exception as e:
            logger.error(f"Error enriching market metadata: {e}")

    async def _update_all_metrics(self):
        """Update metrics for all active markets"""
        try:
            # Get all conditions with recent activity
            recent_conditions = await self.sql_indexer.pool.fetch("""
                SELECT DISTINCT 
                    CASE 
                        WHEN pt.condition_id IS NOT NULL THEN pt.condition_id
                        ELSE SUBSTRING(t.token_id FROM 1 FOR 64)
                    END as condition_id
                FROM trades t
                LEFT JOIN position_tokens pt ON t.token_id = pt.position_id
                WHERE t.block_timestamp > NOW() - INTERVAL '24 hours'
                LIMIT 100
            """)

            for row in recent_conditions:
                condition_id = row['condition_id']
                if condition_id:
                    await self.sql_indexer.update_market_metrics(condition_id)

            logger.info(f"Updated metrics for {len(recent_conditions)} markets")

        except Exception as e:
            logger.error(f"Error updating metrics: {e}")


@shared_task(name="hybrid_indexer.run_hybrid_indexer")
def run_hybrid_indexer():
    """Celery task to run the hybrid indexer"""
    logger.info("Starting hybrid Polymarket indexer (Blockchain + CLOB)")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(index_hybrid_data())
        logger.info("Hybrid indexing task completed successfully")
    except Exception as e:
        logger.error(f"Error in hybrid indexing task: {e}")
        raise
    finally:
        loop.close()


async def index_hybrid_data():
    """Main async indexing function"""
    indexer = HybridPolymarketIndexer(settings)

    try:
        await indexer.initialize()
        await indexer.index_all_data()
    finally:
        await indexer.cleanup()


if __name__ == "__main__":
    asyncio.run(index_hybrid_data())