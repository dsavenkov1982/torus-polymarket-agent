# core/tasks/blockchain_indexer.py
import asyncio
from datetime import datetime, timedelta
from celery import shared_task
from web3 import Web3
from web3.middleware import geth_poa_middleware
from loguru import logger
from typing import Dict, List, Any

from core.tasks.polymarket_sql_indexer import PolymarketSQLIndexer
from settings import settings

# Smart contract ABIs
CONDITIONAL_TOKENS_ABI = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "conditionId", "type": "bytes32"},
            {"indexed": True, "name": "oracle", "type": "address"},
            {"indexed": True, "name": "questionId", "type": "bytes32"},
            {"indexed": False, "name": "outcomeSlotCount", "type": "uint256"}
        ],
        "name": "ConditionPreparation",
        "type": "event"
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "conditionId", "type": "bytes32"},
            {"indexed": True, "name": "oracle", "type": "address"},
            {"indexed": True, "name": "questionId", "type": "bytes32"},
            {"indexed": False, "name": "payoutNumerators", "type": "uint256[]"}
        ],
        "name": "ConditionResolution",
        "type": "event"
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "from", "type": "address"},
            {"indexed": True, "name": "to", "type": "address"},
            {"indexed": True, "name": "id", "type": "uint256"},
            {"indexed": False, "name": "value", "type": "uint256"}
        ],
        "name": "TransferSingle",
        "type": "event"
    }
]

CTF_EXCHANGE_ABI = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "maker", "type": "address"},
            {"indexed": True, "name": "taker", "type": "address"},
            {"indexed": False, "name": "tokenId", "type": "uint256"},
            {"indexed": False, "name": "makerAmount", "type": "uint256"},
            {"indexed": False, "name": "takerAmount", "type": "uint256"},
            {"indexed": False, "name": "side", "type": "uint8"}
        ],
        "name": "OrderFilled",
        "type": "event"
    }
]


class PolygonBlockchainIndexer:
    def __init__(self, settings):
        self.settings = settings
        self.sql_indexer = PolymarketSQLIndexer(settings)

        # Initialize Web3
        self.w3 = Web3(Web3.HTTPProvider(settings.POLYGON_RPC_URL))
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)

        # Contract instances
        self.conditional_tokens = self.w3.eth.contract(
            address=settings.CONDITIONAL_TOKENS_ADDRESS,
            abi=CONDITIONAL_TOKENS_ABI
        )
        self.ctf_exchange = self.w3.eth.contract(
            address=settings.CTF_EXCHANGE_ADDRESS,
            abi=CTF_EXCHANGE_ABI
        )

        logger.info(f"Blockchain indexer initialized. Connected: {self.w3.is_connected()}")

    async def initialize(self):
        await self.sql_indexer.connect()

    async def cleanup(self):
        await self.sql_indexer.close()

    async def index_blockchain_data(self) -> None:
        """Main indexing function"""
        try:
            current_block = self.w3.eth.block_number
            logger.info(f"Current Polygon block: {current_block}")

            # Process ConditionalTokens events
            await self._process_conditional_tokens_events(current_block)

            # Process CTF Exchange events
            await self._process_ctf_exchange_events(current_block)

            # Update market metrics for active markets
            await self._update_market_metrics()

            logger.info("Blockchain indexing cycle completed")

        except Exception as e:
            logger.error(f"Error in blockchain indexing: {e}")
            raise

    async def _process_conditional_tokens_events(self, current_block: int) -> None:
        indexer_name = "conditional_tokens"
        last_processed = await self.sql_indexer.get_last_processed_block(indexer_name)

        if last_processed >= current_block:
            logger.debug(f"ConditionalTokens up to date: {last_processed}")
            return

        start_block = last_processed + 1
        end_block = min(start_block + self.settings.BATCH_SIZE - 1, current_block)

        logger.info(f"Processing ConditionalTokens: blocks {start_block}-{end_block}")

        events_processed = 0

        try:
            # Process ConditionPreparation events
            prep_events = self.conditional_tokens.events.ConditionPreparation.get_logs(
                fromBlock=start_block,
                toBlock=end_block
            )

            for event in prep_events:
                await self._handle_condition_preparation(event)
                events_processed += 1

            # Process ConditionResolution events
            resolution_events = self.conditional_tokens.events.ConditionResolution.get_logs(
                fromBlock=start_block,
                toBlock=end_block
            )

            for event in resolution_events:
                await self._handle_condition_resolution(event)
                events_processed += 1

            # Process TransferSingle events
            transfer_events = self.conditional_tokens.events.TransferSingle.get_logs(
                fromBlock=start_block,
                toBlock=end_block
            )

            for event in transfer_events:
                await self._handle_token_transfer(event)
                events_processed += 1

            # Update indexer state
            await self.sql_indexer.update_indexer_state(indexer_name, end_block, events_processed)

            logger.info(f"Processed {events_processed} ConditionalTokens events")

        except Exception as e:
            await self.sql_indexer.mark_indexer_error(indexer_name, str(e))
            raise

    async def _process_ctf_exchange_events(self, current_block: int) -> None:
        indexer_name = "ctf_exchange"
        last_processed = await self.sql_indexer.get_last_processed_block(indexer_name)

        if last_processed >= current_block:
            logger.debug(f"CTF Exchange up to date: {last_processed}")
            return

        start_block = last_processed + 1
        end_block = min(start_block + self.settings.BATCH_SIZE - 1, current_block)

        logger.info(f"Processing CTF Exchange: blocks {start_block}-{end_block}")

        events_processed = 0
        trades_batch = []

        try:
            # Process OrderFilled events
            trade_events = self.ctf_exchange.events.OrderFilled.get_logs(
                fromBlock=start_block,
                toBlock=end_block
            )

            for event in trade_events:
                trade_data = await self._handle_trade_event(event)
                if trade_data:
                    trades_batch.append(trade_data)
                    events_processed += 1

            # Batch insert trades
            if trades_batch:
                await self.sql_indexer.batch_insert_trades(trades_batch)

            # Update indexer state
            await self.sql_indexer.update_indexer_state(indexer_name, end_block, events_processed)

            logger.info(f"Processed {events_processed} CTF Exchange events")

        except Exception as e:
            await self.sql_indexer.mark_indexer_error(indexer_name, str(e))
            raise

    async def _handle_condition_preparation(self, event) -> None:
        """Handle new market creation"""
        try:
            args = event['args']
            block_info = self.w3.eth.get_block(event['blockNumber'])

            condition_data = {
                'condition_id': args['conditionId'].hex(),
                'oracle': args['oracle'],
                'question_id': args['questionId'].hex(),
                'outcome_slot_count': args['outcomeSlotCount'],
                'created_at_block': event['blockNumber'],
                'created_at_tx': event['transactionHash'].hex(),
                'created_at': datetime.fromtimestamp(block_info['timestamp']),
                'question': None,
                'description': None,
                'end_date': None,
                'category': None,
                'image_url': None
            }

            await self.sql_indexer.insert_condition(condition_data)

            await self.sql_indexer.log_event({
                'block_number': event['blockNumber'],
                'tx_hash': event['transactionHash'].hex(),
                'log_index': event['logIndex'],
                'contract_address': event['address'],
                'event_name': 'ConditionPreparation',
                'event_args': dict(args)
            })

            logger.info(f"New market: {condition_data['condition_id'][:10]}...")

        except Exception as e:
            logger.error(f"Error handling ConditionPreparation: {e}")
            raise

    async def _handle_condition_resolution(self, event) -> None:
        """Handle market settlement"""
        try:
            args = event['args']
            block_info = self.w3.eth.get_block(event['blockNumber'])

            resolution_data = {
                'condition_id': args['conditionId'].hex(),
                'block_number': event['blockNumber'],
                'tx_hash': event['transactionHash'].hex(),
                'timestamp': datetime.fromtimestamp(block_info['timestamp']),
                'payout_numerators': list(args['payoutNumerators'])
            }

            await self.sql_indexer.resolve_condition(resolution_data)

            await self.sql_indexer.log_event({
                'block_number': event['blockNumber'],
                'tx_hash': event['transactionHash'].hex(),
                'log_index': event['logIndex'],
                'contract_address': event['address'],
                'event_name': 'ConditionResolution',
                'event_args': dict(args)
            })

            logger.info(f"Market resolved: {resolution_data['condition_id'][:10]}...")

        except Exception as e:
            logger.error(f"Error handling ConditionResolution: {e}")
            raise

    async def _handle_token_transfer(self, event) -> None:
        """Handle position token transfer"""
        try:
            args = event['args']
            block_info = self.w3.eth.get_block(event['blockNumber'])

            # Handle sender balance decrease
            if args['from'] != '0x0000000000000000000000000000000000000000':
                balance_data = {
                    'user_address': args['from'],
                    'token_id': str(args['id']),
                    'balance_delta': -int(args['value']),
                    'block_number': event['blockNumber'],
                    'tx_hash': event['transactionHash'].hex(),
                    'timestamp': datetime.fromtimestamp(block_info['timestamp'])
                }
                await self.sql_indexer.update_balance(balance_data)

            # Handle receiver balance increase
            if args['to'] != '0x0000000000000000000000000000000000000000':
                balance_data = {
                    'user_address': args['to'],
                    'token_id': str(args['id']),
                    'balance_delta': int(args['value']),
                    'block_number': event['blockNumber'],
                    'tx_hash': event['transactionHash'].hex(),
                    'timestamp': datetime.fromtimestamp(block_info['timestamp'])
                }
                await self.sql_indexer.update_balance(balance_data)

        except Exception as e:
            logger.warning(f"Error handling TransferSingle: {e}")

    async def _handle_trade_event(self, event) -> Dict[str, Any]:
        """Handle trade execution"""
        try:
            args = event['args']
            block_info = self.w3.eth.get_block(event['blockNumber'])

            maker_amount = int(args['makerAmount'])
            taker_amount = int(args['takerAmount'])
            price = taker_amount / maker_amount if maker_amount > 0 else 0.5

            trade_data = {
                'tx_hash': event['transactionHash'].hex(),
                'log_index': event['logIndex'],
                'block_number': event['blockNumber'],
                'block_timestamp': datetime.fromtimestamp(block_info['timestamp']),
                'exchange_address': event['address'],
                'trader': args['taker'],
                'token_id': str(args['tokenId']),
                'collateral_token': '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174',  # USDC
                'token_amount': maker_amount,
                'collateral_amount': taker_amount,
                'price': price,
                'is_buy': args['side'] == 0,
                'order_id': None
            }

            await self.sql_indexer.log_event({
                'block_number': event['blockNumber'],
                'tx_hash': event['transactionHash'].hex(),
                'log_index': event['logIndex'],
                'contract_address': event['address'],
                'event_name': 'OrderFilled',
                'event_args': dict(args)
            })

            return trade_data

        except Exception as e:
            logger.error(f"Error handling OrderFilled: {e}")
            return None

    async def _update_market_metrics(self) -> None:
        """Update metrics for recently active markets"""
        try:
            recent_conditions = await self.sql_indexer.pool.fetch("""
                SELECT DISTINCT pt.condition_id
                FROM trades t
                JOIN position_tokens pt ON t.token_id = pt.position_id
                WHERE t.block_timestamp > NOW() - INTERVAL '1 hour'
                LIMIT 50
            """)

            for row in recent_conditions:
                condition_id = row['condition_id']
                await self.sql_indexer.update_market_metrics(condition_id)

            logger.info(f"Updated metrics for {len(recent_conditions)} markets")

        except Exception as e:
            logger.warning(f"Error updating market metrics: {e}")


@shared_task(name="blockchain_indexer.run_polymarket_indexer")
def run_polymarket_indexer():
    """Celery task to run the indexer"""
    logger.info("Starting Polymarket blockchain indexing task")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(index_polymarket_data())
        logger.info("Indexing task completed successfully")
    except Exception as e:
        logger.error(f"Error in indexing task: {e}")
        raise
    finally:
        loop.close()


async def index_polymarket_data():
    """Main async indexing function"""
    indexer = PolygonBlockchainIndexer(settings)

    try:
        await indexer.initialize()
        await indexer.index_blockchain_data()
    finally:
        await indexer.cleanup()


@shared_task(name="blockchain_indexer.enrich_market_metadata")
def enrich_market_metadata():
    """Task to enrich market metadata"""
    logger.info("Starting metadata enrichment task")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(enrich_metadata())
        logger.info("Metadata enrichment completed")
    except Exception as e:
        logger.error(f"Error in metadata enrichment: {e}")
        raise
    finally:
        loop.close()


async def enrich_metadata():
    """Enrich market metadata from external APIs"""
    indexer = PolygonBlockchainIndexer(settings)

    try:
        await indexer.initialize()

        markets_to_enrich = await indexer.sql_indexer.pool.fetch("""
            SELECT condition_id, question_id 
            FROM conditions 
            WHERE question IS NULL 
            AND created_at > NOW() - INTERVAL '30 days'
            LIMIT 100
        """)

        for market in markets_to_enrich:
            try:
                # TODO: Implement actual metadata fetching from Polymarket API
                metadata = {
                    'question': f"Market question for {market['condition_id'][:10]}",
                    'description': "Market description",
                    'category': "General",
                    'end_date': datetime.now() + timedelta(days=30),
                    'image_url': None,
                    'resolution_source': "TBD"
                }

                await indexer.sql_indexer.pool.execute("""
                    UPDATE conditions SET
                        question = $1,
                        description = $2,
                        category = $3,
                        end_date = $4,
                        image_url = $5,
                        resolution_source = $6
                    WHERE condition_id = $7
                """, metadata['question'], metadata['description'],
                                                       metadata['category'], metadata['end_date'],
                                                       metadata['image_url'], metadata['resolution_source'],
                                                       market['condition_id'])

            except Exception as e:
                logger.warning(f"Failed to enrich {market['condition_id']}: {e}")
                continue

        logger.info(f"Enriched metadata for {len(markets_to_enrich)} markets")

    finally:
        await indexer.cleanup()


@shared_task(name="blockchain_indexer.database_maintenance")
def database_maintenance():
    """Database maintenance and cleanup task"""
    logger.info("Starting database maintenance")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(run_maintenance())
        logger.info("Database maintenance completed")
    except Exception as e:
        logger.error(f"Error in maintenance: {e}")
        raise
    finally:
        loop.close()


async def run_maintenance():
    """Run database maintenance"""
    indexer = PolygonBlockchainIndexer(settings)

    try:
        await indexer.initialize()

        # Update metrics for all active markets
        active_markets = await indexer.sql_indexer.get_active_markets(limit=1000)

        for market in active_markets:
            await indexer.sql_indexer.update_market_metrics(market['condition_id'])

        logger.info(f"Refreshed metrics for {len(active_markets)} active markets")

        # Clean up old data
        cutoff_date = datetime.now() - timedelta(days=90)
        await indexer.sql_indexer.pool.execute("""
            DELETE FROM price_history 
            WHERE timestamp < $1 
            AND condition_id NOT IN (
                SELECT condition_id FROM conditions WHERE resolved = FALSE
            )
        """, cutoff_date)

        logger.info("Cleaned up old price history data")

    finally:
        await indexer.cleanup()


if __name__ == "__main__":
    asyncio.run(index_polymarket_data())