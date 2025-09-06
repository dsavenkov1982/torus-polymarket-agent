# core/tasks/polymarket_sql_indexer.py
import asyncpg
from loguru import logger
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from decimal import Decimal
import json


class PolymarketSQLIndexer:
    def __init__(self, settings):
        """
        Initialize the SQL indexer using settings for PostgreSQL connection.
        """
        self.database_url = settings.DATABASE_URL
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        """
        Create PostgreSQL connection pool.
        """
        try:
            self.pool = await asyncpg.create_pool(
                self.database_url,
                min_size=5,
                max_size=20,
                command_timeout=60
            )
            logger.info("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    async def close(self):
        """
        Close the PostgreSQL connection pool.
        """
        if self.pool:
            await self.pool.close()
            logger.info("PostgreSQL connection pool closed")

    async def insert_block(self, block_data: Dict[str, Any]) -> None:
        """
        Insert or update a blockchain block.

        Args:
            block_data (Dict): Block information from Polygon blockchain.
        """
        async with self.pool.acquire() as conn:
            try:
                await conn.execute("""
                    INSERT INTO blocks (number, hash, timestamp, parent_hash, gas_used, gas_limit)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (number) DO UPDATE SET
                        hash = EXCLUDED.hash,
                        timestamp = EXCLUDED.timestamp,
                        parent_hash = EXCLUDED.parent_hash,
                        gas_used = EXCLUDED.gas_used,
                        gas_limit = EXCLUDED.gas_limit,
                        indexed_at = NOW()
                """,
                                   block_data['number'],
                                   block_data['hash'],
                                   block_data['timestamp'],
                                   block_data['parent_hash'],
                                   block_data.get('gas_used'),
                                   block_data.get('gas_limit')
                                   )
                logger.debug(f"Inserted block {block_data['number']}")
            except Exception as e:
                logger.error(f"Error inserting block {block_data['number']}: {e}")
                raise

    async def insert_condition(self, condition_data: Dict[str, Any]) -> None:
        """
        Insert or update a prediction market condition.

        Args:
            condition_data (Dict): Condition data from ConditionPreparation event.
        """
        async with self.pool.acquire() as conn:
            try:
                # Insert the condition
                await conn.execute("""
                    INSERT INTO conditions (
                        condition_id, oracle, question_id, outcome_slot_count,
                        created_at_block, created_at_tx, created_at,
                        question, description, end_date, category, image_url
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (condition_id) DO UPDATE SET
                        oracle = EXCLUDED.oracle,
                        question_id = EXCLUDED.question_id,
                        outcome_slot_count = EXCLUDED.outcome_slot_count,
                        question = COALESCE(EXCLUDED.question, conditions.question),
                        description = COALESCE(EXCLUDED.description, conditions.description),
                        end_date = COALESCE(EXCLUDED.end_date, conditions.end_date),
                        category = COALESCE(EXCLUDED.category, conditions.category),
                        image_url = COALESCE(EXCLUDED.image_url, conditions.image_url)
                """,
                                   condition_data['condition_id'],
                                   condition_data['oracle'],
                                   condition_data['question_id'],
                                   condition_data['outcome_slot_count'],
                                   condition_data['created_at_block'],
                                   condition_data['created_at_tx'],
                                   condition_data['created_at'],
                                   condition_data.get('question'),
                                   condition_data.get('description'),
                                   condition_data.get('end_date'),
                                   condition_data.get('category'),
                                   condition_data.get('image_url')
                                   )

                # Create position tokens for each outcome
                for outcome_index in range(condition_data['outcome_slot_count']):
                    position_id = f"{condition_data['condition_id']}_{outcome_index}"

                    await conn.execute("""
                        INSERT INTO position_tokens (
                            condition_id, collection_id, position_id, outcome_index
                        ) VALUES ($1, $2, $3, $4)
                        ON CONFLICT (position_id) DO NOTHING
                    """,
                                       condition_data['condition_id'],
                                       condition_data['condition_id'],  # Simplified: collection_id = condition_id
                                       position_id,
                                       outcome_index
                                       )

                logger.info(f"Inserted condition: {condition_data['condition_id'][:10]}...")
            except Exception as e:
                logger.error(f"Error inserting condition {condition_data['condition_id']}: {e}")
                raise

    async def insert_trade(self, trade_data: Dict[str, Any]) -> None:
        """
        Insert a trade from OrderFilled event.

        Args:
            trade_data (Dict): Trade data from blockchain event.
        """
        async with self.pool.acquire() as conn:
            try:
                await conn.execute("""
                    INSERT INTO trades (
                        tx_hash, log_index, block_number, block_timestamp,
                        exchange_address, trader, token_id, collateral_token,
                        token_amount, collateral_amount, price, is_buy, order_id
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    ON CONFLICT (tx_hash, log_index) DO NOTHING
                """,
                                   trade_data['tx_hash'],
                                   trade_data['log_index'],
                                   trade_data['block_number'],
                                   trade_data['block_timestamp'],
                                   trade_data['exchange_address'],
                                   trade_data['trader'],
                                   trade_data['token_id'],
                                   trade_data['collateral_token'],
                                   Decimal(str(trade_data['token_amount'])),
                                   Decimal(str(trade_data['collateral_amount'])),
                                   Decimal(str(trade_data['price'])),
                                   trade_data['is_buy'],
                                   trade_data.get('order_id')
                                   )

                # Update price history
                await self._update_price_history(
                    trade_data['token_id'],
                    trade_data['price'],
                    trade_data['collateral_amount'],
                    trade_data['block_timestamp'],
                    trade_data['block_number']
                )

                logger.debug(f"Inserted trade: {trade_data['tx_hash'][:10]}...")
            except Exception as e:
                logger.error(f"Error inserting trade {trade_data['tx_hash']}: {e}")
                raise

    async def _update_price_history(self, token_id: str, price: float, volume: float,
                                    timestamp: datetime, block_number: int) -> None:
        """
        Update price history for charting.
        """
        try:
            # Extract condition_id from token_id (assuming format: condition_id_outcome_index)
            condition_id = token_id.rsplit('_', 1)[0] if '_' in token_id else token_id
            outcome_index = int(token_id.split('_')[-1]) if '_' in token_id else 0

            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO price_history (
                        condition_id, outcome_index, timestamp, block_number,
                        open_price, high_price, low_price, close_price, volume
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                                   condition_id,
                                   outcome_index,
                                   timestamp,
                                   block_number,
                                   Decimal(str(price)),  # open
                                   Decimal(str(price)),  # high
                                   Decimal(str(price)),  # low
                                   Decimal(str(price)),  # close
                                   Decimal(str(volume))
                                   )
        except Exception as e:
            logger.warning(f"Error updating price history: {e}")

    async def update_balance(self, balance_data: Dict[str, Any]) -> None:
        """
        Update user balance for position tokens.

        Args:
            balance_data (Dict): Balance update data from TransferSingle event.
        """
        async with self.pool.acquire() as conn:
            try:
                await conn.execute("""
                    INSERT INTO balances (
                        user_address, token_id, balance, last_updated_block, 
                        last_updated_tx, last_updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (user_address, token_id) DO UPDATE SET
                        balance = balances.balance + EXCLUDED.balance,
                        last_updated_block = EXCLUDED.last_updated_block,
                        last_updated_tx = EXCLUDED.last_updated_tx,
                        last_updated_at = EXCLUDED.last_updated_at
                """,
                                   balance_data['user_address'],
                                   balance_data['token_id'],
                                   Decimal(str(balance_data['balance_delta'])),
                                   balance_data['block_number'],
                                   balance_data['tx_hash'],
                                   balance_data['timestamp']
                                   )
                logger.debug(f"Updated balance for {balance_data['user_address'][:10]}...")
            except Exception as e:
                logger.error(f"Error updating balance: {e}")
                raise

    async def resolve_condition(self, resolution_data: Dict[str, Any]) -> None:
        """
        Mark a condition as resolved.

        Args:
            resolution_data (Dict): Resolution data from ConditionResolution event.
        """
        async with self.pool.acquire() as conn:
            try:
                await conn.execute("""
                    UPDATE conditions SET
                        resolved = TRUE,
                        resolved_at_block = $1,
                        resolved_at_tx = $2,
                        resolved_at = $3,
                        payout_numerators = $4
                    WHERE condition_id = $5
                """,
                                   resolution_data['block_number'],
                                   resolution_data['tx_hash'],
                                   resolution_data['timestamp'],
                                   resolution_data['payout_numerators'],
                                   resolution_data['condition_id']
                                   )
                logger.info(f"Resolved condition: {resolution_data['condition_id'][:10]}...")
            except Exception as e:
                logger.error(f"Error resolving condition: {e}")
                raise

    async def update_market_metrics(self, condition_id: str) -> None:
        """
        Update aggregated market metrics for a condition.

        Args:
            condition_id (str): The condition ID to update metrics for.
        """
        async with self.pool.acquire() as conn:
            try:
                # Call the PostgreSQL function to refresh metrics
                await conn.execute("SELECT refresh_market_metrics($1)", condition_id)

                # Update price data (yes/no prices for binary markets)
                latest_prices = await conn.fetchrow("""
                    SELECT 
                        pt.outcome_index,
                        AVG(t.price) as avg_price,
                        MAX(t.block_timestamp) as latest_timestamp
                    FROM trades t
                    JOIN position_tokens pt ON t.token_id = pt.position_id
                    WHERE pt.condition_id = $1
                    AND t.block_timestamp > NOW() - INTERVAL '1 hour'
                    GROUP BY pt.outcome_index
                    ORDER BY latest_timestamp DESC
                    LIMIT 1
                """, condition_id)

                if latest_prices:
                    yes_price = float(latest_prices['avg_price']) if latest_prices['outcome_index'] == 0 else 1 - float(
                        latest_prices['avg_price'])
                    no_price = 1.0 - yes_price

                    await conn.execute("""
                        UPDATE market_metrics SET
                            yes_price = $1,
                            no_price = $2,
                            computed_at = NOW()
                        WHERE condition_id = $3
                    """, Decimal(str(yes_price)), Decimal(str(no_price)), condition_id)

                logger.debug(f"Updated metrics for condition: {condition_id[:10]}...")
            except Exception as e:
                logger.error(f"Error updating market metrics for {condition_id}: {e}")
                raise

    async def log_event(self, event_data: Dict[str, Any]) -> None:
        """
        Log blockchain event for debugging and reprocessing.

        Args:
            event_data (Dict): Raw event data from blockchain.
        """
        async with self.pool.acquire() as conn:
            try:
                await conn.execute("""
                    INSERT INTO event_logs (
                        block_number, tx_hash, log_index, contract_address,
                        event_name, event_data, processed
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (tx_hash, log_index) DO UPDATE SET
                        event_data = EXCLUDED.event_data,
                        processed = EXCLUDED.processed
                """,
                                   event_data['block_number'],
                                   event_data['tx_hash'],
                                   event_data['log_index'],
                                   event_data['contract_address'],
                                   event_data['event_name'],
                                   json.dumps(event_data['event_args']),
                                   True
                                   )
            except Exception as e:
                logger.warning(f"Error logging event: {e}")

    async def get_last_processed_block(self, indexer_name: str) -> int:
        """
        Get the last processed block for an indexer.

        Args:
            indexer_name (str): Name of the indexer component.

        Returns:
            int: Last processed block number.
        """
        async with self.pool.acquire() as conn:
            try:
                result = await conn.fetchval("""
                    SELECT last_processed_block 
                    FROM indexer_state 
                    WHERE name = $1
                """, indexer_name)
                return result if result is not None else 50000000  # Default start block
            except Exception as e:
                logger.error(f"Error getting last processed block for {indexer_name}: {e}")
                return 50000000

    async def update_indexer_state(self, indexer_name: str, block_number: int,
                                   events_processed: int = 0) -> None:
        """
        Update indexer processing state.

        Args:
            indexer_name (str): Name of the indexer component.
            block_number (int): Last processed block number.
            events_processed (int): Number of events processed in this batch.
        """
        async with self.pool.acquire() as conn:
            try:
                await conn.execute("""
                    INSERT INTO indexer_state (
                        name, last_processed_block, updated_at, status, total_events_processed
                    ) VALUES ($1, $2, NOW(), 'RUNNING', $3)
                    ON CONFLICT (name) DO UPDATE SET
                        last_processed_block = EXCLUDED.last_processed_block,
                        updated_at = NOW(),
                        status = 'RUNNING',
                        total_events_processed = indexer_state.total_events_processed + EXCLUDED.total_events_processed,
                        error_message = NULL
                """, indexer_name, block_number, events_processed)

                logger.debug(f"Updated indexer state: {indexer_name} -> block {block_number}")
            except Exception as e:
                logger.error(f"Error updating indexer state: {e}")
                raise

    async def mark_indexer_error(self, indexer_name: str, error_message: str) -> None:
        """
        Mark indexer as having an error.

        Args:
            indexer_name (str): Name of the indexer component.
            error_message (str): Error description.
        """
        async with self.pool.acquire() as conn:
            try:
                await conn.execute("""
                    UPDATE indexer_state SET
                        status = 'ERROR',
                        error_message = $1,
                        updated_at = NOW()
                    WHERE name = $2
                """, error_message, indexer_name)
                logger.error(f"Marked indexer {indexer_name} as ERROR: {error_message}")
            except Exception as e:
                logger.error(f"Error marking indexer error: {e}")

    async def get_active_markets(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get active prediction markets with current metrics.

        Args:
            limit (int): Maximum number of markets to return.

        Returns:
            List[Dict]: List of active markets with metrics.
        """
        async with self.pool.acquire() as conn:
            try:
                results = await conn.fetch("""
                    SELECT 
                        c.condition_id,
                        c.question,
                        c.description,
                        c.category,
                        c.end_date,
                        c.resolved,
                        c.created_at,
                        m.volume_24h,
                        m.volume_7d,
                        m.yes_price,
                        m.no_price,
                        m.total_liquidity,
                        m.trade_count_24h,
                        m.unique_traders_24h
                    FROM conditions c
                    LEFT JOIN market_metrics m ON c.condition_id = m.condition_id
                    WHERE c.resolved = FALSE
                    ORDER BY COALESCE(m.volume_24h, 0) DESC
                    LIMIT $1
                """, limit)

                return [dict(row) for row in results]
            except Exception as e:
                logger.error(f"Error getting active markets: {e}")
                return []

    async def get_market_trades(self, condition_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get recent trades for a specific market.

        Args:
            condition_id (str): The condition ID.
            limit (int): Maximum number of trades to return.

        Returns:
            List[Dict]: List of recent trades.
        """
        async with self.pool.acquire() as conn:
            try:
                results = await conn.fetch("""
                    SELECT 
                        t.tx_hash,
                        t.block_timestamp,
                        t.trader,
                        t.token_amount,
                        t.collateral_amount,
                        t.price,
                        t.is_buy,
                        pt.outcome_index
                    FROM trades t
                    JOIN position_tokens pt ON t.token_id = pt.position_id
                    WHERE pt.condition_id = $1
                    ORDER BY t.block_timestamp DESC
                    LIMIT $2
                """, condition_id, limit)

                return [dict(row) for row in results]
            except Exception as e:
                logger.error(f"Error getting market trades: {e}")
                return []

    async def get_indexer_stats(self) -> Dict[str, Any]:
        """
        Get overall indexer statistics.

        Returns:
            Dict: Indexer statistics and health info.
        """
        async with self.pool.acquire() as conn:
            try:
                # Get indexer states
                indexer_states = await conn.fetch("""
                    SELECT name, last_processed_block, status, updated_at, 
                           total_events_processed, error_message
                    FROM indexer_state
                    ORDER BY name
                """)

                # Get database stats
                db_stats = await conn.fetchrow("""
                    SELECT 
                        (SELECT COUNT(*) FROM conditions) as total_conditions,
                        (SELECT COUNT(*) FROM conditions WHERE resolved = FALSE) as active_conditions,
                        (SELECT COUNT(*) FROM trades) as total_trades,
                        (SELECT SUM(volume_24h) FROM market_metrics) as total_volume_24h,
                        (SELECT COUNT(DISTINCT trader) FROM trades WHERE block_timestamp > NOW() - INTERVAL '1 day') as unique_traders_24h,
                        (SELECT MAX(number) FROM blocks) as latest_block
                """)

                return {
                    "indexers": [dict(row) for row in indexer_states],
                    "database": dict(db_stats) if db_stats else {},
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            except Exception as e:
                logger.error(f"Error getting indexer stats: {e}")
                return {"error": str(e)}

    async def cleanup_old_data(self, days_to_keep: int = 30) -> None:
        """
        Clean up old data to manage database size.

        Args:
            days_to_keep (int): Number of days of data to retain.
        """
        async with self.pool.acquire() as conn:
            try:
                cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)

                # Clean old price history (keep only recent for active markets)
                deleted_price_history = await conn.fetchval("""
                    DELETE FROM price_history 
                    WHERE timestamp < $1 
                    AND condition_id NOT IN (
                        SELECT condition_id FROM conditions WHERE resolved = FALSE
                    )
                    RETURNING count(*)
                """, cutoff_date)

                # Clean old event logs
                deleted_event_logs = await conn.fetchval("""
                    DELETE FROM event_logs 
                    WHERE block_number < (
                        SELECT number FROM blocks 
                        WHERE timestamp < $1 
                        ORDER BY number DESC 
                        LIMIT 1
                    )
                    AND processed = TRUE
                    RETURNING count(*)
                """, cutoff_date)

                logger.info(f"Cleanup completed: removed {deleted_price_history} price history records, "
                            f"{deleted_event_logs} event log records older than {days_to_keep} days")

            except Exception as e:
                logger.error(f"Error during cleanup: {e}")

    async def enrich_market_metadata(self, condition_id: str, metadata: Dict[str, Any]) -> None:
        """
        Enrich market with metadata from external sources (APIs, IPFS).

        Args:
            condition_id (str): The condition ID to enrich.
            metadata (Dict): Additional metadata to store.
        """
        async with self.pool.acquire() as conn:
            try:
                await conn.execute("""
                    UPDATE conditions SET
                        question = COALESCE($1, question),
                        description = COALESCE($2, description),
                        category = COALESCE($3, category),
                        end_date = COALESCE($4, end_date),
                        image_url = COALESCE($5, image_url),
                        resolution_source = COALESCE($6, resolution_source)
                    WHERE condition_id = $7
                """,
                                   metadata.get('question'),
                                   metadata.get('description'),
                                   metadata.get('category'),
                                   metadata.get('end_date'),
                                   metadata.get('image_url'),
                                   metadata.get('resolution_source'),
                                   condition_id
                                   )
                logger.debug(f"Enriched metadata for condition: {condition_id[:10]}...")
            except Exception as e:
                logger.warning(f"Error enriching metadata for {condition_id}: {e}")

    async def batch_insert_trades(self, trades_data: List[Dict[str, Any]]) -> None:
        """
        Efficiently insert multiple trades in a single transaction.

        Args:
            trades_data (List[Dict]): List of trade data to insert.
        """
        if not trades_data:
            return

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                try:
                    # Prepare data for batch insert
                    trade_records = []
                    for trade in trades_data:
                        trade_records.append((
                            trade['tx_hash'],
                            trade['log_index'],
                            trade['block_number'],
                            trade['block_timestamp'],
                            trade['exchange_address'],
                            trade['trader'],
                            trade['token_id'],
                            trade['collateral_token'],
                            Decimal(str(trade['token_amount'])),
                            Decimal(str(trade['collateral_amount'])),
                            Decimal(str(trade['price'])),
                            trade['is_buy'],
                            trade.get('order_id')
                        ))

                    # Batch insert trades
                    await conn.executemany("""
                        INSERT INTO trades (
                            tx_hash, log_index, block_number, block_timestamp,
                            exchange_address, trader, token_id, collateral_token,
                            token_amount, collateral_amount, price, is_buy, order_id
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                        ON CONFLICT (tx_hash, log_index) DO NOTHING
                    """, trade_records)

                    logger.info(f"Batch inserted {len(trades_data)} trades")
                except Exception as e:
                    logger.error(f"Error in batch insert trades: {e}")
                    raise