# core/tasks/polymarket_sql_indexer.py
import asyncpg
from loguru import logger
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import json


class PolymarketSQLIndexer:
    def __init__(self, settings):
        self.database_url = settings.DATABASE_URL
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
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
        if self.pool:
            await self.pool.close()
            logger.info("PostgreSQL connection pool closed")

    async def insert_block(self, block_data: Dict[str, Any]) -> None:
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
                """, block_data['number'], block_data['hash'], block_data['timestamp'],
                                   block_data['parent_hash'], block_data.get('gas_used'), block_data.get('gas_limit'))
                logger.debug(f"Inserted block {block_data['number']}")
            except Exception as e:
                logger.error(f"Error inserting block {block_data['number']}: {e}")
                raise

    async def insert_condition(self, condition_data: Dict[str, Any]) -> None:
        async with self.pool.acquire() as conn:
            try:
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
                """, condition_data['condition_id'], condition_data['oracle'],
                                   condition_data['question_id'], condition_data['outcome_slot_count'],
                                   condition_data['created_at_block'], condition_data['created_at_tx'],
                                   condition_data['created_at'], condition_data.get('question'),
                                   condition_data.get('description'), condition_data.get('end_date'),
                                   condition_data.get('category'), condition_data.get('image_url'))

                for outcome_index in range(condition_data['outcome_slot_count']):
                    position_id = f"{condition_data['condition_id']}_{outcome_index}"
                    await conn.execute("""
                        INSERT INTO position_tokens (
                            condition_id, collection_id, position_id, outcome_index
                        ) VALUES ($1, $2, $3, $4)
                        ON CONFLICT (position_id) DO NOTHING
                    """, condition_data['condition_id'], condition_data['condition_id'],
                                       position_id, outcome_index)

                logger.info(f"Inserted condition: {condition_data['condition_id'][:10]}...")
            except Exception as e:
                logger.error(f"Error inserting condition {condition_data['condition_id']}: {e}")
                raise

    async def insert_trade(self, trade_data: Dict[str, Any]) -> None:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                try:
                    # Insert trade
                    await conn.execute("""
                        INSERT INTO trades (
                            tx_hash, log_index, block_number, block_timestamp,
                            exchange_address, trader, token_id, collateral_token,
                            token_amount, collateral_amount, price, is_buy, order_id
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                        ON CONFLICT (tx_hash, log_index) DO NOTHING
                    """, trade_data['tx_hash'], trade_data['log_index'], trade_data['block_number'],
                                       trade_data['block_timestamp'], trade_data['exchange_address'],
                                       trade_data['trader'], trade_data['token_id'], trade_data['collateral_token'],
                                       Decimal(str(trade_data['token_amount'])),
                                       Decimal(str(trade_data['collateral_amount'])),
                                       Decimal(str(trade_data['price'])), trade_data['is_buy'],
                                       trade_data.get('order_id'))

                    # Update user position
                    await self._update_user_position(conn, trade_data)

                    # Update user stats
                    await self._update_user_stats(conn, trade_data)

                    # Update price history
                    await self._update_price_history(
                        trade_data['token_id'], trade_data['price'],
                        trade_data['collateral_amount'], trade_data['block_timestamp'],
                        trade_data['block_number']
                    )

                    logger.debug(f"Inserted trade: {trade_data['tx_hash'][:10]}...")
                except Exception as e:
                    logger.error(f"Error inserting trade {trade_data['tx_hash']}: {e}")
                    raise

    async def _update_user_position(self, conn, trade_data: Dict[str, Any]) -> None:
        """Update user position tracking for PnL calculations"""
        try:
            # Get condition_id and outcome_index from token_id
            token_result = await conn.fetchrow("""
                SELECT condition_id, outcome_index 
                FROM position_tokens 
                WHERE position_id = $1
            """, trade_data['token_id'])

            if not token_result:
                logger.warning(f"Position token not found: {trade_data['token_id']}")
                return

            condition_id = token_result['condition_id']
            outcome_index = token_result['outcome_index']

            trader = trade_data['trader']
            token_amount = Decimal(str(trade_data['token_amount']))
            collateral_amount = Decimal(str(trade_data['collateral_amount']))
            price = Decimal(str(trade_data['price']))
            is_buy = trade_data['is_buy']

            # Get current position
            position = await conn.fetchrow("""
                SELECT * FROM user_market_positions
                WHERE user_address = $1 AND condition_id = $2 AND outcome_index = $3
            """, trader, condition_id, outcome_index)

            if is_buy:
                # Buying shares
                new_total_bought = (position['total_shares_bought'] if position else Decimal(0)) + token_amount
                new_cost_basis = (position['total_cost_basis'] if position else Decimal(0)) + collateral_amount
                new_avg_buy_price = new_cost_basis / new_total_bought if new_total_bought > 0 else price

                await conn.execute("""
                    INSERT INTO user_market_positions (
                        user_address, condition_id, outcome_index,
                        total_shares_bought, current_shares, total_cost_basis,
                        average_buy_price, first_trade_at, last_trade_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (user_address, condition_id, outcome_index) DO UPDATE SET
                        total_shares_bought = user_market_positions.total_shares_bought + EXCLUDED.total_shares_bought,
                        current_shares = user_market_positions.current_shares + EXCLUDED.current_shares,
                        total_cost_basis = user_market_positions.total_cost_basis + EXCLUDED.total_cost_basis,
                        average_buy_price = EXCLUDED.average_buy_price,
                        last_trade_at = EXCLUDED.last_trade_at,
                        last_updated_at = NOW()
                """, trader, condition_id, outcome_index, token_amount, token_amount,
                                   collateral_amount, new_avg_buy_price, trade_data['block_timestamp'],
                                   trade_data['block_timestamp'])

            else:
                # Selling shares
                if not position:
                    logger.warning(f"User {trader} selling without position in {condition_id}")
                    return

                new_total_sold = position['total_shares_sold'] + token_amount
                new_proceeds = position['total_proceeds'] + collateral_amount

                # Calculate realized PnL for this sale
                if position['average_buy_price']:
                    cost_of_sold_shares = position['average_buy_price'] * token_amount
                    realized_pnl_delta = collateral_amount - cost_of_sold_shares
                    new_realized_pnl = position['realized_pnl'] + realized_pnl_delta
                else:
                    new_realized_pnl = position['realized_pnl']

                await conn.execute("""
                    UPDATE user_market_positions SET
                        total_shares_sold = total_shares_sold + $1,
                        current_shares = current_shares - $1,
                        total_proceeds = total_proceeds + $2,
                        realized_pnl = $3,
                        last_trade_at = $4,
                        last_updated_at = NOW()
                    WHERE user_address = $5 AND condition_id = $6 AND outcome_index = $7
                """, token_amount, collateral_amount, new_realized_pnl,
                                   trade_data['block_timestamp'], trader, condition_id, outcome_index)

        except Exception as e:
            logger.error(f"Error updating user position: {e}")

    async def _update_user_stats(self, conn, trade_data: Dict[str, Any]) -> None:
        """Update aggregate user statistics"""
        try:
            trader = trade_data['trader']
            collateral_amount = Decimal(str(trade_data['collateral_amount']))

            await conn.execute("""
                INSERT INTO user_stats (
                    user_address, total_volume, total_trades, 
                    first_trade_at, last_trade_at
                ) VALUES ($1, $2, 1, $3, $4)
                ON CONFLICT (user_address) DO UPDATE SET
                    total_volume = user_stats.total_volume + EXCLUDED.total_volume,
                    total_trades = user_stats.total_trades + 1,
                    last_trade_at = EXCLUDED.last_trade_at,
                    last_updated_at = NOW()
            """, trader, collateral_amount, trade_data['block_timestamp'],
                               trade_data['block_timestamp'])

        except Exception as e:
            logger.error(f"Error updating user stats: {e}")

    async def _update_price_history(self, token_id: str, price: float, volume: float,
                                    timestamp: datetime, block_number: int) -> None:
        """Update price history for charting"""
        try:
            condition_id = token_id.rsplit('_', 1)[0] if '_' in token_id else token_id
            outcome_index = int(token_id.split('_')[-1]) if '_' in token_id else 0

            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO price_history (
                        condition_id, outcome_index, timestamp, block_number,
                        open_price, high_price, low_price, close_price, volume, trade_count
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 1)
                """, condition_id, outcome_index, timestamp, block_number,
                                   Decimal(str(price)), Decimal(str(price)), Decimal(str(price)),
                                   Decimal(str(price)), Decimal(str(volume)))
        except Exception as e:
            logger.warning(f"Error updating price history: {e}")

    async def update_balance(self, balance_data: Dict[str, Any]) -> None:
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
                """, balance_data['user_address'], balance_data['token_id'],
                                   Decimal(str(balance_data['balance_delta'])), balance_data['block_number'],
                                   balance_data['tx_hash'], balance_data['timestamp'])
                logger.debug(f"Updated balance for {balance_data['user_address'][:10]}...")
            except Exception as e:
                logger.error(f"Error updating balance: {e}")
                raise

    async def resolve_condition(self, resolution_data: Dict[str, Any]) -> None:
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
                """, resolution_data['block_number'], resolution_data['tx_hash'],
                                   resolution_data['timestamp'], resolution_data['payout_numerators'],
                                   resolution_data['condition_id'])
                logger.info(f"Resolved condition: {resolution_data['condition_id'][:10]}...")
            except Exception as e:
                logger.error(f"Error resolving condition: {e}")
                raise

    async def update_market_metrics(self, condition_id: str) -> None:
        """Update comprehensive market metrics"""
        async with self.pool.acquire() as conn:
            try:
                # Call PostgreSQL function for basic metrics
                await conn.execute("SELECT refresh_market_metrics($1)", condition_id)

                # Get latest trades for price analysis
                recent_trades = await conn.fetch("""
                    SELECT price, block_timestamp, collateral_amount
                    FROM trades t
                    JOIN position_tokens pt ON t.token_id = pt.position_id
                    WHERE pt.condition_id = $1 AND pt.outcome_index = 0
                    ORDER BY block_timestamp DESC
                    LIMIT 100
                """, condition_id)

                if not recent_trades:
                    return

                # Calculate price changes
                current_price = float(recent_trades[0]['price'])

                trades_12h = [t for t in recent_trades if
                              t['block_timestamp'] > datetime.now(timezone.utc) - timedelta(hours=12)]
                trades_24h = [t for t in recent_trades if
                              t['block_timestamp'] > datetime.now(timezone.utc) - timedelta(hours=24)]

                price_12h_ago = float(trades_12h[-1]['price']) if trades_12h else current_price
                price_24h_ago = float(trades_24h[-1]['price']) if trades_24h else current_price

                price_12h_change = ((current_price - price_12h_ago) / price_12h_ago * 100) if price_12h_ago > 0 else 0
                price_24h_change = ((current_price - price_24h_ago) / price_24h_ago * 100) if price_24h_ago > 0 else 0

                # Calculate advanced metrics
                metrics = await self._calculate_advanced_metrics(condition_id, recent_trades)

                # Update market metrics with calculated values
                await conn.execute("""
                    UPDATE market_metrics SET
                        yes_price = $1,
                        no_price = $2,
                        yes_price_12h_ago = $3,
                        yes_price_24h_ago = $4,
                        price_12h_change_pct = $5,
                        price_24h_change_pct = $6,
                        price_momentum = $7,
                        volume_momentum = $8,
                        turnover_ratio = $9,
                        adjusted_volatility = $10,
                        computed_at = NOW()
                    WHERE condition_id = $11
                """, Decimal(str(current_price)), Decimal(str(1 - current_price)),
                                   Decimal(str(price_12h_ago)), Decimal(str(price_24h_ago)),
                                   Decimal(str(price_12h_change)), Decimal(str(price_24h_change)),
                                   Decimal(str(metrics['price_momentum'])), Decimal(str(metrics['volume_momentum'])),
                                   Decimal(str(metrics['turnover_ratio'])), Decimal(str(metrics['volatility'])),
                                   condition_id)

                logger.debug(f"Updated metrics for condition: {condition_id[:10]}...")
            except Exception as e:
                logger.error(f"Error updating market metrics for {condition_id}: {e}")
                raise

    async def _calculate_advanced_metrics(self, condition_id: str, trades: List) -> Dict[str, float]:
        """Calculate advanced metrics like Polysights"""
        try:
            if not trades:
                return {'price_momentum': 0, 'volume_momentum': 0, 'turnover_ratio': 0, 'volatility': 0}

            # Price momentum (24h price change rate)
            first_price = float(trades[-1]['price']) if len(trades) > 1 else float(trades[0]['price'])
            last_price = float(trades[0]['price'])
            price_momentum = (last_price - first_price) / first_price if first_price > 0 else 0

            # Volume momentum (compare recent half vs older half)
            mid = len(trades) // 2
            recent_vol = sum(float(t['collateral_amount']) for t in trades[:mid])
            older_vol = sum(float(t['collateral_amount']) for t in trades[mid:])
            volume_momentum = (recent_vol - older_vol) / older_vol if older_vol > 0 else 0

            # Get liquidity for turnover ratio
            async with self.pool.acquire() as conn:
                metrics = await conn.fetchrow("""
                    SELECT volume_24h, total_liquidity, open_interest 
                    FROM market_metrics WHERE condition_id = $1
                """, condition_id)

            turnover_ratio = 0
            if metrics and metrics['total_liquidity']:
                liq = float(metrics['total_liquidity'])
                vol = float(metrics['volume_24h'] or 0)
                turnover_ratio = vol / liq if liq > 0 else 0

            # Price volatility (standard deviation of prices)
            prices = [float(t['price']) for t in trades]
            if len(prices) > 1:
                mean_price = sum(prices) / len(prices)
                variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
                volatility = variance ** 0.5
            else:
                volatility = 0

            return {
                'price_momentum': price_momentum,
                'volume_momentum': volume_momentum,
                'turnover_ratio': turnover_ratio,
                'volatility': volatility
            }
        except Exception as e:
            logger.error(f"Error calculating advanced metrics: {e}")
            return {'price_momentum': 0, 'volume_momentum': 0, 'turnover_ratio': 0, 'volatility': 0}

    async def calculate_user_pnl(self, user_address: str, condition_id: str) -> Dict[str, Any]:
        """Calculate realized and unrealized PnL for a user in a market"""
        async with self.pool.acquire() as conn:
            try:
                position = await conn.fetchrow("""
                    SELECT * FROM user_market_positions
                    WHERE user_address = $1 AND condition_id = $2
                """, user_address, condition_id)

                if not position:
                    return {'realized_pnl': 0, 'unrealized_pnl': 0, 'total_pnl': 0}

                # Get current market price
                current_price = await conn.fetchval("""
                    SELECT yes_price FROM market_metrics WHERE condition_id = $1
                """, condition_id)

                current_shares = float(position['current_shares'])
                avg_buy_price = float(position['average_buy_price'] or 0)
                realized_pnl = float(position['realized_pnl'])

                # Calculate unrealized PnL
                if current_price and current_shares > 0:
                    current_value = current_shares * float(current_price)
                    cost_basis = current_shares * avg_buy_price
                    unrealized_pnl = current_value - cost_basis
                else:
                    unrealized_pnl = 0

                return {
                    'realized_pnl': realized_pnl,
                    'unrealized_pnl': unrealized_pnl,
                    'total_pnl': realized_pnl + unrealized_pnl,
                    'current_shares': current_shares,
                    'average_buy_price': avg_buy_price
                }
            except Exception as e:
                logger.error(f"Error calculating user PnL: {e}")
                return {'realized_pnl': 0, 'unrealized_pnl': 0, 'total_pnl': 0}

    async def get_top_positions(self, condition_id: str, limit: int = 10) -> Dict[str, List]:
        """Get top Yes and No positions for a market"""
        async with self.pool.acquire() as conn:
            try:
                # Top Yes positions (outcome_index = 0)
                yes_positions = await conn.fetch("""
                    SELECT 
                        ump.user_address,
                        ump.current_shares,
                        ump.average_buy_price,
                        ump.total_cost_basis,
                        ump.realized_pnl,
                        ump.unrealized_pnl,
                        (ump.current_shares * mm.yes_price) as current_value
                    FROM user_market_positions ump
                    JOIN market_metrics mm ON ump.condition_id = mm.condition_id
                    WHERE ump.condition_id = $1 AND ump.outcome_index = 0
                    AND ump.current_shares > 0
                    ORDER BY current_value DESC
                    LIMIT $2
                """, condition_id, limit)

                # Top No positions (outcome_index = 1)
                no_positions = await conn.fetch("""
                    SELECT 
                        ump.user_address,
                        ump.current_shares,
                        ump.average_buy_price,
                        ump.total_cost_basis,
                        ump.realized_pnl,
                        ump.unrealized_pnl,
                        (ump.current_shares * mm.no_price) as current_value
                    FROM user_market_positions ump
                    JOIN market_metrics mm ON ump.condition_id = mm.condition_id
                    WHERE ump.condition_id = $1 AND ump.outcome_index = 1
                    AND ump.current_shares > 0
                    ORDER BY current_value DESC
                    LIMIT $2
                """, condition_id, limit)

                return {
                    'yes_positions': [dict(row) for row in yes_positions],
                    'no_positions': [dict(row) for row in no_positions]
                }
            except Exception as e:
                logger.error(f"Error getting top positions: {e}")
                return {'yes_positions': [], 'no_positions': []}

    async def get_user_aggregate_stats(self, user_address: str) -> Dict[str, Any]:
        """Get Predictfolio-style aggregate user statistics"""
        async with self.pool.acquire() as conn:
            try:
                # Get user stats
                stats = await conn.fetchrow("""
                    SELECT * FROM user_stats WHERE user_address = $1
                """, user_address)

                if not stats:
                    return {}

                # Calculate total unrealized PnL across all positions
                positions = await conn.fetch("""
                    SELECT 
                        ump.condition_id,
                        ump.current_shares,
                        ump.average_buy_price,
                        ump.outcome_index,
                        CASE 
                            WHEN ump.outcome_index = 0 THEN mm.yes_price
                            ELSE mm.no_price
                        END as current_price
                    FROM user_market_positions ump
                    JOIN market_metrics mm ON ump.condition_id = mm.condition_id
                    WHERE ump.user_address = $1 AND ump.current_shares > 0
                """, user_address)

                total_unrealized = sum(
                    (float(p['current_shares']) * float(p['current_price']) -
                     float(p['current_shares']) * float(p['average_buy_price']))
                    for p in positions if p['current_price']
                )

                portfolio_value = sum(
                    float(p['current_shares']) * float(p['current_price'])
                    for p in positions if p['current_price']
                )

                return {
                    **dict(stats),
                    'total_unrealized_pnl': total_unrealized,
                    'portfolio_value': portfolio_value,
                    'active_positions': len(positions)
                }
            except Exception as e:
                logger.error(f"Error getting user aggregate stats: {e}")
                return {}

    async def log_event(self, event_data: Dict[str, Any]) -> None:
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
                """, event_data['block_number'], event_data['tx_hash'],
                                   event_data['log_index'], event_data['contract_address'],
                                   event_data['event_name'], json.dumps(event_data['event_args']), True)
            except Exception as e:
                logger.warning(f"Error logging event: {e}")

    async def get_last_processed_block(self, indexer_name: str) -> int:
        async with self.pool.acquire() as conn:
            try:
                result = await conn.fetchval("""
                    SELECT last_processed_block FROM indexer_state WHERE name = $1
                """, indexer_name)
                return result if result is not None else 50000000
            except Exception as e:
                logger.error(f"Error getting last processed block for {indexer_name}: {e}")
                return 50000000

    async def update_indexer_state(self, indexer_name: str, block_number: int,
                                   events_processed: int = 0) -> None:
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
        async with self.pool.acquire() as conn:
            try:
                results = await conn.fetch("""
                    SELECT 
                        c.condition_id, c.question, c.description, c.category,
                        c.end_date, c.resolved, c.created_at,
                        m.volume_1h, m.volume_4h, m.volume_12h, m.volume_24h,
                        m.yes_price, m.no_price,
                        m.total_liquidity, m.open_interest,
                        m.trade_count_24h, m.unique_traders_24h,
                        m.price_12h_change_pct, m.price_24h_change_pct,
                        m.turnover_ratio, m.price_momentum
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
        async with self.pool.acquire() as conn:
            try:
                results = await conn.fetch("""
                    SELECT 
                        t.tx_hash, t.block_timestamp, t.trader,
                        t.token_amount, t.collateral_amount, t.price, t.is_buy,
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

    async def batch_insert_trades(self, trades_data: List[Dict[str, Any]]) -> None:
        if not trades_data:
            return

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                try:
                    for trade in trades_data:
                        await self.insert_trade(trade)
                    logger.info(f"Batch inserted {len(trades_data)} trades")
                except Exception as e:
                    logger.error(f"Error in batch insert trades: {e}")
                    raise

    async def get_indexer_stats(self) -> Dict[str, Any]:
        async with self.pool.acquire() as conn:
            try:
                indexer_states = await conn.fetch("""
                    SELECT name, last_processed_block, status, updated_at, 
                           total_events_processed, error_message
                    FROM indexer_state ORDER BY name
                """)

                db_stats = await conn.fetchrow("""
                    SELECT 
                        (SELECT COUNT(*) FROM conditions) as total_conditions,
                        (SELECT COUNT(*) FROM conditions WHERE resolved = FALSE) as active_conditions,
                        (SELECT COUNT(*) FROM trades) as total_trades,
                        (SELECT SUM(volume_24h) FROM market_metrics) as total_volume_24h,
                        (SELECT COUNT(DISTINCT trader) FROM trades 
                         WHERE block_timestamp > NOW() - INTERVAL '1 day') as unique_traders_24h,
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