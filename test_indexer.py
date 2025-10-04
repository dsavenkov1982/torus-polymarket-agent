#!/usr/bin/env python3
# test_indexer.py - Test script for Polymarket indexer

import asyncio
import sys
from decimal import Decimal
from datetime import datetime, timedelta
from loguru import logger
from core.tasks.polymarket_sql_indexer import PolymarketSQLIndexer
from core.tasks.blockchain_indexer import PolygonBlockchainIndexer
from settings import settings

logger.remove()
logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>")


async def test_database_connection():
    """Test 1: Database connection"""
    logger.info("TEST 1: Database Connection")
    logger.info("-" * 50)

    indexer = PolymarketSQLIndexer(settings)

    try:
        await indexer.connect()
        logger.success("✓ Connected to PostgreSQL")

        # Test query
        result = await indexer.pool.fetchval("SELECT 1")
        assert result == 1
        logger.success("✓ Database query works")

        await indexer.close()
        logger.success("✓ Connection closed properly")
        return True

    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        return False


async def test_schema_exists():
    """Test 2: Verify schema tables exist"""
    logger.info("\nTEST 2: Schema Verification")
    logger.info("-" * 50)

    indexer = PolymarketSQLIndexer(settings)

    try:
        await indexer.connect()

        required_tables = [
            'blocks', 'conditions', 'position_tokens', 'trades',
            'balances', 'market_metrics', 'user_market_positions',
            'user_stats', 'order_book_snapshots', 'price_history',
            'indexer_state', 'event_logs'
        ]

        for table in required_tables:
            result = await indexer.pool.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = $1
                )
            """, table)

            if result:
                logger.success(f"✓ Table '{table}' exists")
            else:
                logger.error(f"✗ Table '{table}' missing")
                return False

        await indexer.close()
        return True

    except Exception as e:
        logger.error(f"✗ Schema verification failed: {e}")
        return False


async def test_insert_mock_data():
    """Test 3: Insert mock data"""
    logger.info("\nTEST 3: Insert Mock Data")
    logger.info("-" * 50)

    indexer = PolymarketSQLIndexer(settings)

    try:
        await indexer.connect()

        # Mock block data
        block_data = {
            'number': 999999999,
            'hash': '0x' + 'a' * 64,
            'timestamp': datetime.now(),
            'parent_hash': '0x' + 'b' * 64,
            'gas_used': 100000,
            'gas_limit': 200000
        }

        await indexer.insert_block(block_data)
        logger.success("✓ Inserted mock block")

        # Mock condition data
        condition_data = {
            'condition_id': '0x' + 'c' * 64,
            'oracle': '0x' + '1' * 40,
            'question_id': '0x' + 'd' * 64,
            'outcome_slot_count': 2,
            'created_at_block': 999999999,
            'created_at_tx': '0x' + 'e' * 64,
            'created_at': datetime.now(),
            'question': 'Test market question?',
            'description': 'Test market description',
            'end_date': datetime.now() + timedelta(days=30),
            'category': 'Politics',
            'image_url': None
        }

        await indexer.insert_condition(condition_data)
        logger.success("✓ Inserted mock condition")

        # Mock trade data
        trade_data = {
            'tx_hash': '0x' + 'f' * 64,
            'log_index': 0,
            'block_number': 999999999,
            'block_timestamp': datetime.now(),
            'exchange_address': '0x' + '2' * 40,
            'trader': '0x' + '3' * 40,
            'token_id': '0x' + 'c' * 64 + '_0',
            'collateral_token': '0x' + '4' * 40,
            'token_amount': 100.0,
            'collateral_amount': 50.0,
            'price': 0.5,
            'is_buy': True,
            'order_id': None
        }

        await indexer.insert_trade(trade_data)
        logger.success("✓ Inserted mock trade")

        # Verify data was inserted
        trade_count = await indexer.pool.fetchval("""
            SELECT COUNT(*) FROM trades WHERE tx_hash = $1
        """, trade_data['tx_hash'])

        assert trade_count == 1
        logger.success("✓ Mock trade verified in database")

        await indexer.close()
        return True

    except Exception as e:
        logger.error(f"✗ Mock data insertion failed: {e}")
        return False


async def test_market_metrics_calculation():
    """Test 4: Market metrics calculation"""
    logger.info("\nTEST 4: Market Metrics Calculation")
    logger.info("-" * 50)

    indexer = PolymarketSQLIndexer(settings)

    try:
        await indexer.connect()

        # Get or create a test condition
        condition_id = '0x' + 'c' * 64

        # Update metrics
        await indexer.update_market_metrics(condition_id)
        logger.success("✓ Market metrics calculated")

        # Verify metrics exist
        metrics = await indexer.pool.fetchrow("""
            SELECT * FROM market_metrics WHERE condition_id = $1
        """, condition_id)

        if metrics:
            logger.success("✓ Market metrics saved to database")
            logger.info(f"  - Volume 24h: {metrics['volume_24h']}")
            logger.info(f"  - Open Interest: {metrics['open_interest']}")
            logger.info(f"  - Yes Price: {metrics['yes_price']}")
        else:
            logger.warning("⚠ No metrics found (might be expected for new market)")

        await indexer.close()
        return True

    except Exception as e:
        logger.error(f"✗ Metrics calculation failed: {e}")
        return False


async def test_user_position_tracking():
    """Test 5: User position and PnL tracking"""
    logger.info("\nTEST 5: User Position Tracking")
    logger.info("-" * 50)

    indexer = PolymarketSQLIndexer(settings)

    try:
        await indexer.connect()

        user_address = '0x' + '3' * 40
        condition_id = '0x' + 'c' * 64

        # Check if position was created from trade
        position = await indexer.pool.fetchrow("""
            SELECT * FROM user_market_positions 
            WHERE user_address = $1 AND condition_id = $2
        """, user_address, condition_id)

        if position:
            logger.success("✓ User position tracked")
            logger.info(f"  - Current shares: {position['current_shares']}")
            logger.info(f"  - Average buy price: {position['average_buy_price']}")
            logger.info(f"  - Realized PnL: {position['realized_pnl']}")
        else:
            logger.warning("⚠ No position found (might be expected)")

        # Check user stats
        stats = await indexer.pool.fetchrow("""
            SELECT * FROM user_stats WHERE user_address = $1
        """, user_address)

        if stats:
            logger.success("✓ User stats tracked")
            logger.info(f"  - Total volume: {stats['total_volume']}")
            logger.info(f"  - Total trades: {stats['total_trades']}")
        else:
            logger.warning("⚠ No user stats found")

        await indexer.close()
        return True

    except Exception as e:
        logger.error(f"✗ Position tracking test failed: {e}")
        return False


async def test_blockchain_connection():
    """Test 6: Blockchain connection"""
    logger.info("\nTEST 6: Blockchain Connection")
    logger.info("-" * 50)

    try:
        indexer = PolygonBlockchainIndexer(settings)

        if indexer.w3.is_connected():
            logger.success("✓ Connected to Polygon RPC")

            block_number = indexer.w3.eth.block_number
            logger.success(f"✓ Current block: {block_number}")

            # Test contract connection
            logger.info(f"  - ConditionalTokens: {indexer.conditional_tokens.address}")
            logger.info(f"  - CTFExchange: {indexer.ctf_exchange.address}")

            return True
        else:
            logger.error("✗ Not connected to Polygon RPC")
            return False

    except Exception as e:
        logger.error(f"✗ Blockchain connection failed: {e}")
        return False


async def test_indexer_state():
    """Test 7: Indexer state management"""
    logger.info("\nTEST 7: Indexer State Management")
    logger.info("-" * 50)

    indexer = PolymarketSQLIndexer(settings)

    try:
        await indexer.connect()

        # Check indexer state
        states = await indexer.pool.fetch("""
            SELECT name, last_processed_block, status 
            FROM indexer_state
        """)

        for state in states:
            logger.success(f"✓ Indexer '{state['name']}' at block {state['last_processed_block']}")
            logger.info(f"  Status: {state['status']}")

        if not states:
            logger.warning("⚠ No indexer states found - will be created on first run")

        await indexer.close()
        return True

    except Exception as e:
        logger.error(f"✗ Indexer state test failed: {e}")
        return False


async def test_get_active_markets():
    """Test 8: Get active markets"""
    logger.info("\nTEST 8: Get Active Markets")
    logger.info("-" * 50)

    indexer = PolymarketSQLIndexer(settings)

    try:
        await indexer.connect()

        markets = await indexer.get_active_markets(limit=5)

        if markets:
            logger.success(f"✓ Retrieved {len(markets)} active markets")

            for i, market in enumerate(markets[:3], 1):
                logger.info(f"\n  Market {i}:")
                logger.info(f"    Question: {market['question'] or 'N/A'}")
                logger.info(f"    Category: {market['category'] or 'N/A'}")
                logger.info(f"    Volume 24h: ${market['volume_24h'] or 0:,.2f}")
                logger.info(f"    Yes Price: {market['yes_price'] or 'N/A'}")
        else:
            logger.warning("⚠ No active markets found (database might be empty)")

        await indexer.close()
        return True

    except Exception as e:
        logger.error(f"✗ Get active markets failed: {e}")
        return False


async def cleanup_test_data():
    """Clean up test data"""
    logger.info("\nCleaning up test data...")

    indexer = PolymarketSQLIndexer(settings)

    try:
        await indexer.connect()

        # Delete test data
        await indexer.pool.execute("DELETE FROM trades WHERE block_number = 999999999")
        await indexer.pool.execute("DELETE FROM user_market_positions WHERE condition_id = $1", '0x' + 'c' * 64)
        await indexer.pool.execute("DELETE FROM user_stats WHERE user_address = $1", '0x' + '3' * 40)
        await indexer.pool.execute("DELETE FROM market_metrics WHERE condition_id = $1", '0x' + 'c' * 64)
        await indexer.pool.execute("DELETE FROM position_tokens WHERE condition_id = $1", '0x' + 'c' * 64)
        await indexer.pool.execute("DELETE FROM conditions WHERE condition_id = $1", '0x' + 'c' * 64)
        await indexer.pool.execute("DELETE FROM blocks WHERE number = 999999999")

        logger.success("✓ Test data cleaned up")

        await indexer.close()

    except Exception as e:
        logger.warning(f"⚠ Cleanup warning: {e}")


async def main():
    """Run all tests"""
    logger.info("=" * 60)
    logger.info("POLYMARKET INDEXER TEST SUITE")
    logger.info("=" * 60)

    tests = [
        ("Database Connection", test_database_connection),
        ("Schema Verification", test_schema_exists),
        ("Mock Data Insertion", test_insert_mock_data),
        ("Market Metrics", test_market_metrics_calculation),
        ("User Position Tracking", test_user_position_tracking),
        ("Blockchain Connection", test_blockchain_connection),
        ("Indexer State", test_indexer_state),
        ("Active Markets Query", test_get_active_markets),
    ]

    results = []

    for name, test_func in tests:
        try:
            result = await test_func()
            results.append((name, result))
        except Exception as e:
            logger.error(f"Test '{name}' crashed: {e}")
            results.append((name, False))

        await asyncio.sleep(0.5)

    # Cleanup
    await cleanup_test_data()

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        logger.info(f"{status}: {name}")

    logger.info(f"\nTotal: {passed}/{total} tests passed")

    if passed == total:
        logger.success("\n🎉 All tests passed!")
        return 0
    else:
        logger.error(f"\n❌ {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)