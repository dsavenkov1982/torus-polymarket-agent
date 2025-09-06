#!/usr/bin/env python3
"""
Complete Polymarket Indexer POC - Fully Working Version
Run: python polymarket_indexer.py

Optional: pip install fastapi uvicorn (for web interface)
"""

import sqlite3
import json
import logging
import random
import threading
import time
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# Try FastAPI import
try:
    from fastapi import FastAPI, HTTPException, Query
    from fastapi.responses import JSONResponse
    from fastapi.middleware.cors import CORSMiddleware
    import uvicorn

    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False
    print("FastAPI not available. Running in CLI mode.")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, db_path: str = "polymarket_indexer.db"):
        self.db_path = db_path
        self.conn = None
        self._lock = threading.Lock()
        self.setup_database()

    def setup_database(self):
        """Create database and tables"""
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

        # Enable optimizations
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=10000")

        # Create tables
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS blocks (
            number INTEGER PRIMARY KEY,
            hash TEXT NOT NULL UNIQUE,
            timestamp TEXT NOT NULL,
            parent_hash TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS conditions (
            condition_id TEXT PRIMARY KEY,
            oracle TEXT NOT NULL,
            question_id TEXT NOT NULL,
            question TEXT NOT NULL,
            description TEXT,
            outcome_slot_count INTEGER DEFAULT 2,
            created_at_block INTEGER,
            created_at TEXT,
            resolved BOOLEAN DEFAULT 0,
            end_date TEXT,
            category TEXT DEFAULT 'Politics'
        );

        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tx_hash TEXT NOT NULL,
            log_index INTEGER NOT NULL,
            block_number INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            trader TEXT NOT NULL,
            token_id TEXT NOT NULL,
            token_amount REAL NOT NULL,
            collateral_amount REAL NOT NULL,
            price REAL NOT NULL,
            is_buy BOOLEAN NOT NULL,
            condition_id TEXT NOT NULL,
            UNIQUE(tx_hash, log_index)
        );

        CREATE TABLE IF NOT EXISTS market_metrics (
            condition_id TEXT PRIMARY KEY,
            volume_24h REAL DEFAULT 0,
            volume_7d REAL DEFAULT 0,
            volume_all_time REAL DEFAULT 0,
            trade_count_24h INTEGER DEFAULT 0,
            yes_price REAL DEFAULT 0.5,
            no_price REAL DEFAULT 0.5,
            last_price REAL DEFAULT 0.5,
            total_liquidity REAL DEFAULT 0,
            unique_traders_24h INTEGER DEFAULT 0,
            price_change_24h REAL DEFAULT 0,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            condition_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            price REAL NOT NULL,
            volume REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS indexer_state (
            name TEXT PRIMARY KEY,
            last_processed_block INTEGER NOT NULL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'RUNNING'
        );

        -- Create indexes
        CREATE INDEX IF NOT EXISTS idx_trades_condition ON trades(condition_id);
        CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
        CREATE INDEX IF NOT EXISTS idx_price_history_condition ON price_history(condition_id, timestamp);
        """)

        self.conn.commit()
        logger.info(f"Database initialized: {self.db_path}")

    def execute(self, query: str, params: tuple = ()):
        """Execute query with thread safety"""
        with self._lock:
            cursor = self.conn.execute(query, params)
            self.conn.commit()
            return cursor

    def fetchall(self, query: str, params: tuple = ()) -> List[Dict]:
        """Fetch all results"""
        with self._lock:
            cursor = self.conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def fetchone(self, query: str, params: tuple = ()) -> Optional[Dict]:
        """Fetch one result"""
        with self._lock:
            cursor = self.conn.execute(query, params)
            row = cursor.fetchone()
            return dict(row) if row else None

    def insert_condition(self, condition_data: Dict):
        """Insert a new condition"""
        self.execute("""
            INSERT OR REPLACE INTO conditions 
            (condition_id, oracle, question_id, question, description, 
             outcome_slot_count, created_at_block, created_at, end_date, category)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            condition_data['condition_id'], condition_data['oracle'],
            condition_data['question_id'], condition_data['question'],
            condition_data['description'], condition_data['outcome_slot_count'],
            condition_data['created_at_block'], condition_data['created_at'],
            condition_data['end_date'], condition_data['category']
        ))

    def insert_trade(self, trade_data: Dict):
        """Insert a new trade"""
        self.execute("""
            INSERT OR IGNORE INTO trades 
            (tx_hash, log_index, block_number, timestamp, trader, token_id,
             token_amount, collateral_amount, price, is_buy, condition_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            trade_data['tx_hash'], trade_data['log_index'], trade_data['block_number'],
            trade_data['timestamp'], trade_data['trader'], trade_data['token_id'],
            trade_data['token_amount'], trade_data['collateral_amount'],
            trade_data['price'], trade_data['is_buy'], trade_data['condition_id']
        ))

        # Add to price history
        self.execute("""
            INSERT INTO price_history (condition_id, timestamp, price, volume)
            VALUES (?, ?, ?, ?)
        """, (trade_data['condition_id'], trade_data['timestamp'],
              trade_data['price'], trade_data['collateral_amount']))

    def update_market_metrics(self, condition_id: str):
        """Update market metrics for a condition"""
        # Calculate 24h metrics
        metrics = self.fetchone("""
            SELECT 
                COUNT(*) as trade_count_24h,
                SUM(collateral_amount) as volume_24h,
                COUNT(DISTINCT trader) as unique_traders_24h
            FROM trades 
            WHERE condition_id = ? 
            AND datetime(timestamp) > datetime('now', '-1 day')
        """, (condition_id,))

        # Calculate 7d and all-time volume
        volume_7d = self.fetchone("""
            SELECT SUM(collateral_amount) as volume_7d
            FROM trades WHERE condition_id = ? 
            AND datetime(timestamp) > datetime('now', '-7 days')
        """, (condition_id,))['volume_7d'] or 0

        volume_all = self.fetchone("""
            SELECT SUM(collateral_amount) as volume_all_time
            FROM trades WHERE condition_id = ?
        """, (condition_id,))['volume_all_time'] or 0

        # Get latest price
        latest_trade = self.fetchone("""
            SELECT price FROM trades 
            WHERE condition_id = ? ORDER BY timestamp DESC LIMIT 1
        """, (condition_id,))

        # Get price 24h ago
        price_24h_ago = self.fetchone("""
            SELECT price FROM trades WHERE condition_id = ? 
            AND datetime(timestamp) <= datetime('now', '-1 day')
            ORDER BY timestamp DESC LIMIT 1
        """, (condition_id,))

        # Calculate values
        last_price = latest_trade['price'] if latest_trade else 0.5
        yes_price = last_price
        no_price = 1.0 - last_price
        price_change_24h = 0

        if price_24h_ago and price_24h_ago['price']:
            price_change_24h = last_price - price_24h_ago['price']

        volume_24h = metrics['volume_24h'] or 0
        total_liquidity = volume_24h * 2.5 if volume_24h > 0 else 1000

        # Update metrics
        self.execute("""
            INSERT OR REPLACE INTO market_metrics 
            (condition_id, volume_24h, volume_7d, volume_all_time, trade_count_24h,
             yes_price, no_price, last_price, total_liquidity, unique_traders_24h,
             price_change_24h, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            condition_id, volume_24h, volume_7d, volume_all,
            metrics['trade_count_24h'] or 0, yes_price, no_price, last_price,
            total_liquidity, metrics['unique_traders_24h'] or 0,
            price_change_24h, datetime.now().isoformat()
        ))

    def get_active_markets(self, limit: int = 20) -> List[Dict]:
        """Get active markets with metrics"""
        return self.fetchall("""
            SELECT 
                c.condition_id, c.question, c.description, c.end_date,
                c.resolved, c.created_at, c.category,
                m.yes_price, m.no_price, m.volume_24h, m.volume_7d,
                m.total_liquidity, m.trade_count_24h, m.last_price,
                m.price_change_24h, m.unique_traders_24h
            FROM conditions c
            LEFT JOIN market_metrics m ON c.condition_id = m.condition_id
            WHERE c.resolved = 0
            ORDER BY COALESCE(m.volume_24h, 0) DESC
            LIMIT ?
        """, (limit,))

    def get_market_by_id(self, condition_id: str) -> Optional[Dict]:
        """Get market by ID"""
        return self.fetchone("""
            SELECT 
                c.condition_id, c.question, c.description, c.end_date,
                c.resolved, c.created_at, c.category,
                m.yes_price, m.no_price, m.volume_24h, m.volume_7d,
                m.total_liquidity, m.trade_count_24h, m.last_price,
                m.price_change_24h, m.unique_traders_24h
            FROM conditions c
            LEFT JOIN market_metrics m ON c.condition_id = m.condition_id
            WHERE c.condition_id = ?
        """, (condition_id,))

    def get_trades(self, condition_id: Optional[str] = None, limit: int = 100) -> List[Dict]:
        """Get trades"""
        if condition_id:
            return self.fetchall("""
                SELECT * FROM trades WHERE condition_id = ? 
                ORDER BY timestamp DESC LIMIT ?
            """, (condition_id, limit))
        else:
            return self.fetchall("""
                SELECT * FROM trades ORDER BY timestamp DESC LIMIT ?
            """, (limit,))

    def get_price_history(self, condition_id: str, hours: int = 24) -> List[Dict]:
        """Get price history"""
        return self.fetchall("""
            SELECT timestamp, price, volume FROM price_history 
            WHERE condition_id = ? 
            AND datetime(timestamp) > datetime('now', '-{} hours')
            ORDER BY timestamp ASC
        """.format(hours), (condition_id,))

    def get_stats(self) -> Dict:
        """Get overall statistics"""
        market_stats = self.fetchone("""
            SELECT 
                COUNT(*) as total_markets,
                SUM(CASE WHEN resolved = 0 THEN 1 ELSE 0 END) as active_markets,
                SUM(CASE WHEN resolved = 1 THEN 1 ELSE 0 END) as resolved_markets
            FROM conditions
        """) or {}

        trade_stats = self.fetchone("""
            SELECT 
                COUNT(*) as total_trades,
                SUM(collateral_amount) as total_volume,
                COUNT(DISTINCT trader) as unique_traders
            FROM trades
        """) or {}

        block_stats = self.fetchone("""
            SELECT MAX(number) as latest_block FROM blocks
        """) or {}

        return {**market_stats, **trade_stats, **block_stats}


class PolymarketIndexer:
    def __init__(self, db: DatabaseManager):
        self.db = db
        self.running = False
        self.current_block = 50000000
        self.start_time = datetime.now()

        # Sample markets
        self.sample_markets = [
            {
                'condition_id': hashlib.sha256(b"bitcoin_150k_2025").hexdigest(),
                'oracle': '0x' + '8' * 40,
                'question_id': hashlib.sha256(b"bitcoin_question").hexdigest(),
                'question': "Will Bitcoin reach $150,000 by end of 2025?",
                'description': "This market will resolve to Yes if Bitcoin (BTC) reaches or exceeds $150,000 USD at any point before December 31, 2025, 11:59 PM ET.",
                'outcome_slot_count': 2,
                'created_at_block': 0,
                'created_at': datetime.now().isoformat(),
                'end_date': datetime(2025, 12, 31).isoformat(),
                'category': 'Crypto'
            },
            {
                'condition_id': hashlib.sha256(b"us_recession_2025").hexdigest(),
                'oracle': '0x' + '8' * 40,
                'question_id': hashlib.sha256(b"recession_question").hexdigest(),
                'question': "US recession in 2025?",
                'description': "This market will resolve to Yes if the NBER declares a recession occurred in the US during 2025.",
                'outcome_slot_count': 2,
                'created_at_block': 0,
                'created_at': datetime.now().isoformat(),
                'end_date': datetime(2025, 12, 31).isoformat(),
                'category': 'Economics'
            },
            {
                'condition_id': hashlib.sha256(b"gpt5_release_2025").hexdigest(),
                'oracle': '0x' + '8' * 40,
                'question_id': hashlib.sha256(b"gpt5_question").hexdigest(),
                'question': "OpenAI releases GPT-5 in 2025?",
                'description': "This market will resolve to Yes if OpenAI officially releases a model called GPT-5 during 2025.",
                'outcome_slot_count': 2,
                'created_at_block': 0,
                'created_at': datetime.now().isoformat(),
                'end_date': datetime(2025, 12, 31).isoformat(),
                'category': 'AI'
            },
            {
                'condition_id': hashlib.sha256(b"tesla_stock_500").hexdigest(),
                'oracle': '0x' + '8' * 40,
                'question_id': hashlib.sha256(b"tesla_question").hexdigest(),
                'question': "Tesla stock above $500 by June 2025?",
                'description': "This market will resolve to Yes if Tesla (TSLA) stock price is above $500 at market close on June 30, 2025.",
                'outcome_slot_count': 2,
                'created_at_block': 0,
                'created_at': datetime.now().isoformat(),
                'end_date': datetime(2025, 6, 30).isoformat(),
                'category': 'Stocks'
            },
            {
                'condition_id': hashlib.sha256(b"fed_rate_cut").hexdigest(),
                'oracle': '0x' + '8' * 40,
                'question_id': hashlib.sha256(b"fed_question").hexdigest(),
                'question': "Fed cuts rates by 100+ bps in 2025?",
                'description': "This market will resolve to Yes if the Federal Reserve cuts interest rates by a cumulative 100 basis points or more during 2025.",
                'outcome_slot_count': 2,
                'created_at_block': 0,
                'created_at': datetime.now().isoformat(),
                'end_date': datetime(2025, 12, 31).isoformat(),
                'category': 'Economics'
            }
        ]

        # Track prices
        self.market_prices = {}
        for market in self.sample_markets:
            self.market_prices[market['condition_id']] = random.uniform(0.2, 0.8)

    def start(self):
        """Start indexer"""
        if not self.running:
            self.running = True
            self.start_time = datetime.now()
            self._populate_initial_data()
            thread = threading.Thread(target=self._indexing_loop, daemon=True)
            thread.start()
            logger.info("Indexer started")

    def stop(self):
        """Stop indexer"""
        self.running = False
        logger.info("Indexer stopped")

    def _populate_initial_data(self):
        """Populate with sample data"""
        logger.info("Populating sample data...")

        # Insert markets
        for market in self.sample_markets:
            market['created_at_block'] = self.current_block - random.randint(1000, 10000)
            self.db.insert_condition(market)

        # Generate historical trades
        base_time = datetime.now() - timedelta(days=7)

        for i in range(1000):
            # Create block
            block_time = base_time + timedelta(minutes=i * 10)
            self.db.execute("""
                INSERT OR REPLACE INTO blocks (number, hash, timestamp, parent_hash)
                VALUES (?, ?, ?, ?)
            """, (
                self.current_block - 1000 + i,
                '0x' + hashlib.sha256(f"block_{i}".encode()).hexdigest(),
                block_time.isoformat(),
                '0x' + hashlib.sha256(f"parent_{i}".encode()).hexdigest()
            ))

            # Maybe generate trade
            if random.random() < 0.3:
                market = random.choice(self.sample_markets)
                condition_id = market['condition_id']

                # Update price
                current_price = self.market_prices[condition_id]
                price_change = random.uniform(-0.02, 0.02)
                new_price = max(0.01, min(0.99, current_price + price_change))
                self.market_prices[condition_id] = new_price

                # Create trade
                trade_data = {
                    'tx_hash': '0x' + hashlib.sha256(f"tx_{i}_{condition_id}".encode()).hexdigest(),
                    'log_index': 0,
                    'block_number': self.current_block - 1000 + i,
                    'timestamp': block_time.isoformat(),
                    'trader': '0x' + hashlib.sha256(f"trader_{random.randint(1, 50)}".encode()).hexdigest()[:40],
                    'token_id': f"{condition_id}_0",
                    'token_amount': random.uniform(100, 3000),
                    'collateral_amount': random.uniform(50, 1500),
                    'price': new_price,
                    'is_buy': random.choice([True, False]),
                    'condition_id': condition_id
                }

                self.db.insert_trade(trade_data)

        # Update metrics
        for market in self.sample_markets:
            self.db.update_market_metrics(market['condition_id'])

        logger.info("Sample data populated")

    def _indexing_loop(self):
        """Main indexing loop"""
        while self.running:
            try:
                self._process_block()
                time.sleep(3)  # Process every 3 seconds
            except Exception as e:
                logger.error(f"Indexing error: {e}")
                time.sleep(10)

    def _process_block(self):
        """Process new block"""
        self.current_block += 1

        # Create block
        self.db.execute("""
            INSERT OR REPLACE INTO blocks (number, hash, timestamp, parent_hash)
            VALUES (?, ?, ?, ?)
        """, (
            self.current_block,
            '0x' + hashlib.sha256(f"block_{self.current_block}".encode()).hexdigest(),
            datetime.now().isoformat(),
            '0x' + hashlib.sha256(f"parent_{self.current_block}".encode()).hexdigest()
        ))

        # Update indexer state
        self.db.execute("""
            INSERT OR REPLACE INTO indexer_state (name, last_processed_block, updated_at)
            VALUES (?, ?, ?)
        """, ("main", self.current_block, datetime.now().isoformat()))

        # Maybe generate trade
        if random.random() < 0.25:
            market = random.choice(self.sample_markets)
            condition_id = market['condition_id']

            # Update price
            current_price = self.market_prices[condition_id]
            price_change = random.uniform(-0.03, 0.03)
            new_price = max(0.01, min(0.99, current_price + price_change))
            self.market_prices[condition_id] = new_price

            # Create trade
            trade_data = {
                'tx_hash': '0x' + hashlib.sha256(
                    f"tx_{self.current_block}_{random.randint(1, 1000)}".encode()).hexdigest(),
                'log_index': 0,
                'block_number': self.current_block,
                'timestamp': datetime.now().isoformat(),
                'trader': '0x' + hashlib.sha256(f"trader_{random.randint(1, 100)}".encode()).hexdigest()[:40],
                'token_id': f"{condition_id}_0",
                'token_amount': random.uniform(100, 5000),
                'collateral_amount': random.uniform(50, 2500),
                'price': new_price,
                'is_buy': random.choice([True, False]),
                'condition_id': condition_id
            }

            self.db.insert_trade(trade_data)
            self.db.update_market_metrics(condition_id)

            logger.info(f"Block {self.current_block}: Trade at ${new_price:.3f} for {market['question'][:40]}...")

    def get_stats(self) -> Dict:
        """Get indexer stats"""
        return {
            'current_block': self.current_block,
            'indexer_status': 'running' if self.running else 'stopped',
            'uptime_seconds': int((datetime.now() - self.start_time).total_seconds()),
            'markets_count': len(self.sample_markets)
        }


# Global instances
database = DatabaseManager()
indexer = PolymarketIndexer(database)

# FastAPI app
if FASTAPI_AVAILABLE:
    app = FastAPI(
        title="Polymarket Indexer POC",
        description="Blockchain indexer for Polymarket prediction markets",
        version="2.0.0"
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


    @app.on_event("startup")
    async def startup_event():
        indexer.start()


    @app.on_event("shutdown")
    async def shutdown_event():
        indexer.stop()


    @app.get("/")
    async def root():
        return {
            "message": "Polymarket Indexer POC v2.0",
            "status": "running",
            "database": database.db_path,
            "endpoints": {
                "markets": "/api/v1/markets",
                "trades": "/api/v1/trades",
                "stats": "/api/v1/stats",
                "docs": "/docs"
            }
        }


    @app.get("/health")
    async def health_check():
        try:
            stats = indexer.get_stats()
            db_stats = database.get_stats()
            return {
                "status": "healthy",
                "indexer": stats,
                "database": db_stats
            }
        except Exception as e:
            return JSONResponse(
                status_code=503,
                content={"status": "unhealthy", "error": str(e)}
            )


    @app.get("/api/v1/markets")
    async def get_markets(
            limit: int = Query(20, ge=1, le=100),
            category: Optional[str] = Query(None)
    ):
        try:
            markets = database.get_active_markets(limit=limit)
            if category:
                markets = [m for m in markets if m.get('category', '').lower() == category.lower()]
            return {"markets": markets, "count": len(markets)}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


    @app.get("/api/v1/markets/{condition_id}")
    async def get_market(condition_id: str):
        try:
            market = database.get_market_by_id(condition_id)
            if not market:
                raise HTTPException(status_code=404, detail="Market not found")
            return {"market": market}
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


    @app.get("/api/v1/markets/{condition_id}/trades")
    async def get_market_trades(
            condition_id: str,
            limit: int = Query(50, ge=1, le=500)
    ):
        try:
            market = database.get_market_by_id(condition_id)
            if not market:
                raise HTTPException(status_code=404, detail="Market not found")
            trades = database.get_trades(condition_id=condition_id, limit=limit)
            return {"trades": trades, "count": len(trades), "market": market['question']}
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


    @app.get("/api/v1/markets/{condition_id}/price-history")
    async def get_price_history(
            condition_id: str,
            hours: int = Query(24, ge=1, le=168)
    ):
        try:
            market = database.get_market_by_id(condition_id)
            if not market:
                raise HTTPException(status_code=404, detail="Market not found")
            history = database.get_price_history(condition_id, hours)
            return {"price_history": history, "count": len(history), "market": market['question']}
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


    @app.get("/api/v1/trades")
    async def get_recent_trades(limit: int = Query(100, ge=1, le=1000)):
        try:
            trades = database.get_trades(limit=limit)
            return {"trades": trades, "count": len(trades)}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


    @app.get("/api/v1/stats")
    async def get_indexer_stats():
        try:
            indexer_stats = indexer.get_stats()
            db_stats = database.get_stats()
            return {"indexer": indexer_stats, "database": db_stats}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


    # MCP endpoints
    @app.get("/mcp/markets")
    async def mcp_get_markets():
        try:
            markets = database.get_active_markets(limit=50)
            return {"jsonrpc": "2.0", "result": {"markets": markets}}
        except Exception as e:
            return {"jsonrpc": "2.0", "error": {"code": -32603, "message": str(e)}}


    @app.get("/mcp/search")
    async def mcp_search_markets(q: str = Query(...)):
        try:
            markets = database.fetchall("""
                SELECT c.*, m.yes_price, m.no_price, m.volume_24h 
                FROM conditions c
                LEFT JOIN market_metrics m ON c.condition_id = m.condition_id
                WHERE c.question LIKE ? OR c.description LIKE ? OR c.category LIKE ?
                ORDER BY COALESCE(m.volume_24h, 0) DESC LIMIT 20
            """, (f"%{q}%", f"%{q}%", f"%{q}%"))
            return {"jsonrpc": "2.0", "result": {"markets": markets, "query": q}}
        except Exception as e:
            return {"jsonrpc": "2.0", "error": {"code": -32603, "message": str(e)}}


def print_sample_data():
    """Print sample data for CLI mode"""
    print("\n" + "=" * 60)
    print("📊 POLYMARKET INDEXER POC - SAMPLE DATA")
    print("=" * 60)

    markets = database.get_active_markets(limit=5)
    for market in markets:
        print(f"\n🔮 {market['question']}")
        print(f"   Category: {market['category']}")
        print(f"   Yes Price: ${market['yes_price']:.3f} ({market['yes_price'] * 100:.1f}%)")
        print(f"   24h Volume: ${market['volume_24h']:.0f}")
        print(f"   24h Trades: {market['trade_count_24h']}")
        print(f"   Liquidity: ${market['total_liquidity']:.0f}")

    print(f"\n📈 RECENT TRADES")
    print("-" * 40)
    trades = database.get_trades(limit=5)
    for trade in trades:
        side = "BUY" if trade['is_buy'] else "SELL"
        print(f"   {side} ${trade['price']:.3f} | ${trade['collateral_amount']:.0f} | {trade['timestamp'][:19]}")

    stats = database.get_stats()
    print(f"\n📊 INDEXER STATISTICS")
    print("-" * 40)
    print(f"   Total Markets: {stats.get('total_markets', 0)}")
    print(f"   Total Trades: {stats.get('total_trades', 0)}")
    print(f"   Total Volume: ${stats.get('total_volume', 0):.0f}")
    print(f"   Unique Traders: {stats.get('unique_traders', 0)}")
    print(f"   Latest Block: {stats.get('latest_block', 0)}")
    print(f"   Database: {database.db_path}")


def main():
    """Main function"""
    print("🚀 Polymarket Indexer POC v2.0")
    print("=" * 50)
    print("Features:")
    print("✓ SQLite database (auto-created)")
    print("✓ Real-time blockchain simulation")
    print("✓ 5 sample prediction markets")
    print("✓ Realistic trading data")
    print("✓ REST API endpoints")
    print("✓ MCP server compatibility")

    # Start indexer
    print(f"\n🔄 Starting indexer...")
    print(f"📁 Database: {database.db_path}")
    indexer.start()

    # Wait for initial data
    time.sleep(5)

    # Show sample data
    print_sample_data()

    if FASTAPI_AVAILABLE:
        print(f"\n🌐 Starting web server on port 8000...")
        print(f"   API Documentation: http://localhost:8000/docs")
        print(f"   Market Data: http://localhost:8000/api/v1/markets")
        print(f"   Statistics: http://localhost:8000/api/v1/stats")
        print(f"   Health Check: http://localhost:8000/health")
        print(f"   MCP Endpoint: http://localhost:8000/mcp/markets")

        try:
            uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
        except KeyboardInterrupt:
            print("\n🛑 Shutting down...")
            indexer.stop()
    else:
        print(f"\n📊 Running in CLI mode (FastAPI not available)")
        print("   Install FastAPI for web interface: pip install fastapi uvicorn")
        print(f"   Database available at: {database.db_path}")
        print("\nPress Ctrl+C to stop...")

        try:
            while True:
                time.sleep(30)
                print_sample_data()
        except KeyboardInterrupt:
            print("\n🛑 Shutting down...")
            indexer.stop()


if __name__ == "__main__":
    main()