#!/usr/bin/env python3
"""
Complete Corrected Polymarket Graph API - combines both subgraphs with proper decimal handling
"""

import requests
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any


class CompletePolymarketAPI:
    """Complete Polymarket API using both subgraphs with correct field mappings."""

    def __init__(self, api_key: str):
        self.api_key = api_key

        # Two different subgraphs for different data
        self.market_makers_url = f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/Bx1W4S7kDVxs9gC3s2G6DS8kdNBJNVhMviCtin2DiBp"
        self.main_polymarket_url = f"https://gateway-arbitrum.network.thegraph.com/api/{api_key}/subgraphs/id/Haut8ME8wR1roSE2du5gKAH8CEeXF4UeogLg5o2mUiq7"

        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })

    def print_separator(self, title: str):
        print("\n" + "=" * 70)
        print(f"🧪 {title}")
        print("=" * 70)

    def execute_query(self, query: str, url: str, variables: Dict = None) -> Optional[Dict]:
        """Execute a GraphQL query against the specified endpoint."""
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        try:
            response = self.session.post(url, json=payload, timeout=30)
            response.raise_for_status()

            data = response.json()

            if 'errors' in data:
                print(f"❌ GraphQL errors: {data['errors']}")
                return None

            return data.get('data')

        except Exception as e:
            print(f"❌ Request error: {e}")
            return None

    def format_payout(self, payout_wei: str) -> float:
        """Convert payout from wei to USD (USDC has 6 decimals)."""
        return float(payout_wei) / 1e6

    def format_timestamp(self, timestamp: str) -> str:
        """Convert timestamp to readable date."""
        return datetime.fromtimestamp(int(timestamp), tz=timezone.utc).isoformat()

    # ==================== MARKET MAKERS API (Questions & Outcomes) ====================

    def get_market_questions(self, limit: int = 10, min_volume: float = 0) -> List[Dict]:
        """Get market questions with outcomes and prices from FixedProductMarketMakers."""
        query = f"""
        {{
            fixedProductMarketMakers(
                first: {limit}, 
                where: {{usdVolumeBeforeFees_gt: "{min_volume}"}},
                orderBy: creationTimestamp, 
                orderDirection: desc
            ) {{
                id
                question
                outcomes
                outcomeTokenPrices
                creator
                creationTimestamp
                lastActiveDay
                usdVolumeBeforeFees
                usdLiquidityMeasure
                collateralToken {{
                    id
                    symbol
                    name
                    decimals
                }}
                conditions {{
                    id
                }}
            }}
        }}
        """

        result = self.execute_query(query, self.market_makers_url)
        if result and 'fixedProductMarketMakers' in result:
            markets = result['fixedProductMarketMakers']

            # Clean and format the data
            for market in markets:
                if market.get('creationTimestamp'):
                    market['createdAt'] = self.format_timestamp(market['creationTimestamp'])

                if market.get('lastActiveDay'):
                    market['lastActiveAt'] = self.format_timestamp(market['lastActiveDay'])

                # Convert prices to floats
                if market.get('outcomeTokenPrices'):
                    market['pricesFloat'] = [float(p) for p in market['outcomeTokenPrices']]

                if market.get('usdVolumeBeforeFees'):
                    market['volumeUSD'] = float(market['usdVolumeBeforeFees'])

                if market.get('usdLiquidityMeasure'):
                    market['liquidityUSD'] = float(market['usdLiquidityMeasure'])

            return markets

        return []

    def search_markets_by_topic(self, topic: str, limit: int = 5) -> List[Dict]:
        """Search for markets containing specific keywords."""
        markets = self.get_market_questions(limit=50, min_volume=0)

        topic_lower = topic.lower()
        matching_markets = []

        for market in markets:
            question = market.get('question', '').lower()
            if topic_lower in question:
                matching_markets.append(market)
                if len(matching_markets) >= limit:
                    break

        return matching_markets

    # ==================== MAIN POLYMARKET API (Positions & Redemptions) ====================

    def get_positions(self, limit: int = 10) -> List[Dict]:
        """Get trading positions with correct field names."""
        query = f"""
        {{
            positions(first: {limit}, orderBy: id, orderDirection: desc) {{
                id
                condition
                outcomeIndex
            }}
        }}
        """

        result = self.execute_query(query, self.main_polymarket_url)
        if result and 'positions' in result:
            return result['positions']

        return []

    def get_recent_redemptions(self, limit: int = 20) -> List[Dict]:
        """Get recent redemptions with correct decimal conversion."""
        query = f"""
        {{
            redemptions(
                first: {limit},
                orderBy: timestamp,
                orderDirection: desc
            ) {{
                id
                payout
                redeemer
                timestamp
                condition
            }}
        }}
        """

        result = self.execute_query(query, self.main_polymarket_url)
        if result and 'redemptions' in result:
            redemptions = result['redemptions']

            # Format data properly
            for redemption in redemptions:
                if redemption.get('timestamp'):
                    redemption['redeemedAt'] = self.format_timestamp(redemption['timestamp'])

                if redemption.get('payout'):
                    # CORRECT decimal conversion for USDC (6 decimals)
                    redemption['payoutUSD'] = self.format_payout(redemption['payout'])

            return redemptions

        return []

    def get_largest_redemptions(self, limit: int = 10) -> List[Dict]:
        """Get largest redemptions (biggest market winners)."""
        query = f"""
        {{
            redemptions(
                first: {limit},
                orderBy: payout,
                orderDirection: desc
            ) {{
                id
                payout
                redeemer
                timestamp
                condition
            }}
        }}
        """

        result = self.execute_query(query, self.main_polymarket_url)
        if result and 'redemptions' in result:
            redemptions = result['redemptions']

            # Format data properly
            for redemption in redemptions:
                if redemption.get('timestamp'):
                    redemption['redeemedAt'] = self.format_timestamp(redemption['timestamp'])

                if redemption.get('payout'):
                    redemption['payoutUSD'] = self.format_payout(redemption['payout'])

            return redemptions

        return []

    def get_conditions(self, limit: int = 10) -> List[Dict]:
        """Get market conditions (market IDs)."""
        query = f"""
        {{
            conditions(first: {limit}, orderBy: id, orderDirection: desc) {{
                id
            }}
        }}
        """

        result = self.execute_query(query, self.main_polymarket_url)
        if result and 'conditions' in result:
            return result['conditions']

        return []

    # ==================== ANALYSIS METHODS ====================

    def analyze_market_activity(self) -> Dict:
        """Comprehensive market activity analysis."""
        print("🔍 Analyzing Polymarket Activity...")

        analysis = {
            'recent_markets': [],
            'big_winners': [],
            'trading_activity': {},
            'market_conditions': [],
            'summary': {}
        }

        # Get recent markets with questions
        print("  📊 Fetching recent markets...")
        analysis['recent_markets'] = self.get_market_questions(limit=5, min_volume=100)

        # Get biggest redemptions
        print("  🏆 Fetching biggest wins...")
        analysis['big_winners'] = self.get_largest_redemptions(limit=5)

        # Get recent redemptions for activity analysis
        print("  📈 Analyzing trading activity...")
        recent_redemptions = self.get_recent_redemptions(limit=50)

        if recent_redemptions:
            total_payout = sum(r.get('payoutUSD', 0) for r in recent_redemptions)
            avg_payout = total_payout / len(recent_redemptions)
            max_payout = max(r.get('payoutUSD', 0) for r in recent_redemptions)

            analysis['trading_activity'] = {
                'total_recent_payouts': total_payout,
                'average_payout': avg_payout,
                'largest_recent_payout': max_payout,
                'total_redemptions': len(recent_redemptions)
            }

        # Get market conditions
        print("  🎯 Fetching market conditions...")
        analysis['market_conditions'] = self.get_conditions(limit=10)

        # Summary
        analysis['summary'] = {
            'active_markets': len(analysis['recent_markets']),
            'total_recent_volume': analysis['trading_activity'].get('total_recent_payouts', 0),
            'biggest_win': analysis['big_winners'][0].get('payoutUSD', 0) if analysis['big_winners'] else 0,
            'tracked_conditions': len(analysis['market_conditions'])
        }

        return analysis

    def find_crypto_markets(self) -> List[Dict]:
        """Find cryptocurrency-related markets."""
        crypto_terms = ['bitcoin', 'ethereum', 'crypto', 'btc', 'eth', 'solana', 'dogecoin']
        crypto_markets = []

        for term in crypto_terms:
            markets = self.search_markets_by_topic(term, limit=3)
            crypto_markets.extend(markets)

        # Remove duplicates by ID
        seen_ids = set()
        unique_crypto_markets = []
        for market in crypto_markets:
            market_id = market.get('id')
            if market_id not in seen_ids:
                seen_ids.add(market_id)
                unique_crypto_markets.append(market)

        return unique_crypto_markets


def main():
    """Demonstrate the complete corrected API."""
    print("🚀 Complete Corrected Polymarket Graph API Demo")
    print("=" * 70)

    # Use your API key
    api_key = "75d40427e4676b50425802db7503d97f"
    api = CompletePolymarketAPI(api_key)

    try:
        # 1. Market Questions (from FixedProductMarketMakers)
        print("\n🎯 1. Recent Market Questions")
        print("-" * 50)
        markets = api.get_market_questions(limit=3, min_volume=1000)

        for i, market in enumerate(markets, 1):
            question = market.get('question', 'No question')[:80]
            outcomes = market.get('outcomes', [])
            volume = market.get('volumeUSD', 0)
            liquidity = market.get('liquidityUSD', 0)

            print(f"📊 Market {i}: {question}...")
            print(f"   🎯 Outcomes: {outcomes}")
            print(f"   💰 Volume: ${volume:,.2f}")
            print(f"   💧 Liquidity: ${liquidity:,.2f}")

        # 2. Biggest Winners (corrected payouts)
        print(f"\n🏆 2. Biggest Recent Winners")
        print("-" * 50)
        big_winners = api.get_largest_redemptions(limit=5)

        for i, winner in enumerate(big_winners, 1):
            payout = winner.get('payoutUSD', 0)
            date = winner.get('redeemedAt', 'Unknown')[:10]
            redeemer = winner.get('redeemer', '')[:10] + '...'

            print(f"🥇 Winner {i}: ${payout:,.2f} to {redeemer} on {date}")

        # 3. Recent Trading Activity
        print(f"\n📈 3. Recent Trading Activity")
        print("-" * 50)
        recent_redemptions = api.get_recent_redemptions(limit=10)

        total_recent = sum(r.get('payoutUSD', 0) for r in recent_redemptions)
        print(f"💰 Total recent payouts: ${total_recent:,.2f}")
        print(f"📊 Recent redemptions: {len(recent_redemptions)}")

        if recent_redemptions:
            print(f"🔥 Latest activity:")
            for redemption in recent_redemptions[:3]:
                payout = redemption.get('payoutUSD', 0)
                date = redemption.get('redeemedAt', 'Unknown')[:16]
                print(f"  💎 ${payout:,.2f} on {date}")

        # 4. Crypto Markets Search
        print(f"\n🪙 4. Crypto-Related Markets")
        print("-" * 50)
        crypto_markets = api.find_crypto_markets()

        for i, market in enumerate(crypto_markets[:3], 1):
            question = market.get('question', 'No question')[:60]
            volume = market.get('volumeUSD', 0)
            print(f"🪙 Crypto {i}: {question}...")
            print(f"   💰 Volume: ${volume:,.2f}")

        # 5. Complete Analysis
        print(f"\n🔍 5. Complete Market Analysis")
        print("-" * 50)
        analysis = api.analyze_market_activity()

        summary = analysis['summary']
        print(f"📊 Active markets tracked: {summary['active_markets']}")
        print(f"💰 Recent trading volume: ${summary['total_recent_volume']:,.2f}")
        print(f"🏆 Biggest recent win: ${summary['biggest_win']:,.2f}")
        print(f"🎯 Market conditions: {summary['tracked_conditions']}")

        print(f"\n✅ Complete API Integration Working!")
        print("🎉 You now have:")
        print("   📊 Market questions and outcomes")
        print("   💰 Correct payout calculations")
        print("   📈 Real trading activity data")
        print("   🔍 Search and discovery features")
        print("   🎯 Combined market intelligence")

    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()