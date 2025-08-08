import requests
import json
from datetime import datetime, timezone


class ExploreMarketMakers:
    """Explore fixedProductMarketMakers for detailed market information."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/Bx1W4S7kDVxs9gC3s2G6DS8kdNBJNVhMviCtin2DiBp"

        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })

    def print_separator(self, title: str):
        print("\n" + "=" * 70)
        print(f"🧪 {title}")
        print("=" * 70)

    def print_results(self, data, max_items: int = 3):
        if isinstance(data, list):
            print(f"📊 Found {len(data)} items")
            for i, item in enumerate(data[:max_items]):
                print(f"\n📋 Item {i + 1}:")
                self.print_dict(item, indent=2)
            if len(data) > max_items:
                print(f"\n... and {len(data) - max_items} more items")
        elif isinstance(data, dict):
            self.print_dict(data)
        else:
            print(json.dumps(data, indent=2, default=str))

    def print_dict(self, data: dict, indent: int = 0):
        spaces = "  " * indent
        for key, value in data.items():
            if isinstance(value, dict):
                print(f"{spaces}{key}:")
                self.print_dict(value, indent + 1)
            elif isinstance(value, list) and len(value) > 0:
                print(f"{spaces}{key}: [{len(value)} items]")
                if isinstance(value[0], dict):
                    print(f"{spaces}  Sample keys: {list(value[0].keys())}")
                else:
                    print(f"{spaces}  Sample values: {value[:3]}")
            else:
                if isinstance(value, str) and len(value) > 100:
                    value = value[:100] + "..."
                print(f"{spaces}{key}: {value}")

    def execute_query(self, query: str, variables: dict = None):
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        try:
            response = self.session.post(self.base_url, json=payload)
            response.raise_for_status()

            data = response.json()

            if 'errors' in data:
                print(f"❌ GraphQL errors: {data['errors']}")
                return None

            return data.get('data')

        except requests.exceptions.RequestException as e:
            print(f"❌ Request error: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"❌ JSON decode error: {e}")
            return None

    def discover_fpmm_schema(self):
        """Discover what fields fixedProductMarketMakers has."""
        self.print_separator("FixedProductMarketMaker Schema Discovery")

        # Introspection for FPMM type
        introspection_query = """
        {
            __type(name: "FixedProductMarketMaker") {
                fields {
                    name
                    type {
                        name
                        ofType {
                            name
                        }
                    }
                }
            }
        }
        """

        result = self.execute_query(introspection_query)

        if result and '__type' in result and result['__type']:
            fields = result['__type']['fields']
            print(f"🎯 FixedProductMarketMaker has {len(fields)} fields:")

            for field in fields:
                field_name = field['name']
                field_type = field['type']['name'] if field['type'] else 'Unknown'
                if field['type'] and field['type'].get('ofType'):
                    field_type += f"[{field['type']['ofType']['name']}]"
                print(f"  📝 {field_name}: {field_type}")

            return [f['name'] for f in fields]
        else:
            print("❌ Could not discover FPMM schema")
            return []

    def test_basic_fpmm_data(self):
        """Test basic fixedProductMarketMakers data."""
        self.print_separator("Basic Market Makers Data")

        query = """
        {
            fixedProductMarketMakers(first: 5, orderBy: creationTimestamp, orderDirection: desc) {
                id
                question
                outcomes
                creator
                creationTimestamp
                collateralToken {
                    id
                    symbol
                    name
                    decimals
                }
            }
        }
        """

        result = self.execute_query(query)

        if result and 'fixedProductMarketMakers' in result:
            markets = result['fixedProductMarketMakers']
            print(f"🎯 Found {len(markets)} market makers")
            self.print_results(markets)

            # Check if we have actual market questions!
            if markets:
                questions_found = [m for m in markets if m.get('question')]
                print(f"\n📊 Markets with questions: {len(questions_found)}")

                if questions_found:
                    print(f"\n🎉 FOUND MARKET QUESTIONS!")
                    for i, market in enumerate(questions_found[:3], 1):
                        question = market.get('question', 'No question')
                        outcomes = market.get('outcomes', [])
                        print(f"  🎯 Market {i}: {question}")
                        print(f"     📊 Outcomes: {outcomes}")

            return markets
        else:
            print("❌ No market makers found")
            return []

    def test_detailed_fpmm_data(self):
        """Test detailed market maker data with all available fields."""
        self.print_separator("Detailed Market Makers Data")

        query = """
        {
            fixedProductMarketMakers(first: 3, orderBy: creationTimestamp, orderDirection: desc) {
                id
                question
                outcomes
                outcomeTokenAmounts
                outcomeTokenPrices
                outcomeTokenMarginalPrices
                creator
                creationTimestamp
                collateralToken {
                    id
                    symbol
                    name
                    decimals
                }
                conditions {
                    id
                }
                fee
                funding
                liquidityMeasure
                scaledLiquidityMeasure
                usdLiquidityMeasure
                collateralVolumeBeforeFees
                scaledCollateralVolumeBeforeFees
                usdVolumeBeforeFees
                lastActiveDay
                runningDailyVolumeByHour
            }
        }
        """

        result = self.execute_query(query)

        if result and 'fixedProductMarketMakers' in result:
            markets = result['fixedProductMarketMakers']
            print(f"🎯 Found {len(markets)} detailed market makers")
            self.print_results(markets)

            if markets:
                # Analyze the data quality
                print(f"\n📊 Data Quality Analysis:")

                for i, market in enumerate(markets, 1):
                    question = market.get('question')
                    outcomes = market.get('outcomes', [])
                    prices = market.get('outcomeTokenPrices', [])
                    volume = market.get('usdVolumeBeforeFees', '0')
                    liquidity = market.get('usdLiquidityMeasure', '0')

                    print(f"\n  🎯 Market {i}:")
                    print(f"     ❓ Question: {'✅' if question else '❌'} {question or 'Missing'}")
                    print(f"     📊 Outcomes: {'✅' if outcomes else '❌'} {len(outcomes)} outcomes")
                    print(f"     💰 Prices: {'✅' if prices else '❌'} {len(prices) if prices else 0} prices")
                    print(f"     💵 Volume: ${float(volume):,.2f}" if volume and volume != '0' else "     💵 Volume: $0")
                    print(
                        f"     💧 Liquidity: ${float(liquidity):,.2f}" if liquidity and liquidity != '0' else "     💧 Liquidity: $0")

            return markets
        else:
            print("❌ No detailed market makers found")
            return []

    def test_recent_active_markets(self):
        """Find recently active markets with trading activity."""
        self.print_separator("Recent Active Markets")

        query = """
        {
            fixedProductMarketMakers(
                first: 10, 
                where: {usdVolumeBeforeFees_gt: "100"},
                orderBy: lastActiveDay, 
                orderDirection: desc
            ) {
                id
                question
                outcomes
                outcomeTokenPrices
                usdVolumeBeforeFees
                usdLiquidityMeasure
                lastActiveDay
                creationTimestamp
            }
        }
        """

        result = self.execute_query(query)

        if result and 'fixedProductMarketMakers' in result:
            markets = result['fixedProductMarketMakers']
            print(f"💹 Found {len(markets)} active markets (volume > $100)")
            self.print_results(markets)

            if markets:
                print(f"\n🔥 Most Active Markets:")
                for i, market in enumerate(markets[:5], 1):
                    question = market.get('question', 'No question')[:60]
                    volume = float(market.get('usdVolumeBeforeFees', 0))
                    liquidity = float(market.get('usdLiquidityMeasure', 0))
                    last_active = market.get('lastActiveDay', 0)

                    # Convert timestamp to date
                    if last_active:
                        date = datetime.fromtimestamp(int(last_active), tz=timezone.utc)
                        date_str = date.strftime('%Y-%m-%d')
                    else:
                        date_str = 'Unknown'

                    print(f"  🏆 #{i}: {question}...")
                    print(f"       💰 Volume: ${volume:,.2f}")
                    print(f"       💧 Liquidity: ${liquidity:,.2f}")
                    print(f"       📅 Last Active: {date_str}")

            return markets
        else:
            print("❌ No active markets found")
            return []

    def test_crypto_markets(self):
        """Search for crypto-related markets in FPMM."""
        self.print_separator("Crypto Markets Search")

        # Get all recent markets and filter by crypto terms
        query = """
        {
            fixedProductMarketMakers(first: 50, orderBy: creationTimestamp, orderDirection: desc) {
                id
                question
                outcomes
                outcomeTokenPrices
                usdVolumeBeforeFees
                usdLiquidityMeasure
                creationTimestamp
            }
        }
        """

        result = self.execute_query(query)

        if result and 'fixedProductMarketMakers' in result:
            all_markets = result['fixedProductMarketMakers']

            # Filter for crypto terms
            crypto_terms = ['bitcoin', 'ethereum', 'crypto', 'btc', 'eth', 'defi', 'nft', 'solana', 'dogecoin',
                            'blockchain']
            crypto_markets = []

            for market in all_markets:
                question = market.get('question', '').lower()
                if any(term in question for term in crypto_terms):
                    crypto_markets.append(market)

            print(f"🪙 Found {len(crypto_markets)} crypto markets out of {len(all_markets)} total")

            if crypto_markets:
                self.print_results(crypto_markets)

                print(f"\n💎 Crypto Market Examples:")
                for i, market in enumerate(crypto_markets[:5], 1):
                    question = market.get('question', 'No question')
                    volume = float(market.get('usdVolumeBeforeFees', 0))
                    timestamp = int(market.get('creationTimestamp', 0))
                    date = datetime.fromtimestamp(timestamp, tz=timezone.utc)

                    print(f"  🪙 #{i}: {question}")
                    print(f"       💰 Volume: ${volume:,.2f}")
                    print(f"       📅 Created: {date.strftime('%Y-%m-%d')}")
            else:
                print("❌ No crypto markets found")

            return crypto_markets
        else:
            print("❌ Could not search for crypto markets")
            return []


def main():
    """Main exploration function."""

    # Use your API key
    api_key = "75d40427e4676b50425802db7503d97f"

    print("🔍 Exploring FixedProductMarketMakers for Market Details")
    print(f"🔑 Using API key: {api_key[:10]}...")

    explorer = ExploreMarketMakers(api_key)

    try:
        # Run comprehensive exploration
        fields = explorer.discover_fpmm_schema()
        basic_markets = explorer.test_basic_fpmm_data()
        detailed_markets = explorer.test_detailed_fpmm_data()
        active_markets = explorer.test_recent_active_markets()
        crypto_markets = explorer.test_crypto_markets()

        print(f"\n{'=' * 70}")
        print("🎉 Exploration Complete!")

        if any([basic_markets, detailed_markets, active_markets, crypto_markets]):
            print("✅ SUCCESS: Found market questions and data!")
            print("💡 This can replace your stale REST API data")
            print("🚀 Ready to integrate into your MCP agent")
        else:
            print("❌ No usable market data found")

        print(f"{'=' * 70}")

    except Exception as e:
        print(f"\n❌ Exploration failed: {e}")


if __name__ == "__main__":
    main()