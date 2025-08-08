import requests
import json
from datetime import datetime
from typing import Dict, List, Any


class PolymarketDiagnostic:
    def __init__(self):
        self.base_url = "https://gamma-api.polymarket.com"
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'Polymarket-Diagnostic/1.0'
        })

    def format_market(self, market: Dict[str, Any]) -> str:
        """Format a market for readable display."""
        question = market.get('question', 'No question')
        end_date = market.get('endDate', 'No end date')
        closed = market.get('closed', 'Unknown')
        archived = market.get('archived', 'Unknown')
        volume_24h = market.get('volume24hr', 0)
        liquidity = market.get('liquidityNum', 0)

        # Parse end date for better display
        try:
            if end_date and end_date != 'No end date':
                parsed_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                formatted_date = parsed_date.strftime('%Y-%m-%d')
                days_from_now = (parsed_date.date() - datetime.now().date()).days
                date_info = f"{formatted_date} ({days_from_now:+d} days)"
            else:
                date_info = "No end date"
        except:
            date_info = end_date

        return f"""
 {question[:80]}{'...' if len(question) > 80 else ''}
    End Date: {date_info}
    Closed: {closed} | Archived: {archived}
    24h Volume: ${volume_24h} | Liquidity: ${liquidity}
    ID: {market.get('id', 'N/A')}
"""

    def test_endpoint(self, endpoint: str, params: Dict[str, Any] = None, description: str = "") -> Dict[str, Any]:
        """Test an API endpoint with given parameters."""
        try:
            url = f"{self.base_url}{endpoint}"
            print(f"\n Testing: {description or endpoint}")
            print(f"   URL: {url}")
            print(f"   Params: {params or 'None'}")

            response = self.session.get(url, params=params, timeout=10)
            print(f"   Status: {response.status_code}")

            if response.status_code == 200:
                try:
                    data = response.json()
                    return {"success": True, "data": data, "url": response.url}
                except json.JSONDecodeError:
                    print(f"Invalid JSON response")
                    return {"success": False, "error": "Invalid JSON"}
            else:
                print(f"HTTP Error: {response.text[:200]}")
                return {"success": False, "error": f"HTTP {response.status_code}", "text": response.text}

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return {"success": False, "error": str(e)}

    def analyze_markets(self, markets: List[Dict], title: str):
        """Analyze and display market data."""
        if not markets:
            print(f"\n{title}: No markets found")
            return

        print(f"\n{title}: Found {len(markets)} markets")

        # Analyze dates
        current_year = datetime.now().year
        future_markets = []
        current_markets = []
        past_markets = []

        for market in markets:
            end_date = market.get('endDate')
            if end_date:
                try:
                    parsed_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                    if parsed_date.year >= current_year and parsed_date.date() > datetime.now().date():
                        future_markets.append(market)
                    elif parsed_date.year == current_year:
                        current_markets.append(market)
                    else:
                        past_markets.append(market)
                except:
                    past_markets.append(market)
            else:
                past_markets.append(market)

        print(f"Future markets: {len(future_markets)}")
        print(f"Current year markets: {len(current_markets)}")
        print(f"Past markets: {len(past_markets)}")

        # Show some examples
        if future_markets:
            print(f"\nFUTURE MARKETS (showing first 3):")
            for market in future_markets[:3]:
                print(self.format_market(market))

        if current_markets:
            print(f"\nCURRENT YEAR MARKETS (showing first 3):")
            for market in current_markets[:3]:
                print(self.format_market(market))

        if past_markets and not (future_markets or current_markets):
            print(f"\nPAST MARKETS (showing first 3 - this is what we're trying to avoid):")
            for market in past_markets[:3]:
                print(self.format_market(market))

    def run_comprehensive_test(self):
        """Run comprehensive tests to find current markets."""
        print("Starting Polymarket API Comprehensive Test")
        print("=" * 60)

        # Test 1: Default endpoint
        result = self.test_endpoint("/markets", description="Default /markets endpoint")
        if result["success"]:
            self.analyze_markets(result["data"], "DEFAULT ENDPOINT")

        # Test 2: Common filtering attempts
        filter_tests = [
            ({"closed": "false"}, "Only non-closed markets"),
            ({"archived": "false"}, "Only non-archived markets"),
            ({"closed": "false", "archived": "false"}, "Non-closed AND non-archived"),
            ({"active": "true"}, "Only active markets"),
            ({"closed": "false", "active": "true"}, "Non-closed AND active"),
            ({"order": "volume24hr", "order_dir": "desc"}, "Ordered by 24h volume"),
            ({"order": "liquidity", "order_dir": "desc"}, "Ordered by liquidity"),
            ({"order": "created_at", "order_dir": "desc"}, "Ordered by creation date"),
            ({"limit": 100}, "Increased limit to 100"),
            ({"offset": 0, "limit": 50}, "With offset parameter"),
        ]

        for params, description in filter_tests:
            result = self.test_endpoint("/markets", params, description)
            if result["success"] and isinstance(result["data"], list):
                self.analyze_markets(result["data"], f"FILTERED: {description}")

        # Test 3: Alternative endpoint structures
        alternative_endpoints = [
            "/markets/active",
            "/markets/live",
            "/markets/current",
            "/markets/open",
            "/v1/markets",
            "/v2/markets",
            "/events",
            "/live-markets",
            "/active-markets"
        ]

        for endpoint in alternative_endpoints:
            result = self.test_endpoint(endpoint, description=f"Alternative endpoint: {endpoint}")
            if result["success"]:
                data = result["data"]
                if isinstance(data, list):
                    self.analyze_markets(data, f"ENDPOINT: {endpoint}")
                elif isinstance(data, dict) and "data" in data:
                    self.analyze_markets(data["data"], f"ENDPOINT: {endpoint}")

        # Test 4: Date-based filtering
        current_date = datetime.now().isoformat()
        date_tests = [
            ({"end_date_min": current_date}, "Markets ending after today"),
            ({"start_date_max": current_date}, "Markets starting before today"),
            ({"created_after": "2024-01-01T00:00:00Z"}, "Created after 2024"),
            ({"updated_after": "2024-01-01T00:00:00Z"}, "Updated after 2024"),
        ]

        for params, description in date_tests:
            result = self.test_endpoint("/markets", params, description)
            if result["success"] and isinstance(result["data"], list):
                self.analyze_markets(result["data"], f"DATE FILTER: {description}")

        # Test 5: Category-based searches
        category_tests = [
            ({"category": "crypto"}, "Crypto category"),
            ({"category": "politics"}, "Politics category"),
            ({"category": "sports"}, "Sports category"),
            ({"search": "2025"}, "Search for 2025"),
            ({"search": "Trump"}, "Search for Trump"),
            ({"search": "Bitcoin"}, "Search for Bitcoin"),
        ]

        for params, description in category_tests:
            result = self.test_endpoint("/markets", params, description)
            if result["success"] and isinstance(result["data"], list):
                self.analyze_markets(result["data"], f"CATEGORY: {description}")

        print("\n" + "=" * 60)
        print("Diagnostic Complete!")
        print("\nIf you see mostly past markets, the API might be:")
        print("1. Returning historical data by default")
        print("2. Using different parameter names than expected")
        print("3. Requiring authentication for current markets")
        print("4. Having a different endpoint structure")


def main():
    diagnostic = PolymarketDiagnostic()
    diagnostic.run_comprehensive_test()


if __name__ == "__main__":
    main()