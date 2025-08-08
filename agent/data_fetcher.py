import httpx
import asyncio
import re
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PolymarketService:
    """Polymarket service with real current market data from official APIs."""

    # Official Polymarket APIs (2025)
    GAMMA_API_URL = "https://gamma-api.polymarket.com"

    # Goldsky GraphQL endpoints (official Polymarket subgraphs)
    GOLDSKY_ACTIVITY_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/activity-subgraph/0.0.4/gn"
    GOLDSKY_POSITIONS_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/positions-subgraph/0.0.7/gn"
    GOLDSKY_ORDERS_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers={"Accept": "application/json", "Content-Type": "application/json"}
        )

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()

    def _sanitize_string(self, text: str) -> str:
        """Sanitize string data to prevent JSON issues."""
        if not isinstance(text, str):
            return str(text)

        replacements = {
            '\u201c': '"', '\u201d': '"', '\u2018': "'", '\u2019': "'",
            '\u2013': '-', '\u2014': '-', '\u2026': '...', '\u00a0': ' ',
        }

        for old, new in replacements.items():
            text = text.replace(old, new)

        text = ''.join(char for char in text if char.isprintable() or char in '\n\t')
        return text

    def _sanitize_data(self, data: Any) -> Any:
        """Recursively sanitize data to ensure JSON compatibility."""
        if isinstance(data, dict):
            return {
                self._sanitize_string(k): self._sanitize_data(v)
                for k, v in data.items()
            }
        elif isinstance(data, list):
            return [self._sanitize_data(item) for item in data]
        elif isinstance(data, str):
            return self._sanitize_string(data)
        elif isinstance(data, (int, float, bool)) or data is None:
            return data
        elif hasattr(data, 'isoformat'):
            return data.isoformat()
        else:
            return str(data)

    def _safe_json_response(self, data: Any, query_info: Optional[Dict] = None) -> Dict[str, Any]:
        """Create a JSON-safe response wrapper."""
        try:
            sanitized_data = self._sanitize_data(data)
            json.dumps(sanitized_data, ensure_ascii=False, default=str)

            response = {
                "success": True,
                "timestamp": datetime.now().isoformat(),
                "data": sanitized_data
            }

            if query_info:
                response.update(query_info)

            return response

        except (TypeError, ValueError, UnicodeDecodeError) as e:
            logger.error(f"JSON serialization error: {e}")
            return {
                "success": False,
                "error": f"Data serialization error: {str(e)}",
                "error_type": type(e).__name__,
                "timestamp": datetime.now().isoformat(),
                **(query_info or {})
            }

    # ========== Gamma REST API Methods ==========

    async def _fetch_gamma_markets(self, limit: int = 20, **params) -> List[Dict]:
        """Fetch markets from Gamma REST API."""
        try:
            # Default params for active markets
            default_params = {
                "limit": min(limit, 100),
                "active": True,
                "closed": False,
                "archived": False
            }

            # Override with provided params
            default_params.update(params)

            response = await self.client.get(f"{self.GAMMA_API_URL}/markets", params=default_params)
            response.raise_for_status()

            data = response.json()
            logger.info(f"Gamma API returned {len(data)} markets")

            return data if isinstance(data, list) else []

        except Exception as e:
            logger.error(f"Error fetching Gamma markets: {e}")
            return []

    async def _fetch_gamma_events(self, limit: int = 20, **params) -> List[Dict]:
        """Fetch events from Gamma REST API."""
        try:
            default_params = {
                "limit": min(limit, 100),
                "active": True,
                "closed": False,
                "archived": False
            }

            default_params.update(params)

            response = await self.client.get(f"{self.GAMMA_API_URL}/events", params=default_params)
            response.raise_for_status()

            data = response.json()
            logger.info(f"Gamma API returned {len(data)} events")

            return data if isinstance(data, list) else []

        except Exception as e:
            logger.error(f"Error fetching Gamma events: {e}")
            return []

    async def _fetch_gamma_market_by_slug(self, slug: str) -> Optional[Dict]:
        """Fetch a specific market by slug."""
        try:
            response = await self.client.get(f"{self.GAMMA_API_URL}/markets/{slug}")
            response.raise_for_status()

            return response.json()

        except Exception as e:
            logger.error(f"Error fetching market {slug}: {e}")
            return None

    # ========== Goldsky GraphQL Methods ==========

    async def _execute_goldsky_query(self, query: str, url: str) -> Optional[Dict]:
        """Execute a GraphQL query against Goldsky endpoints."""
        payload = {"query": query}

        try:
            response = await self.client.post(url, json=payload)
            response.raise_for_status()

            data = response.json()

            if 'errors' in data:
                logger.warning(f"Goldsky GraphQL errors: {data['errors']}")
                return None

            return data.get('data')

        except Exception as e:
            logger.error(f"Goldsky query error: {e}")
            return None

    async def _fetch_recent_trades(self, limit: int = 10) -> List[Dict]:
        """Fetch recent trades from Goldsky activity subgraph."""
        query = f"""
        {{
            trades(
                first: {min(limit, 100)},
                orderBy: timestamp,
                orderDirection: desc
            ) {{
                id
                timestamp
                trader
                collateralAmount
                outcomeTokensAmount
                fpmmAddress
                outcomeIndex
            }}
        }}
        """

        result = await self._execute_goldsky_query(query, self.GOLDSKY_ACTIVITY_URL)

        if result and 'trades' in result:
            return result['trades']
        return []

    # ========== Enhanced Market Processing ==========

    def _process_gamma_market(self, market: Dict) -> Dict:
        """Process and enhance market data from Gamma API."""
        try:
            processed = market.copy()

            # Parse JSON fields that come as strings
            if 'outcomePrices' in processed and isinstance(processed['outcomePrices'], str):
                try:
                    processed['outcomePrices'] = json.loads(processed['outcomePrices'])
                except:
                    processed['outcomePrices'] = []

            if 'clobTokenIds' in processed and isinstance(processed['clobTokenIds'], str):
                try:
                    processed['clobTokenIds'] = json.loads(processed['clobTokenIds'])
                except:
                    processed['clobTokenIds'] = []

            # Add computed fields
            processed['volume_usd'] = float(processed.get('volumeNum', 0))
            processed['liquidity_usd'] = float(processed.get('liquidityNum', 0))

            # Parse outcome prices to individual fields
            outcome_prices = processed.get('outcomePrices', [])
            if len(outcome_prices) >= 2:
                processed['yes_price'] = float(outcome_prices[0])
                processed['no_price'] = float(outcome_prices[1])
            else:
                processed['yes_price'] = 0.5
                processed['no_price'] = 0.5

            # Determine market status
            processed['is_active'] = processed.get('active', False) and not processed.get('closed', True)
            processed['status'] = 'active' if processed['is_active'] else 'closed'

            # Clean up text fields
            if 'question' in processed:
                processed['question'] = self._sanitize_string(processed['question'])
            if 'description' in processed:
                processed['description'] = self._sanitize_string(processed['description'])

            # Add timestamp info
            processed['created_at'] = processed.get('createdAt', '')
            processed['end_date'] = processed.get('endDateIso', '')

            return processed

        except Exception as e:
            logger.warning(f"Error processing market: {e}")
            return market

    def _process_gamma_event(self, event: Dict) -> Dict:
        """Process and enhance event data from Gamma API."""
        try:
            processed = event.copy()

            # Get markets from the event
            markets = processed.get('markets', [])

            # Calculate totals from related markets
            total_volume = sum(float(m.get('volumeNum', 0)) for m in markets)
            total_liquidity = sum(float(m.get('liquidityNum', 0)) for m in markets)

            processed['volume_usd'] = total_volume
            processed['liquidity_usd'] = total_liquidity
            processed['markets_count'] = len(markets)

            # Clean up text fields
            if 'title' in processed:
                processed['title'] = self._sanitize_string(processed['title'])
            if 'description' in processed:
                processed['description'] = self._sanitize_string(processed['description'])

            # Add status
            processed['is_active'] = processed.get('active', False)
            processed['status'] = 'active' if processed['is_active'] else 'closed'

            return processed

        except Exception as e:
            logger.warning(f"Error processing event: {e}")
            return event

    # ========== Core API Methods ==========

    async def fetch_markets(self, limit: int = 20, **filters) -> Dict[str, Any]:
        """Fetch real current markets from Gamma API."""
        try:
            logger.info(f"Fetching markets from Gamma API with limit: {limit}")

            # Build Gamma API parameters
            params = {}

            # Handle search filter
            if 'search' in filters:
                # Gamma API doesn't have direct text search, so we'll filter after
                pass

            # Handle active filter
            if filters.get('active_only'):
                params['active'] = True
                params['closed'] = False
                params['archived'] = False

            # Handle category filter
            if 'category' in filters:
                params['tag'] = filters['category']

            # Fetch markets
            raw_markets = await self._fetch_gamma_markets(limit, **params)

            # Process markets
            markets = [self._process_gamma_market(m) for m in raw_markets]

            # Apply post-fetch filters
            if 'search' in filters:
                search_term = filters['search'].lower()
                markets = [
                    m for m in markets
                    if search_term in m.get('question', '').lower() or
                       search_term in m.get('description', '').lower()
                ]

            if 'min_volume' in filters:
                min_vol = float(filters['min_volume'])
                markets = [m for m in markets if m.get('volume_usd', 0) >= min_vol]

            response_data = {
                'data': markets[:limit],
                'count': len(markets),
                'source': 'gamma_api',
                'filters_applied': filters
            }

            return self._safe_json_response(response_data)

        except Exception as e:
            logger.error(f"Error fetching markets: {e}")
            return self._safe_json_response(None, {
                "error": f"Markets fetch error: {str(e)}",
                "endpoint": "markets"
            })

    async def fetch_events(self, limit: int = 20, **filters) -> Dict[str, Any]:
        """Fetch real current events from Gamma API."""
        try:
            logger.info(f"Fetching events from Gamma API with limit: {limit}")

            params = {}

            if filters.get('active_only'):
                params['active'] = True
                params['closed'] = False
                params['archived'] = False

            if 'category' in filters:
                params['tag'] = filters['category']

            raw_events = await self._fetch_gamma_events(limit, **params)

            events = [self._process_gamma_event(e) for e in raw_events]

            # Apply search filter
            if 'search' in filters:
                search_term = filters['search'].lower()
                events = [
                    e for e in events
                    if search_term in e.get('title', '').lower() or
                       search_term in e.get('description', '').lower()
                ]

            if 'min_volume' in filters:
                min_vol = float(filters['min_volume'])
                events = [e for e in events if e.get('volume_usd', 0) >= min_vol]

            response_data = {
                'data': events[:limit],
                'count': len(events),
                'source': 'gamma_api',
                'filters_applied': filters
            }

            return self._safe_json_response(response_data)

        except Exception as e:
            logger.error(f"Error fetching events: {e}")
            return self._safe_json_response(None, {
                "error": f"Events fetch error: {str(e)}",
                "endpoint": "events"
            })

    async def fetch_market_by_id(self, market_id: str) -> Dict[str, Any]:
        """Fetch a single market by ID/slug."""
        try:
            sanitized_id = self._sanitize_string(market_id)
            logger.info(f"Fetching market by ID: {sanitized_id}")

            market = await self._fetch_gamma_market_by_slug(sanitized_id)

            if not market:
                return self._safe_json_response(None, {
                    "error": "Market not found",
                    "market_id": sanitized_id
                })

            processed_market = self._process_gamma_market(market)

            # Try to get recent trades
            try:
                trades = await self._fetch_recent_trades(10)
                processed_market['recent_trades'] = trades[:5]
            except:
                processed_market['recent_trades'] = []

            return self._safe_json_response(processed_market)

        except Exception as e:
            logger.error(f"Error fetching market {market_id}: {e}")
            return self._safe_json_response(None, {
                "error": f"Market fetch error: {str(e)}",
                "market_id": market_id
            })

    # ========== Search Function for MCP ==========

    async def search_polymarket_data(self, search_terms: str, limit: int = 20) -> Dict[str, Any]:
        """Search both events and markets - for MCP compatibility."""
        try:
            search_filters = {'search': search_terms, 'active_only': True}

            # Fetch both concurrently
            events_task = self.fetch_events(limit, **search_filters)
            markets_task = self.fetch_markets(limit, **search_filters)

            events_result, markets_result = await asyncio.gather(
                events_task, markets_task, return_exceptions=True
            )

            return self._safe_json_response({
                "events": events_result if not isinstance(events_result, Exception) else {"error": str(events_result)},
                "markets": markets_result if not isinstance(markets_result, Exception) else {
                    "error": str(markets_result)}
            })

        except Exception as e:
            logger.error(f"Error in search: {e}")
            return self._safe_json_response(None, {
                "error": f"Search error: {str(e)}",
                "search_terms": search_terms
            })

    # ========== Natural Language Processing ==========

    async def process_natural_query(self, query: str) -> Dict[str, Any]:
        """Process natural language queries for real market data."""
        try:
            sanitized_query = self._sanitize_string(query.strip())
            query_lower = sanitized_query.lower()

            logger.info(f"Processing natural query: {sanitized_query}")

            intent = self._classify_query_intent(query_lower)
            params = self._extract_query_parameters(query_lower)

            logger.info(f"Classified intent: {intent}, params: {params}")

            # Route to appropriate handler
            if intent == "events":
                result = await self.fetch_events(limit=params.get('limit', 20), **params.get('filters', {}))
            elif intent == "markets":
                result = await self.fetch_markets(limit=params.get('limit', 20), **params.get('filters', {}))
            elif intent in ["search", "trending", "recent", "general"]:
                # For all these intents, search both markets and events
                search_term = params.get('search_term', '')
                filters = params.get('filters', {})
                filters['active_only'] = True

                if intent == "trending":
                    filters['min_volume'] = 1000

                events_task = self.fetch_events(limit=params.get('limit', 10), **filters)
                markets_task = self.fetch_markets(limit=params.get('limit', 10), **filters)

                events_result, markets_result = await asyncio.gather(
                    events_task, markets_task, return_exceptions=True
                )

                result = self._safe_json_response({
                    "events": events_result if not isinstance(events_result, Exception) else {
                        "error": str(events_result)},
                    "markets": markets_result if not isinstance(markets_result, Exception) else {
                        "error": str(markets_result)}
                })

            # Add query metadata
            result["original_query"] = sanitized_query
            result["query_intent"] = intent
            result["extracted_params"] = params

            return result

        except Exception as e:
            logger.error(f"Error processing natural query '{query}': {e}")
            return self._safe_json_response(None, {
                "error": f"Query processing error: {str(e)}",
                "original_query": query,
                "query_type": "natural_language"
            })

    def _classify_query_intent(self, query: str) -> str:
        """Classify the intent of a natural language query."""
        if any(word in query for word in ["event", "events", "happening", "outcome"]):
            return "events"
        elif any(word in query for word in ["market", "markets", "betting", "odds", "price", "prediction"]):
            return "markets"
        elif any(word in query for word in ["trending", "popular", "hot", "most traded", "high volume"]):
            return "trending"
        elif any(word in query for word in ["recent", "latest", "new", "today", "current", "active"]):
            return "recent"
        elif any(word in query for word in ["search", "find", "looking for", "about"]):
            return "search"
        else:
            return "general"

    def _extract_query_parameters(self, query: str) -> Dict[str, Any]:
        """Extract parameters from natural language query."""
        params = {'filters': {}}

        # Extract limit/count
        limit_match = re.search(r'\b(\d+)\s*(?:events?|markets?|results?|items?)\b', query)
        if limit_match:
            params['limit'] = min(int(limit_match.group(1)), 100)
        elif 'few' in query:
            params['limit'] = 5
        elif 'many' in query or 'all' in query:
            params['limit'] = 50
        else:
            params['limit'] = 20

        # Extract search terms
        topics = [
            "crypto", "cryptocurrency", "bitcoin", "ethereum", "btc", "eth",
            "politics", "election", "political", "vote", "candidate", "trump", "biden",
            "sports", "game", "match", "team", "player", "nfl", "nba", "soccer",
            "weather", "climate", "ai", "tech", "technology", "stock", "finance"
        ]

        found_topics = [topic for topic in topics if topic in query]
        if found_topics:
            params['search_term'] = found_topics[0]
            params['filters']['search'] = found_topics[0]

        # Extract filters
        if any(word in query for word in ["active", "current", "ongoing"]):
            params['filters']['active_only'] = True

        return params


# Test function
async def test_real_apis():
    """Test the service with real Gamma API."""
    service = PolymarketService()

    try:
        logger.info("Testing real Polymarket APIs...")

        # Test Gamma API markets
        markets_result = await service.fetch_markets(5)
        if markets_result.get('success') and markets_result['data']['data']:
            markets = markets_result['data']['data']
            logger.info(f"Found {len(markets)} real markets from Gamma API")

            for market in markets[:2]:
                question = market.get('question', 'No question')[:100]
                volume = market.get('volume_usd', 0)
                status = market.get('status', 'unknown')
                logger.info(f"  - {question}... (${volume:,.2f}, {status})")
        else:
            logger.error(f"Failed to fetch markets: {markets_result.get('error')}")

        # Test natural language query
        query_result = await service.process_natural_query("show me recent crypto markets")
        if query_result.get('success'):
            logger.info("Natural language query processing works")
        else:
            logger.error(f"Natural query failed: {query_result.get('error')}")

    except Exception as e:
        logger.error(f"Test failed with exception: {e}")
    finally:
        await service.close()


if __name__ == "__main__":
    asyncio.run(test_real_apis())