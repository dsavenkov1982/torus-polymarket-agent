# agent/data_fetcher.py
"""
Clean Polymarket service with natural language query processing.
Pure data service - no HTTP endpoints, just API calls and NL processing.
Fixed for JSON serialization issues.
"""

import httpx
import asyncio
import re
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PolymarketService:
    """Polymarket service with natural language query processing."""

    BASE_URL = "https://gamma-api.polymarket.com"

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

        # Remove or replace problematic characters
        # Replace smart quotes and other Unicode characters using Unicode codes
        replacements = {
            '\u201c': '"',  # Smart quote left
            '\u201d': '"',  # Smart quote right
            '\u2018': "'",  # Smart apostrophe left
            '\u2019': "'",  # Smart apostrophe right
            '\u2013': '-',  # En dash
            '\u2014': '-',  # Em dash
            '\u2026': '...',  # Ellipsis
            '\u00a0': ' ',  # Non-breaking space
            '\u00ab': '"',  # Left-pointing double angle quotation mark
            '\u00bb': '"',  # Right-pointing double angle quotation mark
        }

        for old, new in replacements.items():
            text = text.replace(old, new)

        # Remove any remaining non-printable characters except newlines and tabs
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
        elif hasattr(data, 'isoformat'):  # datetime objects
            return data.isoformat()
        else:
            # Convert unknown types to string
            return str(data)

    def _safe_json_response(self, data: Any, query_info: Optional[Dict] = None) -> Dict[str, Any]:
        """Create a JSON-safe response wrapper."""
        try:
            # Sanitize the data first
            sanitized_data = self._sanitize_data(data)

            # Test JSON serialization
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
            logger.error(f"Problematic data type: {type(data)}")

            # Return error response with safe fallback
            return {
                "success": False,
                "error": f"Data serialization error: {str(e)}",
                "error_type": type(e).__name__,
                "timestamp": datetime.now().isoformat(),
                "data_type": str(type(data)),
                "data_preview": str(data)[:200] if data else None,
                **(query_info or {})
            }

    # ========== Core API Methods ==========

    async def fetch_events(self, limit: int = 20, **filters) -> Dict[str, Any]:
        """Fetch Polymarket events with optional filters."""
        try:
            params = {"limit": min(limit, 100)}  # Cap limit to prevent issues
            params.update(filters)

            logger.info(f"Fetching events with params: {params}")

            response = await self.client.get(f"{self.BASE_URL}/events", params=params)
            response.raise_for_status()

            raw_data = response.json()
            return self._safe_json_response(raw_data)

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching events: {e}")
            return self._safe_json_response(None, {
                "error": f"HTTP {e.response.status_code}: {e.response.text}",
                "endpoint": "events"
            })
        except Exception as e:
            logger.error(f"Unexpected error fetching events: {e}")
            return self._safe_json_response(None, {
                "error": f"Unexpected error: {str(e)}",
                "endpoint": "events"
            })

    async def fetch_event_by_id(self, event_id: str) -> Dict[str, Any]:
        """Fetch a single event by ID."""
        try:
            sanitized_id = self._sanitize_string(event_id)
            logger.info(f"Fetching event by ID: {sanitized_id}")

            response = await self.client.get(f"{self.BASE_URL}/events/{sanitized_id}")
            response.raise_for_status()

            raw_data = response.json()
            return self._safe_json_response(raw_data)

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching event {event_id}: {e}")
            return self._safe_json_response(None, {
                "error": f"HTTP {e.response.status_code}: {e.response.text}",
                "endpoint": f"events/{event_id}"
            })
        except Exception as e:
            logger.error(f"Unexpected error fetching event {event_id}: {e}")
            return self._safe_json_response(None, {
                "error": f"Unexpected error: {str(e)}",
                "endpoint": f"events/{event_id}"
            })

    async def fetch_markets(self, limit: int = 20, **filters) -> Dict[str, Any]:
        """Fetch Polymarket markets with optional filters."""
        try:
            params = {"limit": min(limit, 100)}  # Cap limit to prevent issues
            params.update(filters)

            logger.info(f"Fetching markets with params: {params}")

            response = await self.client.get(f"{self.BASE_URL}/markets", params=params)
            response.raise_for_status()

            raw_data = response.json()
            return self._safe_json_response(raw_data)

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching markets: {e}")
            return self._safe_json_response(None, {
                "error": f"HTTP {e.response.status_code}: {e.response.text}",
                "endpoint": "markets"
            })
        except Exception as e:
            logger.error(f"Unexpected error fetching markets: {e}")
            return self._safe_json_response(None, {
                "error": f"Unexpected error: {str(e)}",
                "endpoint": "markets"
            })

    async def fetch_market_by_id(self, market_id: str) -> Dict[str, Any]:
        """Fetch a single market by ID."""
        try:
            sanitized_id = self._sanitize_string(market_id)
            logger.info(f"Fetching market by ID: {sanitized_id}")

            response = await self.client.get(f"{self.BASE_URL}/markets/{sanitized_id}")
            response.raise_for_status()

            raw_data = response.json()
            return self._safe_json_response(raw_data)

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching market {market_id}: {e}")
            return self._safe_json_response(None, {
                "error": f"HTTP {e.response.status_code}: {e.response.text}",
                "endpoint": f"markets/{market_id}"
            })
        except Exception as e:
            logger.error(f"Unexpected error fetching market {market_id}: {e}")
            return self._safe_json_response(None, {
                "error": f"Unexpected error: {str(e)}",
                "endpoint": f"markets/{market_id}"
            })

    # ========== Natural Language Processing ==========

    async def process_natural_query(self, query: str) -> Dict[str, Any]:
        """
        Process natural language queries and convert to API calls.
        This is the core "LLM-like transformation" capability.
        """
        try:
            sanitized_query = self._sanitize_string(query.strip())
            query_lower = sanitized_query.lower()

            logger.info(f"Processing natural query: {sanitized_query}")

            # Determine query intent and extract parameters
            intent = self._classify_query_intent(query_lower)
            params = self._extract_query_parameters(query_lower)

            logger.info(f"Classified intent: {intent}, params: {params}")

            # Route to appropriate handler based on intent
            if intent == "events":
                result = await self._handle_events_query(params)
            elif intent == "markets":
                result = await self._handle_markets_query(params)
            elif intent == "search":
                result = await self._handle_search_query(params)
            elif intent == "trending":
                result = await self._handle_trending_query(params)
            elif intent == "recent":
                result = await self._handle_recent_query(params)
            else:
                result = await self._handle_general_query(sanitized_query, params)

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
        """Classify the intent of a natural language query using regex patterns."""

        # Event-related keywords
        if any(word in query for word in ["event", "events", "happening", "outcome"]):
            return "events"

        # Market-related keywords
        if any(word in query for word in ["market", "markets", "betting", "odds", "price"]):
            return "markets"

        # Search keywords
        if any(word in query for word in ["search", "find", "looking for", "about"]):
            return "search"

        # Trending keywords
        if any(word in query for word in ["trending", "popular", "hot", "most traded"]):
            return "trending"

        # Recent/time-based keywords
        if any(word in query for word in ["recent", "latest", "new", "today", "yesterday"]):
            return "recent"

        return "general"

    def _extract_query_parameters(self, query: str) -> Dict[str, Any]:
        """Extract parameters from natural language query using regex."""
        params = {}

        # Extract limit/count using regex
        limit_match = re.search(r'\b(\d+)\s*(?:events?|markets?|results?|items?)\b', query)
        if limit_match:
            params['limit'] = min(int(limit_match.group(1)), 100)  # Cap at 100
        elif 'few' in query:
            params['limit'] = 5
        elif 'many' in query or 'all' in query:
            params['limit'] = 50  # Reduced from 100 to prevent issues
        else:
            params['limit'] = 20

        # Extract topic keywords
        topics = [
            "crypto", "cryptocurrency", "bitcoin", "ethereum", "btc", "eth",
            "politics", "election", "political", "vote", "candidate",
            "sports", "game", "match", "team", "player",
            "weather", "climate", "temperature", "storm",
            "ai", "artificial intelligence", "machine learning", "tech", "technology"
        ]

        found_topics = [topic for topic in topics if topic in query]
        if found_topics:
            params['topics'] = found_topics

        # Extract time-related parameters
        if any(word in query for word in ["today", "daily"]):
            params['time_filter'] = 'today'
        elif any(word in query for word in ["week", "weekly"]):
            params['time_filter'] = 'week'
        elif any(word in query for word in ["month", "monthly"]):
            params['time_filter'] = 'month'

        # Extract search terms (filter out stop words)
        stop_words = {
            'the', 'is', 'at', 'which', 'on', 'and', 'or', 'but', 'in', 'with',
            'a', 'an', 'as', 'are', 'was', 'be', 'been', 'have', 'has', 'had',
            'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might',
            'get', 'show', 'find', 'search', 'look', 'for', 'about', 'me', 'i', 'you',
            'this', 'that', 'these', 'those', 'what', 'where', 'when', 'why', 'how'
        }

        # Extract meaningful words
        words = re.findall(r'\b\w+\b', query.lower())
        search_terms = [word for word in words if word not in stop_words and len(word) > 2]

        if search_terms:
            params['search_terms'] = search_terms[:10]  # Limit search terms

        return params

    # ========== Query Handlers ==========

    async def _handle_events_query(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle event-specific queries."""
        result = await self.fetch_events(limit=params.get('limit', 20))

        # Apply search term filtering if needed and if fetch was successful
        if result.get('success') and params.get('search_terms'):
            result = self._filter_by_search_terms(result, params['search_terms'])

        # Add query metadata
        result["query_type"] = "events"
        result["processed_params"] = params

        return result

    async def _handle_markets_query(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle market-specific queries."""
        result = await self.fetch_markets(limit=params.get('limit', 20))

        # Apply search term filtering if needed and if fetch was successful
        if result.get('success') and params.get('search_terms'):
            result = self._filter_by_search_terms(result, params['search_terms'])

        # Add query metadata
        result["query_type"] = "markets"
        result["processed_params"] = params

        return result

    async def _handle_search_query(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle search queries - fetch both events and markets."""
        limit = params.get('limit', 10)

        # Fetch both events and markets concurrently
        events_task = self.fetch_events(limit=limit)
        markets_task = self.fetch_markets(limit=limit)

        try:
            events_result, markets_result = await asyncio.gather(
                events_task, markets_task, return_exceptions=True
            )

            # Handle exceptions in results
            if isinstance(events_result, Exception):
                logger.error(f"Events fetch failed: {events_result}")
                events_result = self._safe_json_response(None, {"error": str(events_result)})

            if isinstance(markets_result, Exception):
                logger.error(f"Markets fetch failed: {markets_result}")
                markets_result = self._safe_json_response(None, {"error": str(markets_result)})

            # Apply search term filtering to both if successful
            if events_result.get('success') and params.get('search_terms'):
                events_result = self._filter_by_search_terms(events_result, params['search_terms'])

            if markets_result.get('success') and params.get('search_terms'):
                markets_result = self._filter_by_search_terms(markets_result, params['search_terms'])

            return self._safe_json_response({
                "events": events_result,
                "markets": markets_result
            }, {
                "query_type": "search",
                "processed_params": params
            })

        except Exception as e:
            logger.error(f"Error in search query handler: {e}")
            return self._safe_json_response(None, {
                "error": f"Search handler error: {str(e)}",
                "query_type": "search",
                "processed_params": params
            })

    async def _handle_trending_query(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle trending/popular queries."""
        limit = params.get('limit', 20)

        try:
            # Fetch recent data (in real implementation, you'd sort by volume/activity)
            events_task = self.fetch_events(limit=limit)
            markets_task = self.fetch_markets(limit=limit)

            events_result, markets_result = await asyncio.gather(
                events_task, markets_task, return_exceptions=True
            )

            # Handle exceptions in results
            if isinstance(events_result, Exception):
                events_result = self._safe_json_response(None, {"error": str(events_result)})

            if isinstance(markets_result, Exception):
                markets_result = self._safe_json_response(None, {"error": str(markets_result)})

            return self._safe_json_response({
                "trending_events": events_result,
                "trending_markets": markets_result
            }, {
                "query_type": "trending",
                "processed_params": params
            })

        except Exception as e:
            logger.error(f"Error in trending query handler: {e}")
            return self._safe_json_response(None, {
                "error": f"Trending handler error: {str(e)}",
                "query_type": "trending",
                "processed_params": params
            })

    async def _handle_recent_query(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle recent/latest queries."""
        limit = params.get('limit', 20)

        try:
            events_task = self.fetch_events(limit=limit)
            markets_task = self.fetch_markets(limit=limit)

            events_result, markets_result = await asyncio.gather(
                events_task, markets_task, return_exceptions=True
            )

            # Handle exceptions in results
            if isinstance(events_result, Exception):
                events_result = self._safe_json_response(None, {"error": str(events_result)})

            if isinstance(markets_result, Exception):
                markets_result = self._safe_json_response(None, {"error": str(markets_result)})

            return self._safe_json_response({
                "recent_events": events_result,
                "recent_markets": markets_result
            }, {
                "query_type": "recent",
                "processed_params": params
            })

        except Exception as e:
            logger.error(f"Error in recent query handler: {e}")
            return self._safe_json_response(None, {
                "error": f"Recent handler error: {str(e)}",
                "query_type": "recent",
                "processed_params": params
            })

    async def _handle_general_query(self, original_query: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle general queries that don't fit specific categories."""
        limit = params.get('limit', 10)

        try:
            # Default to searching both events and markets
            events_task = self.fetch_events(limit=limit)
            markets_task = self.fetch_markets(limit=limit)

            events_result, markets_result = await asyncio.gather(
                events_task, markets_task, return_exceptions=True
            )

            # Handle exceptions in results
            if isinstance(events_result, Exception):
                events_result = self._safe_json_response(None, {"error": str(events_result)})

            if isinstance(markets_result, Exception):
                markets_result = self._safe_json_response(None, {"error": str(markets_result)})

            # Apply search term filtering if successful
            if events_result.get('success') and params.get('search_terms'):
                events_result = self._filter_by_search_terms(events_result, params['search_terms'])

            if markets_result.get('success') and params.get('search_terms'):
                markets_result = self._filter_by_search_terms(markets_result, params['search_terms'])

            return self._safe_json_response({
                "events": events_result,
                "markets": markets_result
            }, {
                "query_type": "general",
                "original_query": original_query,
                "processed_params": params
            })

        except Exception as e:
            logger.error(f"Error in general query handler: {e}")
            return self._safe_json_response(None, {
                "error": f"General handler error: {str(e)}",
                "query_type": "general",
                "original_query": original_query,
                "processed_params": params
            })

    def _filter_by_search_terms(self, data: Dict[str, Any], search_terms: List[str]) -> Dict[str, Any]:
        """Filter data based on search terms."""
        if not search_terms or not data or not data.get('success'):
            return data

        try:
            # Create a copy to avoid modifying original
            filtered_data = data.copy()

            # Filter items based on search terms
            if 'data' in filtered_data and isinstance(filtered_data['data'], dict):
                api_data = filtered_data['data']

                if 'data' in api_data and isinstance(api_data['data'], list):
                    items = api_data['data']
                    filtered_items = []

                    for item in items:
                        try:
                            # Convert item to string for searching (safely)
                            item_text = self._sanitize_string(str(item)).lower()
                            # Check if any search term appears in the item
                            if any(term.lower() in item_text for term in search_terms):
                                filtered_items.append(item)
                        except Exception as e:
                            logger.warning(f"Error filtering item: {e}")
                            # Include item if we can't filter it
                            filtered_items.append(item)

                    filtered_data['data']['data'] = filtered_items
                    filtered_data['filtered_count'] = len(filtered_items)
                    filtered_data['original_count'] = len(items)
                    filtered_data['search_terms'] = search_terms

            return filtered_data

        except Exception as e:
            logger.error(f"Error in filtering: {e}")
            # Return original data if filtering fails
            return data

    # ========== Schema Information ==========

    def get_schema(self) -> Dict[str, Any]:
        """Return schema information for MCP introspection."""
        return {
            "types": {
                "Event": {
                    "fields": [
                        "id", "title", "description", "start_date",
                        "end_date", "category", "volume", "liquidity"
                    ],
                    "description": "Polymarket prediction event"
                },
                "Market": {
                    "fields": [
                        "id", "question", "description", "outcomes",
                        "volume", "liquidity", "price", "probability"
                    ],
                    "description": "Polymarket prediction market"
                }
            },
            "queries": {
                "events": {
                    "description": "Fetch prediction events",
                    "parameters": ["limit", "category", "search", "time_filter"]
                },
                "markets": {
                    "description": "Fetch prediction markets",
                    "parameters": ["limit", "search", "sort_by", "time_filter"]
                },
                "natural_query": {
                    "description": "Process natural language queries",
                    "parameters": ["query"]
                },
                "search": {
                    "description": "Search across events and markets",
                    "parameters": ["search_terms", "limit"]
                }
            },
            "capabilities": [
                "natural_language_processing",
                "intent_classification",
                "parameter_extraction",
                "semantic_search",
                "multi_source_search",
                "regex_based_filtering",
                "json_sanitization",
                "error_handling"
            ],
            "supported_intents": [
                "events", "markets", "search", "trending", "recent", "general"
            ],
            "supported_topics": [
                "crypto", "politics", "sports", "weather", "ai", "technology"
            ]
        }


# Test function for debugging
async def test_service():
    """Test the service for common issues."""
    service = PolymarketService()

    try:
        logger.info("Testing PolymarketService...")

        # Test a simple query
        result = await service.process_natural_query("crypto markets")
        logger.info(f"Test result success: {result.get('success', False)}")

        # Test JSON serialization
        json.dumps(result, ensure_ascii=False, default=str)
        logger.info("JSON serialization successful")

    except Exception as e:
        logger.error(f"Test failed: {e}")
    finally:
        await service.close()


if __name__ == "__main__":
    asyncio.run(test_service())