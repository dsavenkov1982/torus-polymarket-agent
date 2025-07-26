# agent/data_fetcher.py
"""
Clean Polymarket service with natural language query processing.
Pure data service - no HTTP endpoints, just API calls and NL processing.
"""

import httpx
import asyncio
import re
from typing import List, Dict, Any, Optional


class PolymarketService:
    """Polymarket service with natural language query processing."""

    BASE_URL = "https://gamma-api.polymarket.com"

    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30.0)

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()

    # ========== Core API Methods ==========

    async def fetch_events(self, limit: int = 20, **filters) -> Dict[str, Any]:
        """Fetch Polymarket events with optional filters."""
        params = {"limit": limit}
        params.update(filters)

        response = await self.client.get(f"{self.BASE_URL}/events", params=params)
        response.raise_for_status()
        return response.json()

    async def fetch_event_by_id(self, event_id: str) -> Dict[str, Any]:
        """Fetch a single event by ID."""
        response = await self.client.get(f"{self.BASE_URL}/events/{event_id}")
        response.raise_for_status()
        return response.json()

    async def fetch_markets(self, limit: int = 20, **filters) -> Dict[str, Any]:
        """Fetch Polymarket markets with optional filters."""
        params = {"limit": limit}
        params.update(filters)

        response = await self.client.get(f"{self.BASE_URL}/markets", params=params)
        response.raise_for_status()
        return response.json()

    async def fetch_market_by_id(self, market_id: str) -> Dict[str, Any]:
        """Fetch a single market by ID."""
        response = await self.client.get(f"{self.BASE_URL}/markets/{market_id}")
        response.raise_for_status()
        return response.json()

    # ========== Natural Language Processing ==========

    async def process_natural_query(self, query: str) -> Dict[str, Any]:
        """
        Process natural language queries and convert to API calls.
        This is the core "LLM-like transformation" capability.
        """
        query_lower = query.lower().strip()

        # Determine query intent and extract parameters
        intent = self._classify_query_intent(query_lower)
        params = self._extract_query_parameters(query_lower)

        # Route to appropriate handler based on intent
        if intent == "events":
            return await self._handle_events_query(params)
        elif intent == "markets":
            return await self._handle_markets_query(params)
        elif intent == "search":
            return await self._handle_search_query(params)
        elif intent == "trending":
            return await self._handle_trending_query(params)
        elif intent == "recent":
            return await self._handle_recent_query(params)
        else:
            return await self._handle_general_query(query, params)

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
            params['limit'] = int(limit_match.group(1))
        elif 'few' in query:
            params['limit'] = 5
        elif 'many' in query or 'all' in query:
            params['limit'] = 100
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
            params['search_terms'] = search_terms

        return params

    # ========== Query Handlers ==========

    async def _handle_events_query(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle event-specific queries."""
        result = await self.fetch_events(limit=params.get('limit', 20))

        # Apply search term filtering if needed
        if params.get('search_terms'):
            result = self._filter_by_search_terms(result, params['search_terms'])

        return {
            "query_type": "events",
            "processed_params": params,
            "data": result
        }

    async def _handle_markets_query(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle market-specific queries."""
        result = await self.fetch_markets(limit=params.get('limit', 20))

        # Apply search term filtering if needed
        if params.get('search_terms'):
            result = self._filter_by_search_terms(result, params['search_terms'])

        return {
            "query_type": "markets",
            "processed_params": params,
            "data": result
        }

    async def _handle_search_query(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle search queries - fetch both events and markets."""
        # Fetch both events and markets concurrently
        events_task = self.fetch_events(limit=params.get('limit', 10))
        markets_task = self.fetch_markets(limit=params.get('limit', 10))

        events_result, markets_result = await asyncio.gather(events_task, markets_task)

        # Apply search term filtering to both
        if params.get('search_terms'):
            events_result = self._filter_by_search_terms(events_result, params['search_terms'])
            markets_result = self._filter_by_search_terms(markets_result, params['search_terms'])

        return {
            "query_type": "search",
            "processed_params": params,
            "data": {
                "events": events_result,
                "markets": markets_result
            }
        }

    async def _handle_trending_query(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle trending/popular queries."""
        limit = params.get('limit', 20)

        # Fetch recent data (in real implementation, you'd sort by volume/activity)
        events_result = await self.fetch_events(limit=limit)
        markets_result = await self.fetch_markets(limit=limit)

        return {
            "query_type": "trending",
            "processed_params": params,
            "data": {
                "trending_events": events_result,
                "trending_markets": markets_result
            }
        }

    async def _handle_recent_query(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle recent/latest queries."""
        limit = params.get('limit', 20)

        events_result = await self.fetch_events(limit=limit)
        markets_result = await self.fetch_markets(limit=limit)

        return {
            "query_type": "recent",
            "processed_params": params,
            "data": {
                "recent_events": events_result,
                "recent_markets": markets_result
            }
        }

    async def _handle_general_query(self, original_query: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle general queries that don't fit specific categories."""
        # Default to searching both events and markets
        events_task = self.fetch_events(limit=params.get('limit', 10))
        markets_task = self.fetch_markets(limit=params.get('limit', 10))

        events_result, markets_result = await asyncio.gather(events_task, markets_task)

        # Apply search term filtering
        if params.get('search_terms'):
            events_result = self._filter_by_search_terms(events_result, params['search_terms'])
            markets_result = self._filter_by_search_terms(markets_result, params['search_terms'])

        return {
            "query_type": "general",
            "original_query": original_query,
            "processed_params": params,
            "data": {
                "events": events_result,
                "markets": markets_result
            }
        }

    def _filter_by_search_terms(self, data: Dict[str, Any], search_terms: List[str]) -> Dict[str, Any]:
        """Filter data based on search terms."""
        if not search_terms or not data:
            return data

        # Create a copy to avoid modifying original
        filtered_data = data.copy()

        # Filter items based on search terms
        if isinstance(data, dict) and 'data' in data:
            items = data['data']
            if isinstance(items, list):
                filtered_items = []
                for item in items:
                    # Convert item to string for searching
                    item_text = str(item).lower()
                    # Check if any search term appears in the item
                    if any(term.lower() in item_text for term in search_terms):
                        filtered_items.append(item)

                filtered_data['data'] = filtered_items
                filtered_data['filtered_count'] = len(filtered_items)
                filtered_data['original_count'] = len(items)

        return filtered_data

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
                "regex_based_filtering"
            ],
            "supported_intents": [
                "events", "markets", "search", "trending", "recent", "general"
            ],
            "supported_topics": [
                "crypto", "politics", "sports", "weather", "ai", "technology"
            ]
        }