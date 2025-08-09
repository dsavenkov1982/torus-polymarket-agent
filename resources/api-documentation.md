# Polymarket API Documentation

## Base URL
https://gamma-api.polymarket.com

## Authentication
- No API key required for public endpoints
- Rate limit: 100 requests per minute

## Core Endpoints

### Events
**GET /events**
- Returns: List of prediction events
- Parameters:
  - limit: Number of events (default: 20, max: 100)
  - category: Filter by category
  - status: active, resolved, upcoming

**GET /events/{id}**
- Returns: Detailed event information
- Includes: Markets, outcomes, metadata

### Markets
**GET /markets**
- Returns: List of prediction markets
- Parameters:
  - limit: Number of markets
  - search: Search in market questions
  - sort_by: volume, liquidity, created_date

**GET /markets/{id}**
- Returns: Detailed market information
- Includes: Current prices, volume, order book

### Market Data
**GET /markets/{id}/orderbook**
- Returns: Current bid/ask orders
- Real-time pricing information

**GET /markets/{id}/trades**
- Returns: Recent trade history
- Parameters: limit, from_time, to_time

## Response Format
All responses follow this structure:
{
  "data": [...],
  "meta": {
    "total": 150,
    "page": 1,
    "limit": 20
  }
}

## Error Handling
- 400: Bad Request (invalid parameters)
- 404: Not Found (invalid ID)
- 429: Rate Limited
- 500: Server Error

## Rate Limiting
- Standard: 100 requests/minute
- Burst: 20 requests/second
- Headers: X-RateLimit-Remaining, X-RateLimit-Reset

## WebSocket API
- Real-time market updates
- Endpoint: wss://ws.polymarket.com
- Channels: market_updates, orderbook_changes

## Best Practices
1. Cache responses when possible
2. Handle rate limits gracefully
3. Use batch requests for multiple markets
4. Monitor WebSocket for real-time needs
5. Implement proper error handling

## Example Usage
import httpx

async def get_crypto_markets():
    async with httpx.AsyncClient() as client:
        response = await client.get(
            "https://gamma-api.polymarket.com/markets",
            params={"search": "crypto", "limit": 10}
        )
        return response.json()