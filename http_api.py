from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
from pydantic import BaseModel
import asyncio
import uvicorn
from typing import Optional, Dict, Any
from agent.data_fetcher import PolymarketService

app = FastAPI(
    title="Polymarket Agent API",
    description="HTTP API for Polymarket prediction market data",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize service
service = PolymarketService()


# Request models
class QueryRequest(BaseModel):
    query: str


class SearchRequest(BaseModel):
    search_terms: str
    limit: int = 20


class MarketRequest(BaseModel):
    limit: int = 20
    search: Optional[str] = None


# Response models
class APIResponse(BaseModel):
    success: bool
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


@app.get("/")
async def root():
    """API root endpoint"""
    return {
        "name": "Polymarket Agent API",
        "description": "Natural language access to Polymarket prediction market data",
        "version": "1.0.0",
        "endpoints": {
            "query": "/query - Natural language queries",
            "markets": "/markets - Get prediction markets",
            "events": "/events - Get prediction events",
            "search": "/search - Search markets and events",
            "health": "/health - Health check"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "polymarket-agent"}


@app.post("/query", response_model=APIResponse)
async def query_polymarket(request: QueryRequest):
    """Process natural language queries about Polymarket data"""
    try:
        result = await service.process_natural_query(request.query)
        return APIResponse(success=True, data={"query": request.query, "result": result})
    except Exception as e:
        return APIResponse(success=False, error=str(e))


@app.post("/markets", response_model=APIResponse)
async def get_markets(request: MarketRequest):
    """Get Polymarket markets with optional filtering"""
    try:
        result = await service.fetch_markets(limit=request.limit)
        if request.search:
            result = service._filter_by_search_terms(result, [request.search])
        return APIResponse(success=True, data=result)
    except Exception as e:
        return APIResponse(success=False, error=str(e))


@app.post("/events", response_model=APIResponse)
async def get_events(request: MarketRequest):
    """Get Polymarket events with optional filtering"""
    try:
        result = await service.fetch_events(limit=request.limit)
        if request.search:
            result = service._filter_by_search_terms(result, [request.search])
        return APIResponse(success=True, data=result)
    except Exception as e:
        return APIResponse(success=False, error=str(e))


@app.post("/search", response_model=APIResponse)
async def search_polymarket_data(request: SearchRequest):
    """Search across both events and markets"""
    try:
        events = await service.fetch_events(limit=request.limit)
        markets = await service.fetch_markets(limit=request.limit)

        search_terms = request.search_terms.split()
        filtered_events = service._filter_by_search_terms(events, search_terms)
        filtered_markets = service._filter_by_search_terms(markets, search_terms)

        return APIResponse(
            success=True,
            data={
                "search_terms": request.search_terms,
                "events": filtered_events,
                "markets": filtered_markets
            }
        )
    except Exception as e:
        return APIResponse(success=False, error=str(e))


# Analysis endpoints
@app.get("/analysis/crypto-summary")
async def crypto_markets_summary():
    """Get summary of crypto-related prediction markets"""
    try:
        markets = await service.fetch_markets(limit=50)
        crypto_markets = service._filter_by_search_terms(markets, ["crypto", "bitcoin", "ethereum"])
        return APIResponse(success=True, data={"crypto_markets": crypto_markets})
    except Exception as e:
        return APIResponse(success=False, error=str(e))


def load_resource(filename: str) -> str:
    """Load content from a resource file"""
    try:
        # Get the directory where the current script is located
        current_dir = Path(__file__).parent
        resource_path = current_dir / "resources" / filename

        # Check if file exists
        if not resource_path.exists():
            raise FileNotFoundError(f"Resource file not found: {filename}")

        # Read and return file content
        with open(resource_path, 'r', encoding='utf-8') as file:
            return file.read()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading resource: {str(e)}")


@app.get("/templates/analysis")
async def get_analysis_template():
    """Get market analysis template"""
    try:
        template_content = load_resource("analysis-template.md")
        return APIResponse(success=True, data={"template": template_content})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load analysis template: {str(e)}")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)