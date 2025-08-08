import requests

class PolymarketSubgraphDiscovery:
    """Discover and test multiple Polymarket subgraphs."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })

        # Known subgraph IDs (we'll discover more)
        self.known_subgraphs = {
            "main": "Bx1W4S7kDVxs9gC3s2G6DS8kdNBJNVhMviCtin2DiBp",
            # We'll add more as we find them
        }

    def get_subgraph_url(self, subgraph_id: str) -> str:
        """Get the full URL for a subgraph."""
        return f"https://gateway.thegraph.com/api/{self.api_key}/subgraphs/id/{subgraph_id}"

    def test_subgraph_schema(self, subgraph_id: str, name: str = None):
        """Test what entities are available in a subgraph."""
        print(f"\n{'=' * 60}")
        print(f"Testing Subgraph: {name or subgraph_id[:20]}...")
        print(f"ID: {subgraph_id}")
        print(f"{'=' * 60}")

        url = self.get_subgraph_url(subgraph_id)

        # Introspection query to discover schema
        introspection_query = """
        {
            __schema {
                queryType {
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
        }
        """

        try:
            response = self.session.post(url, json={"query": introspection_query})
            response.raise_for_status()

            data = response.json()

            if 'errors' in data:
                print(f"Errors: {data['errors']}")
                return None

            if 'data' in data and '__schema' in data['data']:
                schema = data['data']['__schema']
                query_fields = schema['queryType']['fields']

                print(f"Schema discovered! Found {len(query_fields)} query fields:")

                # Categorize fields
                entity_fields = []
                single_fields = []

                for field in query_fields:
                    field_name = field['name']
                    if field_name.endswith('s') and not field_name.startswith('_'):
                        entity_fields.append(field_name)
                    elif not field_name.startswith('_'):
                        single_fields.append(field_name)

                print(f"\nEntity Collections (plural):")
                for field in sorted(entity_fields)[:10]:  # Show first 10
                    print(f"{field}")

                print(f"\nSingle Entities:")
                for field in sorted(single_fields)[:10]:  # Show first 10
                    print(f"{field}")

                return {
                    'entity_fields': entity_fields,
                    'single_fields': single_fields,
                    'total_fields': len(query_fields)
                }
            else:
                print("No schema data found")
                return None

        except Exception as e:
            print(f"Error testing subgraph: {e}")
            return None

    def test_sample_data(self, subgraph_id: str, entity_name: str, name: str = None):
        """Test sample data from a specific entity."""
        print(f"\nTesting Sample Data: {entity_name} from {name or 'subgraph'}")

        url = self.get_subgraph_url(subgraph_id)

        # Generic query for any entity
        query = f"""
        {{
            {entity_name}(first: 3) {{
                id
            }}
        }}
        """

        try:
            response = self.session.post(url, json={"query": query})
            response.raise_for_status()

            data = response.json()

            if 'errors' in data:
                print(f"Errors: {data['errors']}")
                return None

            if 'data' in data and entity_name in data['data']:
                entities = data['data'][entity_name]
                print(f"Found {len(entities)} {entity_name}")

                if entities:
                    print(f"Sample IDs:")
                    for i, entity in enumerate(entities, 1):
                        entity_id = entity.get('id', 'No ID')
                        print(f"  {i}. {entity_id}")

                return entities
            else:
                print(f"No {entity_name} found")
                return None

        except Exception as e:
            print(f"Error testing {entity_name}: {e}")
            return None

    def search_graph_network(self):
        """Search for Polymarket subgraphs using Graph Network API."""
        print(f"\n{'=' * 60}")
        print("Searching Graph Network for Polymarket subgraphs...")
        print(f"{'=' * 60}")

        # The Graph Network API endpoint for searching subgraphs
        search_url = "https://api.thegraph.com/graphql"

        # Search query for Polymarket subgraphs
        search_query = """
        query SearchSubgraphs($text: String!) {
            subgraphSearch(text: $text, first: 20) {
                id
                displayName
                description
                currentVersion {
                    id
                    subgraphDeployment {
                        id
                        deniedAt
                    }
                }
                owner {
                    id
                }
            }
        }
        """

        try:
            response = self.session.post(
                search_url,
                json={
                    "query": search_query,
                    "variables": {"text": "polymarket"}
                }
            )
            response.raise_for_status()

            data = response.json()

            if 'errors' in data:
                print(f"Search errors: {data['errors']}")
                return []

            if 'data' in data and 'subgraphSearch' in data['data']:
                subgraphs = data['data']['subgraphSearch']
                print(f"Found {len(subgraphs)} Polymarket-related subgraphs:")

                discovered_subgraphs = []

                for i, subgraph in enumerate(subgraphs, 1):
                    name = subgraph.get('displayName', 'Unknown')
                    description = subgraph.get('description', 'No description')[:100]
                    subgraph_id = subgraph.get('id', 'No ID')
                    owner = subgraph.get('owner', {}).get('id', 'Unknown')

                    # Get deployment ID if available
                    deployment_id = None
                    if subgraph.get('currentVersion') and subgraph['currentVersion'].get('subgraphDeployment'):
                        deployment_id = subgraph['currentVersion']['subgraphDeployment'].get('id')

                    print(f"\n#{i}: {name}")
                    print(f"Description: {description}")
                    print(f"Owner: {owner}")
                    print(f"Subgraph ID: {subgraph_id}")
                    if deployment_id:
                        print(f"Deployment ID: {deployment_id}")

                    discovered_subgraphs.append({
                        'name': name,
                        'id': subgraph_id,
                        'deployment_id': deployment_id,
                        'description': description,
                        'owner': owner
                    })

                return discovered_subgraphs
            else:
                print("No search results found")
                return []

        except Exception as e:
            print(f"Error searching: {e}")
            return []

    def test_known_ids(self):
        """Test some common Polymarket subgraph ID patterns."""
        print(f"\n{'=' * 60}")
        print("Testing Known/Common Polymarket Subgraph IDs...")
        print(f"{'=' * 60}")

        # These are educated guesses based on common patterns
        potential_ids = [
            # Main one we know works
            ("Main Polymarket", "Bx1W4S7kDVxs9gC3s2G6DS8kdNBJNVhMviCtin2DiBp"),

            # Common patterns for other versions/networks
            ("Polymarket V2", "2rWm8uGZhQhLG8wr1f9WF6F5eBKrDy8gvFpT9nqDhPvr"),
            ("Polymarket Activity", "3sXn9tGAhQiMH9xs2g7XG7G6fCLsDz9huGqU0oqEhQws"),
            ("Polymarket Analytics", "4tYo0uHBhRjNI0yt3h8YH8H7gDMtE0ikvHrV1prFiRxt"),

            # Try some variations
            ("Polymarket Polygon", "5uZp1vIChSkOJ1zu4i9ZI9I8hENuF1jlwIsW2qsGjSyu"),
            ("Polymarket Markets", "6vAq2wJDhTlPK2Av5j0AJ0J9iFOvG2klxJtX3rtHkTzv"),
        ]

        working_subgraphs = []

        for name, subgraph_id in potential_ids:
            schema_info = self.test_subgraph_schema(subgraph_id, name)
            if schema_info:
                working_subgraphs.append({
                    'name': name,
                    'id': subgraph_id,
                    'schema': schema_info
                })

        return working_subgraphs

    def comprehensive_discovery(self):
        """Run a comprehensive discovery of Polymarket subgraphs."""
        print("Starting Comprehensive Polymarket Subgraph Discovery")
        print("=" * 70)

        all_discovered = []

        # Method 1: Test known working subgraph
        print("\nStep 1: Testing Known Working Subgraph")
        main_schema = self.test_subgraph_schema(
            self.known_subgraphs["main"],
            "Main Polymarket (Known Working)"
        )
        if main_schema:
            all_discovered.append({
                'name': 'Main Polymarket',
                'id': self.known_subgraphs["main"],
                'schema': main_schema,
                'status': 'working'
            })

        # Method 2: Search the network
        print("\nStep 2: Searching Graph Network")
        network_results = self.search_graph_network()

        # Method 3: Test common ID patterns
        print("\nStep 3: Testing Common ID Patterns")
        pattern_results = self.test_known_ids()

        # Combine results
        all_discovered.extend(pattern_results)

        # Summary
        print(f"\n{'=' * 70}")
        print("DISCOVERY SUMMARY")
        print(f"{'=' * 70}")

        working_count = len([s for s in all_discovered if s.get('status') == 'working' or s.get('schema')])

        print(f"Working subgraphs found: {working_count}")
        print(f"Network search results: {len(network_results)}")
        print(f"Total discovered: {len(all_discovered)}")

        if working_count > 0:
            print(f"\nWorking Subgraphs:")
            for subgraph in all_discovered:
                if subgraph.get('schema'):
                    name = subgraph['name']
                    entity_count = len(subgraph['schema'].get('entity_fields', []))
                    print(f"{name}: {entity_count} entity types")

        return all_discovered


def main():
    """Main discovery function."""

    # Use your API key
    api_key = "75d40427e4676b50425802db7503d97f"

    print("Polymarket Subgraph Discovery Tool")
    print(f"Using API key: {api_key[:10]}...")

    discoverer = PolymarketSubgraphDiscovery(api_key)

    try:
        results = discoverer.comprehensive_discovery()

        print(f"\n{'=' * 70}")
        print("Discovery Complete!")
        print("Use the working subgraph IDs to get different types of data")
        print(f"{'=' * 70}")

    except Exception as e:
        print(f"\nDiscovery failed: {e}")


if __name__ == "__main__":
    main()