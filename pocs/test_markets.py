import requests

def test_markets():
    url = "https://gamma-api.polymarket.com/markets"
    response = requests.get(url)
    data = response.json()
    print("Fetched", len(data), "markets")
    print(data[:1])  # Preview the first market

if __name__ == "__main__":
    test_markets()
