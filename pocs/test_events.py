import requests

def test_events():
    url = "https://gamma-api.polymarket.com/events"
    response = requests.get(url)
    data = response.json()
    print("Fetched", len(data), "events")
    print(data[:1])  # Preview the first event

if __name__ == "__main__":
    test_events()
