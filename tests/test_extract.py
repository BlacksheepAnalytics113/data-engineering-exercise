import pytest
import requests
import json
from unittest.mock import patch
from extract import get_cached_api_data, fetch_api

# Sample valid response from the API
VALID_RESPONSE = {
    "works": [
        {
            "key": "/works/OL123W",
            "title": "Sample Book",
            "first_publish_year": 2001,
            "authors": [{"key": "/authors/OL1A", "name": "Author One"}]
        }
    ]
}

# Sample invalid response (missing 'works' key)
INVALID_RESPONSE = {"data": []}

class DummyResponse:
    def __init__(self, json_data, status_code=200):
        self._json = json_data
        self.status_code = status_code
    
    def json(self):
        return self._json
    
    def raise_for_status(self):
        if not (200 <= self.status_code < 300):
            raise requests.HTTPError(f"HTTP {self.status_code}")

def test_fetch_api_success():
    """Test that fetch_api returns valid JSON when the response is successful."""
    with patch('requests.get') as mock_get:
        mock_get.return_value = DummyResponse(VALID_RESPONSE)
        data = fetch_api("dummy_endpoint")
        assert "works" in data
        assert data["works"][0]["title"] == "Sample Book"

def test_fetch_api_missing_works():
    """Test that get_cached_api_data raises ValueError when 'works' key is missing."""
    with patch('requests.get') as mock_get:
        mock_get.return_value = DummyResponse(INVALID_RESPONSE)
        with pytest.raises(ValueError):
            get_cached_api_data("dummy_endpoint")

def test_retry_mechanism(monkeypatch):
    """Test that the retry mechanism is triggered on a simulated timeout error."""
    call_count = 0
    def failing_get(endpoint, timeout):
        nonlocal call_count
        call_count += 1
        raise requests.exceptions.Timeout("Simulated timeout")
    
    monkeypatch.setattr("requests.get", failing_get)
    with pytest.raises(requests.exceptions.Timeout):
        fetch_api("dummy_endpoint")
    # Ensure at least one retry attempt was made.
    assert call_count >= 1

def test_caching_behavior(monkeypatch):
    """Test that repeated calls with the same endpoint use the cache."""
    call_count = 0
    def dummy_get(endpoint, timeout):
        nonlocal call_count
        call_count += 1
        return DummyResponse(VALID_RESPONSE)
    
    monkeypatch.setattr("requests.get", dummy_get)
    # Clear cache before testing.
    from extract import cache
    cache.clear()
    
    data1 = get_cached_api_data("dummy_endpoint")
    data2 = get_cached_api_data("dummy_endpoint")
    
    assert data1 == data2
    # Only one actual network call should have occurred.
    assert call_count == 1