import requests
import logging
import time
from tenacity import retry, wait_exponential, stop_after_attempt
from cachetools import cached, TTLCache

def before_retry(retry_state):
    """
    Callback function that logs details before a retry attempt.
    """
    endpoint = retry_state.args[0] if retry_state.args else "unknown endpoint"
    logging.warning(
        "Retry attempt %s for endpoint %s. Retrying in %.2f seconds.",
        retry_state.attempt_number,
        endpoint,
        retry_state.next_action.sleep
    )

@retry(
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(5),
    before_sleep=before_retry
)
def fetch_api(endpoint):
    """
    Fetches data from the given API endpoint.
    - Uses a timeout of 10 seconds.
    - Logs the time taken to complete the API call.
    - Raises an HTTPError if the request fails.
    """
    start_time = time.time()
    response = requests.get(endpoint, timeout=10)
    response.raise_for_status()  # Raise exception for HTTP errors (4XX/5XX)
    elapsed_time = time.time() - start_time
    logging.info("API call to %s took %.2f seconds.", endpoint, elapsed_time)
    return response.json()

# Set up a TTL (Time-To-Live) cache: caches up to 100 responses for 300 seconds.
cache = TTLCache(maxsize=100, ttl=300)

@cached(cache)
def get_cached_api_data(endpoint):
    """
    Fetches API data using caching to avoid redundant calls.
    - Logs the fetch attempt.
    - Uses the fetch_api function (with retry and timing).
    - Validates that the response contains the expected "works" key.
    
    Returns:
        The JSON response from the API if valid.
    """
    logging.info("Fetching data from endpoint: %s", endpoint)
    data = fetch_api(endpoint)
    
    if "works" not in data:
        logging.error("API response missing 'works' key.")
        raise ValueError("Expected key 'works' not found in API response.")
    
    logging.info("API call succeeded for endpoint: %s", endpoint)
    return data