# --- Imports ---
import backoff   # adds automatic retry-with-backoff to functions
import logging   # records what the code is doing (a diary instead of print())
import httpx     # the HTTP client used to call the API

# --- Endpoint paths stored as constants ---
# One source of truth: change a path here once, not in every notebook.
# Avoids typos and keeps API logic in one place.
HEALTH_CHECK_ENDPOINT = "/"
LIST_LEAGUES_ENDPOINT = "/v0/leagues/"
LIST_PLAYERS_ENDPOINT = "/v0/players/"
LIST_PERFORMANCES_ENDPOINT = "/v0/performances/"
LIST_TEAMS_ENDPOINT = "/v0/teams/"
LIST_WEEKS_ENDPOINT = "/v0/weeks/"
GET_COUNTS_ENDPOINT = "/v0/counts/"

# Create a logger named after this file (__name__), so log messages
# are tagged with where they came from. Useful for debugging.
logger = logging.getLogger(__name__)


# --- Decorator: adds reliability to the function below ---
# If the call fails, retry automatically instead of giving up or
# hammering the server.
@backoff.on_exception(
    wait_gen=backoff.expo,      # wait longer each retry: ~1s, 2s, 4s...
    exception=(                 # only retry on these error types:
        httpx.RequestError,        # couldn't reach the server
        httpx.HTTPStatusError      # server answered with an error code
    ),
    max_time=5,                 # stop retrying after 5 seconds total
    jitter=backoff.random_jitter  # add randomness so many clients don't
                                  # all retry at the exact same instant
)
def call_api_endpoint(
    base_url: str,              # the API's base address (your github.dev URL)
    api_endpoint: str,          # which endpoint to call (e.g. "/v0/leagues/")
    api_params: dict = None     # optional query parameters; defaults to none
) -> httpx.Response:            # this function returns an httpx Response object
    try:
        # Open a client as a context manager: it guarantees the connection
        # is cleaned up when the block ends, even if an error happens.
        with httpx.Client(base_url=base_url) as client:
            logger.debug(f"base_url: {base_url}, api_endpoint: {api_endpoint}")

            # Send the GET request to fetch data.
            response = client.get(api_endpoint, params=api_params)

            # If the server returned an error code (404, 500, etc.),
            # turn it into a Python error we can catch below.
            response.raise_for_status()

            logger.debug(f"Response JSON: {response.json()}")
            return response

    # Server was reached, but replied with an error status (e.g. 404, 500).
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP status error occurred: {e.response.text}")
        return httpx.Response(
            status_code=e.response.status_code,
            content=b"API error"
        )

    # Couldn't even reach the server (network down, bad address, etc.).
    except httpx.RequestError as e:
        logger.error(f"Request error occurred: {str(e)}")
        return httpx.Response(status_code=500, content=b"Network error")

    # Catch-all safety net for anything unexpected.
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}")
        return httpx.Response(status_code=500, content=b"Unexpected error")