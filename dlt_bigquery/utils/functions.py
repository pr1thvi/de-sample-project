import time

from dlt.sources.helpers import requests

MAX_RESULTS = 20
REQUESTS_PER_SECOND = 40

def _create_auth_headers(api_secret_key) -> dict[str, str]:
    """
    Creates the authorization headers required for making API requests to TMDB.

    Parameters: 
    :param api_secret_key (str): The API secret key used for authentication.

    Returns:
    - dict: A dictionary containing the 'accept' and 'Authorization' headers.
    """

    headers = {
        "accept":"application/json",
        "Authorization": f"Bearer {api_secret_key}"
    }

    return headers

def fetch_data(api_secret_key, url, params, page=1, results_key="results"):
    """
    Docstring for fetch_data Fetches data from the given TMDB API URL for a specific page.

    This function makes a single API request to a specific page.

    Parameters:
    :param api_secret_key (str): The API secret key for authentication.
    :param url (str): The TMDB API endpoint URL to fetch data from.
    :param params (dict): Parameters to include in the API request.
    :param page (int): The page number to fetch. Defaults to 1.

    Returns:
    - list: A list of results from the API for the requested page.
    """

    headers = _create_auth_headers(api_secret_key)
    params["page"] = page
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    time.sleep(1 / REQUESTS_PER_SECOND)
    return data.get(results_key, [])