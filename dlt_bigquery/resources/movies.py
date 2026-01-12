import time
from datetime import datetime

import dlt
from dlt.sources.helpers import requests
from utils.functions import _create_auth_headers, fetch_data

REQUESTS_PER_SECOND = 40

# From 'discover movies' endpoint
@dlt.resource(name="movies", write_disposition="replace", primary_key="id")
def themoviedb_movies_resource(api_secret_key: str = dlt.secrets.value):
    # State management
    state = dlt.current.resource_state()
    last_run_date_str = state.get('last_run_date', '1970-01-01')
    next_page = state.get('next_page', 1)
    
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    # Reset if new day
    if today_str != last_run_date_str:
        print(f"New day detected ({today_str} vs {last_run_date_str}). Resetting to page 1.")
        current_page = 1
        is_new_day = True
    else:
        print(f"Same day ({today_str}). Continuing from page {next_page}.")
        current_page = next_page
        is_new_day = False
        
    url = "https://api.themoviedb.org/3/discover/movie"
    params = {
        "include_adult": "false",
        "include_video": "false",
        "language": "en-US",
        "sort_by": "popularity.desc",
    }
    
    data = fetch_data(api_secret_key, url, params, page=current_page)
    
    # Update state
    state['last_run_date'] = today_str
    state['next_page'] = current_page + 1
    state['is_new_day'] = is_new_day
    
    # Yield individual items from the list
    for item in data:
        # Add metadata to track page and date
        item['_dlt_load_page'] = current_page
        item['_dlt_load_date'] = today_str
        yield item


# From 'movie details' endpoint - Converted to Transformer
@dlt.transformer(data_from=themoviedb_movies_resource, name="movie_details", write_disposition="append")
def themoviedb_movie_details_resource(movie_item, api_secret_key: str = dlt.secrets.value):
    # Check for new day to sync replace/append behavior logic could be complex here as it runs per item.
    # Simplified: We append always, but if the parent 'movies' replaced, we might want to replace too?
    # However, 'replace' on transformer input is tricky.
    # If the user wants to "start fresh daily", we should probably handle the write_disposition carefully.
    # For simplicity/safety in this iteration, we will default to append (keeping history of details).
    # If 'movies' table was replaced, 'movie_details' might contain details for movies no longer in 'movies' (orphans)
    # until the full set is repopulated. This is often acceptable in incremental loads.
    # If strict consistency is needed, we would need to coordinate. 
    # Let's assume append is fine for details or we trust dlt to handle write_disposition propagation? (It doesn't automatically propagate).
    
    # OPTION: We check state here too?!
    # Transformer creates its own resource state? Yes.
    
    headers = _create_auth_headers(api_secret_key)
    movie_id = movie_item["id"]
    url = f"https://api.themoviedb.org/3/movie/{movie_id}"
    params = {
        "append_to_response": "recommendations,reviews,similar,videos,images",
        "language": "en-US",
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    yield data
    time.sleep(1 / REQUESTS_PER_SECOND)
