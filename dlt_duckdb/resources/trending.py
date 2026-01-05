import dlt
from utils.functions import fetch_data

# From 'trending movies' endpoint
@dlt.resource(name="trending_movies", write_disposition="replace")
def themoviedb_trending_movies_resource(api_secret_key: str = dlt.secrets.value):
    url = "https://api.themoviedb.org/3/trending/movie/day"
    params = {
        "language": "en-US"
    }
    yield from fetch_data(api_secret_key, url, params)
