import dlt
from resources import movies, trending

@dlt.source
def themoviedb_source(api_secret_key: str = dlt.secrets.value):
    yield movies.themoviedb_movies_resource(api_secret_key)
    
    yield dlt.resource(
        movies.themoviedb_movie_details_resource(api_secret_key), name="movie_details"
    )

    yield trending.themoviedb_trending_movies_resource(api_secret_key)
    yield trending.themoviedb_trending_tv_series_resource(api_secret_key)


if __name__ == "__main__":
    """
    Initializes and runs the data pipeline using the `dlt` library, fetching and processing data
    from The Movie Database API. The pipeline is configured to load data into a `duckdb` database
    """
    pipeline = dlt.pipeline(
        pipeline_name="themoviedb_pipeline",
        destination="duckdb",
        dataset_name="movie_data",
        progress="alive_progress",
    )
    pipeline.drop()
    load_info = pipeline.run(themoviedb_source())
    print(load_info)