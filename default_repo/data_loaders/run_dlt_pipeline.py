import sys
import os

# Change to the dlt_bigquery directory so dlt can find .dlt/secrets.toml
os.chdir('/home/src/dlt_bigquery')

# Add the dlt_bigquery directory to the Python path
sys.path.insert(0, '/home/src/dlt_bigquery')

import dlt
from resources import movies, trending, tv_series, genre

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    """
    Load data from The Movie Database API using dlt.
    This block extracts data and loads it into BigQuery.
    """
    print("Starting dlt pipeline execution...")
    
    # Get the API key from dlt secrets
    api_key = dlt.secrets["sources.api_secret_key"]
    
    print(f"Loaded API key: {api_key[:10]}..." if api_key else "No API key found")
    
    # Initialize the dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="themoviedb_pipeline",
        destination="bigquery",
        dataset_name="movie_data_mage",  # Use new dataset name to avoid schema version conflicts
        progress="log",  # Use log progress instead of alive_progress
    )
    
    # Create the list of resources directly (no @dlt.source decorator)
    resources = [
        movies.themoviedb_movies_resource(api_key),
        movies.themoviedb_movie_details_resource(api_key),
        tv_series.themoviedb_tv_series_details_resource(api_key),
        trending.themoviedb_trending_movies_resource(api_key),
        trending.themoviedb_trending_tv_series_resource(api_key),
        genre.themoviedb_genres_movies_resource(api_key),
        genre.themoviedb_genres_tv_series_resource(api_key),
    ]
    
    # Run the pipeline with all resources
    load_info = pipeline.run(resources)
    print(f"Pipeline execution completed: {load_info}")
    
    return {"status": "success", "load_info": str(load_info)}


@test
def test_output(output, *args) -> None:
    """
    Test that the pipeline executed successfully.
    """
    assert output is not None, 'The output is undefined'
    assert output.get('status') == 'success', 'Pipeline did not complete successfully'

