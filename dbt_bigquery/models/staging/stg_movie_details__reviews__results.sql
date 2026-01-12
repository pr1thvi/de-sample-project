{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('movie_data_mage', 'movie_details__reviews__results') }}
)

SELECT
    author AS movie_review_author_name,
    author_details__rating AS movie_review_author_rating,
    content AS movie_review_author_content,
    created_at AS movie_review_author_created_at,
    id AS movie_review_id,
    updated_at AS movie_review_author_updated_at,
    url AS movie_review_url,
    _dlt_parent_id,
    _dlt_id

FROM source