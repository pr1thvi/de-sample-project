{{ config(materialized='table') }}

SELECT
    dp.content_type,
    EXTRACT(YEAR FROM dp.content_release_date) AS release_year,
    EXTRACT(MONTH FROM dp.content_release_date) AS release_month,
    COUNT(*) AS trending_count

FROM {{ ref('dim_popular_content_details_info') }} dp
GROUP BY release_year, release_month, content_type
ORDER BY release_year, release_month