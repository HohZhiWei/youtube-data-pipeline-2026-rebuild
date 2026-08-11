-- Trending Analytics

SELECT
    COUNT(*) AS row_count,
    MIN(trending_date) AS earliest_date,
    MAX(trending_date) AS latest_date
FROM yt_pipeline_gold_dev.trending_analytics;


-- Channel Analytics

SELECT
    COUNT(*) AS row_count
FROM yt_pipeline_gold_dev.channel_analytics;


-- Category Analytics

SELECT
    COUNT(*) AS row_count
FROM yt_pipeline_gold_dev.category_analytics;


-- Trending Analytics Sample

SELECT *
FROM yt_pipeline_gold_dev.trending_analytics
LIMIT 10;


-- Channel Analytics Sample

SELECT *
FROM yt_pipeline_gold_dev.channel_analytics
LIMIT 10;


-- Category Analytics Sample

SELECT *
FROM yt_pipeline_gold_dev.category_analytics
LIMIT 10;