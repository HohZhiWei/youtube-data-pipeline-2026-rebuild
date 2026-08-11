-- Trending Analytics

SELECT
    COUNT(*) AS row_count,
    MIN(trending_date) AS earliest_date,
    MAX(trending_date) AS latest_date
FROM youtube_gold.trending_analytics;


-- Channel Analytics

SELECT
    COUNT(*) AS row_count
FROM youtube_gold.channel_analytics;


-- Category Analytics

SELECT
    COUNT(*) AS row_count
FROM youtube_gold.category_analytics;


-- Trending Analytics Sample

SELECT *
FROM youtube_gold.trending_analytics
LIMIT 10;


-- Channel Analytics Sample

SELECT *
FROM youtube_gold.channel_analytics
LIMIT 10;


-- Category Analytics Sample

SELECT *
FROM youtube_gold.category_analytics
LIMIT 10;