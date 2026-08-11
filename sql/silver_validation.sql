-- API Statistics

SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT video_id) AS unique_videos,
    MIN(trending_date) AS earliest_date,
    MAX(trending_date) AS latest_date
FROM youtube_silver.api_statistics;


-- Kaggle Statistics

SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT video_id) AS unique_videos,
    MIN(trending_date) AS earliest_date,
    MAX(trending_date) AS latest_date
FROM youtube_silver.kaggle_statistics;


-- API Reference Data

SELECT
    region,
    COUNT(*) AS category_count
FROM youtube_silver.api_reference_data
GROUP BY region
ORDER BY region;


-- Kaggle Reference Data

SELECT
    region,
    COUNT(*) AS category_count
FROM youtube_silver.kaggle_reference_data
GROUP BY region
ORDER BY region;


-- Statistics Null Check

SELECT
    COUNT(*) AS invalid_rows
FROM youtube_silver.api_statistics
WHERE video_id IS NULL
   OR title IS NULL
   OR channel_title IS NULL
   OR views IS NULL;


-- Statistics Negative Metrics Check

SELECT
    COUNT(*) AS invalid_rows
FROM youtube_silver.api_statistics
WHERE views < 0
   OR likes < 0
   OR dislikes < 0
   OR comment_count < 0;