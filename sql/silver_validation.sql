-- API Statistics Summary

SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT video_id) AS unique_videos,
    MIN(trending_date) AS earliest_date,
    MAX(trending_date) AS latest_date
FROM youtube_silver.api_statistics;


-- Kaggle Statistics Summary

SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT video_id) AS unique_videos,
    MIN(trending_date) AS earliest_date,
    MAX(trending_date) AS latest_date
FROM youtube_silver.kaggle_statistics;


-- Kaggle Reference Summary

SELECT
    region,
    COUNT(*) AS category_count
FROM youtube_silver.kaggle_reference_data
GROUP BY region
ORDER BY region;


-- API Statistics Required Fields

SELECT
    COUNT(*) AS invalid_rows
FROM youtube_silver.api_statistics
WHERE video_id IS NULL
   OR title IS NULL
   OR channel_title IS NULL
   OR views IS NULL
   OR region IS NULL
   OR trending_date IS NULL;


-- Kaggle Statistics Required Fields

SELECT
    COUNT(*) AS invalid_rows
FROM youtube_silver.kaggle_statistics
WHERE video_id IS NULL
   OR title IS NULL
   OR channel_title IS NULL
   OR views IS NULL
   OR region IS NULL
   OR trending_date IS NULL;


-- API Statistics Negative Metrics

SELECT
    COUNT(*) AS invalid_rows
FROM youtube_silver.api_statistics
WHERE views < 0
   OR likes < 0
   OR dislikes < 0
   OR comment_count < 0;


-- Kaggle Statistics Negative Metrics

SELECT
    COUNT(*) AS invalid_rows
FROM youtube_silver.kaggle_statistics
WHERE views < 0
   OR likes < 0
   OR dislikes < 0
   OR comment_count < 0;


-- API Statistics Duplicates

SELECT
    video_id,
    region,
    trending_date,
    views,
    likes,
    dislikes,
    comment_count,
    COUNT(*) AS duplicate_count
FROM youtube_silver.api_statistics
GROUP BY
    video_id,
    region,
    trending_date,
    views,
    likes,
    dislikes,
    comment_count
HAVING COUNT(*) > 1;


-- Kaggle Statistics Duplicates

SELECT
    video_id,
    region,
    trending_date,
    views,
    likes,
    dislikes,
    comment_count,
    COUNT(*) AS duplicate_count
FROM youtube_silver.kaggle_statistics
GROUP BY
    video_id,
    region,
    trending_date,
    views,
    likes,
    dislikes,
    comment_count
HAVING COUNT(*) > 1;


-- Reference Required Fields

SELECT
    COUNT(*) AS invalid_rows
FROM youtube_silver.kaggle_reference_data
WHERE category_id IS NULL
   OR category_title IS NULL
   OR region IS NULL;


-- Reference Duplicates

SELECT
    category_id,
    region,
    COUNT(*) AS duplicate_count
FROM youtube_silver.kaggle_reference_data
GROUP BY
    category_id,
    region
HAVING COUNT(*) > 1;