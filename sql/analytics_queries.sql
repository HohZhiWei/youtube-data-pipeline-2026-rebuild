-- Top Channels by Total Views

SELECT
    region,
    channel_title,
    total_views,
    trending_appearances,
    unique_trending_videos,
    average_engagement_rate
FROM youtube_gold.channel_analytics
ORDER BY total_views DESC
LIMIT 20;


-- Highest Engagement Channels

SELECT
    region,
    channel_title,
    trending_appearances,
    average_engagement_rate
FROM youtube_gold.channel_analytics
WHERE trending_appearances >= 5
ORDER BY average_engagement_rate DESC
LIMIT 20;


-- Most Viewed Categories

SELECT
    region,
    category_title,
    total_views,
    trending_appearances,
    unique_trending_videos,
    average_engagement_rate
FROM youtube_gold.category_analytics
ORDER BY total_views DESC
LIMIT 20;


-- Category Performance by Region

SELECT
    region,
    category_title,
    total_views,
    average_views,
    average_engagement_rate
FROM youtube_gold.category_analytics
ORDER BY region, total_views DESC;


-- Daily Trending Activity

SELECT
    region,
    trending_date,
    trending_video_count,
    total_views,
    average_views,
    total_likes,
    total_comments,
    average_engagement_rate
FROM youtube_gold.trending_analytics
ORDER BY trending_date DESC, total_views DESC;


-- Most Active Trending Regions

SELECT
    region,
    SUM(trending_video_count) AS total_trending_videos,
    SUM(total_views) AS total_views
FROM youtube_gold.trending_analytics
GROUP BY region
ORDER BY total_views DESC;