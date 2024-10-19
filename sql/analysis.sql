-- Company with the highest pageviews at 16:00

SELECT company, views
FROM pageviews
WHERE created_at::date = '2024-10-10' AND EXTRACT(HOUR FROM created_at) = 16
ORDER BY views DESC
LIMIT 1;
