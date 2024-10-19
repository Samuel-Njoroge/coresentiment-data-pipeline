CREATE TABLE IF NOT EXISTS pageviews (
    id SERIAL PRIMARY KEY,
    company VARCHAR(255) NOT NULL,
    views INTEGER NOT NULL,
    bytes_sent INTEGER NOT NULL,
    created_at TIMESTAMP
)