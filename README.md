# CoreSentiment Data Pipeline.
CoreSentiment is a stock market prediction tool that applies sentiment analysis based on Wikipedia page view data. The project aims to correlate changes in a company's Wikipedia page views with potential stock price movements.
The focus is on implementing a data pipeline that addresses data ingestion, processing, storage, and analysis.

## Background

This project is based on the assumption that:
- An increase in a company's Wikipedia page views indicates positive sentiment, suggesting a likely increase in stock price.
- A decrease in page views indicates a loss of interest, suggesting a likely decrease in stock price.

## Solution Architecture.

## Data Source

The project utilizes publicly available Wikipedia pageview data provided by the Wikimedia Foundation. 

Key details:
- Data available for October 2024. [Data Link](https://dumps.wikimedia.org/other/pageviews)
- Aggregated per hour per page
- File format: gzip
- Hourly dump size: ~50 MB (gzipped), 200-250 MB (unzipped)

## Technologies Used

- *Docker*: For containerization and easy deployment of the application.
- *Apache Airflow*: For orchestrating and scheduling data pipelines.
- *Python*: As the primary programming language for data processing and analysis.
- *SQL*: For database operations and data querying.

## Analysis
A simple analysis to get the company with the highest pageviews for that hour 16:00 .
### Solution
```sh
SELECT company, views
FROM pageviews
WHERE created_at::date = '2024-10-10' AND EXTRACT(HOUR FROM created_at) = 16
ORDER BY views DESC
LIMIT 1;
```

## Set Up Guide.
#### Step 1: Clone the Repository

```
git clone https://github.com/Samuel-Njoroge/coresentiment.git
cd coresentiment
```
#### Step 2: Environment Setup
Create a `.env` file in the project root and add the following variables:
```
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
POSTGRES_DB=coresentiment
AIRFLOW_UID=50000
```

#### Step 3: Build and Start the Containers
```
docker-compose up -d
```

#### Step 4: Initialize Airflow
```
docker-compose run airflow airflow db init
```
```
docker-compose run airflow airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

#### Step 5: Access Airflow Web UI
Open a web browser and go to `http://localhost:8080`. 

Log in with the credentials you set in Step 4.

#### Step 6: Run the Data Pipeline
In the Airflow Web UI, enable the DAG named `dags/page_views.py`.

#### Step 7: Verify Data in PostgreSQL
You can connect to the PostgreSQL database.

```
docker-compose exec postgres psql -U your_username -d coresentiment

```

Once connected, you can run the analysis query:

```sh
SELECT company, views
FROM pageviews
WHERE created_at::date = '2024-10-10' AND EXTRACT(HOUR FROM created_at) = 16
ORDER BY views DESC
LIMIT 1;
```

## Contributing
If you'd like to contribute to this project, please fork the repository and create a pull request with your proposed changes.
