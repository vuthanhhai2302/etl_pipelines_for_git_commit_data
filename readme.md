# ETL Pipeline: GitHub Commits to PostgreSQL

## Overview


The code will run using a main script and all the processing function will be wraped in a custom lib in src folder, this can be change to shell runner if turned into a framework or run using a ochestrator.

This ETL pipeline extracts commit data from the GitHub API, saves the raw data to file storage partitioned by month, converts the raw data into a list of commit models, and then loads the validated data into a PostgreSQL database. The process also includes post-load validations to ensure data integrity.

## Prerequisites
Install the neccessary library to your venv from requirements.txt

- **Python 3.9+**
- Environment variables set (e.g., `GITHUB_TOKEN`)
- Required packages:
  - `aiohttp`
  - `psycopg2`
  - `pyyaml`
  - `pydantic`
  - Other standard libraries such as `datetime`, `os`, `logging`

## Pipeline Components

- **ConfigLoader:** Loads pipeline configuration from a YAML file.
- **GitCommitIngestor:** Fetches commit data asynchronously from the GitHub API and aggregates it by month.
- **FileStorageLoader:** Saves the aggregated commit data to partitioned folders based on year and month.
- **FileStorageToModelAdapter:** Loads and converts the file storage data into a list of validated commit model instances.
- **PostgresConnector:** Handles connection to PostgreSQL, deletion of old records, and batch insertion of new data.
- **Commit (Pydantic model):** Represents a commit record with validation to ensure data integrity.

## Pipeline Flow
Notes: the code will ingest and load to local storage and then load to destination database. the main reason is if we have trouble loading to the destination database, we can re run the failed task (if we are using a ochestrator). 

1. **Configuration & Environment Setup:**  
   The pipeline run date is determined using the current date. The `ConfigLoader` loads settings from `pipeline_config.yaml`, and environment variables (e.g., `GITHUB_TOKEN`) are read for API authentication

2. **Data Extraction:**  
   The `GitCommitIngestor` collects commit data from GitHub using asynchronous API calls. It aggregates commits by month for the past six months

3. **Saving to File Storage:**  
   The `FileStorageLoader` writes the aggregated data into partitioned folders (organized by year and month), returning a list of file paths

4. **Data Transformation:**  
   The `FileStorageToModelAdapter` loads the commit data from the files and converts it into a list of commit model instances (`Commit`) and here we will be validating rowlevel for the process., while also returning a count of the extracted rows

5. **Data Loading:**  
   A connection to PostgreSQL is established using `PostgresConnector`. Existing records for the current pipeline run date are deleted, and the new commit data is batch-inserted into the target table.

6. **Post-Load Validation:**  
   The pipeline verifies that the number of rows loaded into PostgreSQL matches the expected count from the file storage. If there is a mismatch, an error is logged and raised.

7. **Cleanup:**  
   The PostgreSQL connection is closed and a success log message is produced if all validations pass.

## SQL for queries
you can find the queries from folder sql.
