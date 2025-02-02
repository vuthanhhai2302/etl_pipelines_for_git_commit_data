from src.connector.pg_connector import PostgresConnector
from src.connector.git_commit_ingestor import GitCommitIngestor
from src.connector.file_storage_loader import FileStorageLoader
from src.adapter.file_storage_model_adapter import FileStorageToModelAdapter
from src.utils.config_loader import ConfigLoader

import asyncio
from datetime import datetime
import os
from dotenv import load_dotenv
import logging

logging.basicConfig(
    level=logging.INFO,  # -> log level info
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
load_dotenv()

async def main():
    pipeline_run_date = datetime.now().strftime('%Y-%m-%d')
    config_loader = ConfigLoader("pipeline_conf/pipeline_config.yaml")
    git_ingestor = GitCommitIngestor(
        git_token       =   os.getenv("GITHUB_TOKEN")
        , repo_owner    =   config_loader.get('git_repo', 'owner')
        , repo_name     =   config_loader.get('git_repo', 'repo_name')
        )
    logging.info("Main - Collecting data from github api.")
    data = await git_ingestor.fetch_and_save_commit_by_month()
    logging.info("Main - Loading data from file storage.")
    list_file = FileStorageLoader.load_to_fs_partitioned_by_month(
        folder_path= config_loader.get('local_storage', 'path')
        , data_by_month=data
        )
    logging.info("Main - loading data to model.")
    commit_staging, total_row_extracted = FileStorageToModelAdapter.extract_commit_data_from_fs(list_file=list_file, pipeline_run_date=pipeline_run_date)
    logging.info("Main - connecting to PG db.")
    pg_conn = PostgresConnector(
        dbname      = config_loader.get('database', 'dbname')
        , user      = config_loader.get('database', 'user')
        , password  = config_loader.get('database', 'password')
        , host      = config_loader.get('database', 'host')
        , port      = config_loader.get('database', 'port')
        )
    logging.info("Main - Operating delsert operation.")
    pg_conn.delete_from_table(
        table=config_loader.get('table', 'name')
        , column='pipeline_run_date'
        , value=pipeline_run_date
        )
    pg_conn.insert_data(
        table=config_loader.get('table', 'name')
        , data=commit_staging
        )
    
    logging.info("Main - Validating data loaded")
    total_pg_data = pg_conn.get_db_row_count(
        table=config_loader.get('table', 'name')
        , column='pipeline_run_date'
        , value=pipeline_run_date
    )
    pg_conn.close_connection()
    
    if total_pg_data != total_row_extracted:
        logging.error('Main - Miss match loading total row of data')
        raise 

    logging.info("Main - Done validating number of rows in file storage and loaded location is equal")
    

asyncio.run(main())

