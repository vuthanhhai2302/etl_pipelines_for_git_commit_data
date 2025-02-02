from src.model.commit import Commit

import json
from datetime import datetime
import logging

class FileStorageToModelAdapter:
    @classmethod
    def extract_commit_data_from_fs(self, list_file:list, pipeline_run_date:str)->list:
        """
        Reads commit data from JSON files and converts it into a list of Commit model instances.

        Args:
            list_file (list): List of file paths containing commit data in JSON format
            pipeline_run_date (str): The pipeline run date.

        Returns:
            list: A list of Commit model instances.
        """
        commit_staging_data = []
        total_row_count = 0
        for file_path in list_file:
            logging.info(f"FileStorageToModelAdapter - Loading data from {file_path} to model.")
            with open(file_path, 'r') as f:
                commits_data = json.load(f)
                for commit_data in commits_data:
                    committer = commit_data.get("commit", {}).get("author", {})
                    author_data = commit_data.get("author", {}) or {}
                    commit = Commit(
                        sha = commit_data.get("sha"),
                        committer_id = author_data.get("id", 0),
                        committer_username = author_data.get("login", "None"),
                        committer_name = committer.get("name"),
                        committer_email = committer.get("email"),
                        commit_ts = datetime.strptime(committer.get("date"), "%Y-%m-%dT%H:%M:%SZ"),
                        pipeline_run_date=pipeline_run_date
                    )
                    commit_staging_data.append(commit)
                total_row_count += len(commits_data)

        logging.info(f"FileStorageToModelAdapter - Fully loaded data to model.")
        return commit_staging_data, total_row_count