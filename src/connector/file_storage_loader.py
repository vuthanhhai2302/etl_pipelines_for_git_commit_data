import os
import json
import logging

class FileStorageLoader:
    @classmethod
    def load_to_fs_partitioned_by_month(self, folder_path:str, data_by_month:dict):
        """
        Saves fetched commits into partitioned folders organized by year and month.

        Args:
            folder_path (str): The root directory where commit data should be saved.
            data_by_month (dict): A dictionary where keys are tuples (year, month) and values are commit data.

        Returns:
            list: A list of file paths where commit data has been saved.
        """
        
        os.makedirs(folder_path, exist_ok=True)
        list_file = []
        for (year, month), data in data_by_month.items():
            file_path = f"{folder_path}/{year}/{month:02d}/commits.json"
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            list_file.append(file_path)
            logging.info(f"FileStorageLoader - Saved {len(data)} commits to '{file_path}'")

        return list_file
