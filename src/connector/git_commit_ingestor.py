import asyncio, aiohttp
from datetime import datetime, timedelta
import logging

class GitCommitIngestor:

    def __init__(self, git_token:str, repo_owner:str, repo_name:str):
        """  
            Attributes:
                git_token (str): GitHub authentication token -> passed in local env
                repo_owner (str): Owner of the repository -> passed in through yaml definition
                repo_name (str): Name of the repository -> passed in through yaml definition
                BASE_URL (str): Base URL for the GitHub commits API
                HEADERS (dict): HTTP headers for authentication and content negotiation.
        """
        self.git_token = git_token
        self.repo_owner = repo_owner
        self.repo_name = repo_name
        
        self.BASE_URL = f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}/commits"
        self.HEADERS = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self.git_token}",
            "X-GitHub-Api-Version": "2022-11-28"
        }

    async def fetch_commit(self, session, start_date:datetime, end_date:datetime) -> list:
        """
        Fetch commits within a time range using asynchronous pagination.

        Args:
            session (aiohttp.ClientSession): The session used to make HTTP requests
            start_date (datetime): Start datetime for the commit range
            end_date (datetime): End datetime for the commit range.

        Returns:
            list: A list of commit data (dictionaries) retrieved from the GitHub API.
        """

        all_commits = []
        page = 1

        while True:
            params = {
                "since": start_date.isoformat() + "Z",
                "until": end_date.isoformat() + "Z",
                "per_page": 100,
                "page": page
            }
            async with session.get(self.BASE_URL, headers=self.HEADERS, params=params) as response:
                if response.status != 200:
                    logging.error(f"GitCommitIngestor - Error: {response.status}, {await response.text()}")
                    break

                commits = await response.json()
                if not commits:
                    break

                all_commits.extend(commits)
                page += 1

        return all_commits

    async def fetch_and_save_commit_by_month(self) -> dict:
        """
        Fetch commits for the past six months and aggregate them by month.

        For each of the past six months, this method:
        - Determines the start and end date for the month
        - Logs the fetch operation
        - Asynchronously fetches commit data using `fetch_commit`
        - Aggregates the commit data into a dictionary keyed by (year, month).

        Returns:
            dict: A dictionary with keys as (year, month) tuples and values as lists of commit data.
        """

        commits_by_month = {}

        async with aiohttp.ClientSession() as session:
            tasks = []
            today = datetime.utcnow()

            for _ in range(6):
                end_date = today
                start_date = end_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                year, month = end_date.year, end_date.month

                logging.info(f"GitCommitIngestor - Fetching commits from {start_date} to {end_date}...")
                tasks.append((year, month, self.fetch_commit(session, start_date, end_date)))

                today = (start_date - timedelta(days=1))

            results = await asyncio.gather(*[task[2] for task in tasks])

        logging.info(f"GitCommitIngestor - Arregating result by months")
        for i, (year, month, _) in enumerate(tasks):
            commits_by_month[(year, month)] = results[i]

        logging.info(f"GitCommitIngestor - Done Collecting data from github api.")
        return commits_by_month 
        