from pydantic import BaseModel, EmailStr, ValidationError, field_validator
from datetime import datetime

class Commit(BaseModel):
    """
    Pydantic model representing a commit record. -> choosing pydantic for row level data validation.

    Attributes:
        sha (str): The commit SHA.
        committer_id (int): The unique identifier of the committer.
        committer_username (str): The GitHub username of the committer.
        committer_name (str): The name of the committer.
        committer_email (str): The email address of the committer.
        commit_date (datetime): The datetime when the commit was made.
        pipeline_run_date (str): The pipeline run date associated with this commit.
    """

    sha:str
    committer_id:int
    committer_username:str
    committer_name:str
    committer_email:str
    commit_date: datetime

    pipeline_run_date: str

    @field_validator('sha')
    def sha_must_not_be_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("SHA must not be empty")
        return v

    @field_validator('commit_date')
    def commit_date_cannot_be_future(cls, v):
        if v > datetime.utcnow():
            raise ValueError("Commit date cannot be in the future")
        return v