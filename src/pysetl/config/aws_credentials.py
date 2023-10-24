"""
S3 Credentials
"""
from pydantic import BaseModel


class AwsCredentials(BaseModel):
    """
    S3 AWS Credentials container
    """
    access_key: str
    secret_key: str
    session_token: str
    credentials_provider: str
