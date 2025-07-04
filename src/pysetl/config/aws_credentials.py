"""
AWS Credentials module for PySetl.

Defines a Pydantic model for storing AWS credentials for S3 and related services.
"""
from pydantic import BaseModel


class AwsCredentials(BaseModel):
    """
    Container for AWS credentials used for S3 and related services.

    Attributes:
        access_key (str): AWS access key ID.
        secret_key (str): AWS secret access key.
        session_token (str): AWS session token (if using temporary credentials).
        credentials_provider (str): The credentials provider type or name.
    """

    access_key: str
    secret_key: str
    session_token: str
    credentials_provider: str
