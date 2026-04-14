import logging
from typing import Any, Dict, List, Optional

import boto3
from pydantic import BaseModel

from datahub.secret.secret_store import SecretStore

logger = logging.getLogger(__name__)


class AwsSecretsManagerStoreConfig(BaseModel):
    region: str


class AwsSecretsManagerStore(SecretStore):
    """SecretStore implementation that fetches secrets from AWS Secrets Manager."""

    def __init__(self, config: AwsSecretsManagerStoreConfig):
        self.config = config
        self._client = None

    def _get_client(self):
        if self._client is None:
            self._client = boto3.client(
                "secretsmanager", region_name=self.config.region
            )
        return self._client

    def get_secret_values(self, secret_names: List[str]) -> Dict[str, Optional[str]]:
        client = self._get_client()
        results: Dict[str, Optional[str]] = {}
        for name in secret_names:
            try:
                resp = client.get_secret_value(SecretId=name)
                results[name] = resp.get("SecretString")
            except client.exceptions.ResourceNotFoundException:
                results[name] = None
            except Exception:
                logger.exception(
                    f"Failed to fetch secret '{name}' from AWS Secrets Manager"
                )
                results[name] = None
        return results

    def get_id(self) -> str:
        return "aws-sm"

    def close(self) -> None:
        self._client = None

    @classmethod
    def create(cls, config: Any) -> "AwsSecretsManagerStore":
        config = AwsSecretsManagerStoreConfig.model_validate(config)
        return cls(config)
