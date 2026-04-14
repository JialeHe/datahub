import logging
from typing import Any, Dict, List, Optional

from google.api_core.retry import Retry
from google.cloud import secretmanager
from pydantic import BaseModel

from datahub.secret.secret_store import SecretStore

logger = logging.getLogger(__name__)

# Retry config for GCP Secret Manager.
# The first gRPC call to establish the channel can take 10-30s ("cold channel" problem).
_DEFAULT_RETRY = Retry(initial=1.0, maximum=10.0, multiplier=2.0, deadline=30.0)


class GcpSecretManagerStoreConfig(BaseModel):
    project_id: str


class GcpSecretManagerStore(SecretStore):
    """SecretStore implementation that fetches secrets from GCP Secret Manager."""

    def __init__(self, config: GcpSecretManagerStoreConfig):
        self.config = config
        self._client = None

    def _get_client(self) -> secretmanager.SecretManagerServiceClient:
        if self._client is None:
            self._client = secretmanager.SecretManagerServiceClient()
        return self._client

    def get_secret_values(self, secret_names: List[str]) -> Dict[str, Optional[str]]:
        client = self._get_client()
        results: Dict[str, Optional[str]] = {}
        for name in secret_names:
            resource = (
                f"projects/{self.config.project_id}/secrets/{name}/versions/latest"
            )
            try:
                resp = client.access_secret_version(
                    request={"name": resource}, retry=_DEFAULT_RETRY
                )
                results[name] = resp.payload.data.decode("UTF-8")
            except Exception:
                logger.exception(
                    f"Failed to fetch secret '{name}' from GCP Secret Manager"
                )
                results[name] = None
        return results

    def get_id(self) -> str:
        return "gcp-sm"

    def close(self) -> None:
        self._client = None

    @classmethod
    def create(cls, config: Any) -> "GcpSecretManagerStore":
        config = GcpSecretManagerStoreConfig.model_validate(config)
        return cls(config)
