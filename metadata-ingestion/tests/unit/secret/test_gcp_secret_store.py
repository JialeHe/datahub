from unittest.mock import MagicMock, patch

from datahub.secret.gcp_secret_store import (
    GcpSecretManagerStore,
    GcpSecretManagerStoreConfig,
)


def _mock_secret_response(value: str) -> MagicMock:
    """Create a mock response matching what access_secret_version returns."""
    response = MagicMock()
    response.payload.data = value.encode("UTF-8")
    return response


class TestGcpSecretManagerStore:
    def test_create(self):
        store = GcpSecretManagerStore.create({"project_id": "my-project"})
        assert isinstance(store, GcpSecretManagerStore)
        assert store.config.project_id == "my-project"

    def test_get_id(self):
        store = GcpSecretManagerStore.create({"project_id": "my-project"})
        assert store.get_id() == "gcp-sm"

    @patch("datahub.secret.gcp_secret_store.secretmanager.SecretManagerServiceClient")
    def test_get_secret_values_found(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.access_secret_version.return_value = _mock_secret_response(
            "my-value"
        )

        store = GcpSecretManagerStore.create({"project_id": "my-project"})
        result = store.get_secret_values(["MY_SECRET"])

        assert result == {"MY_SECRET": "my-value"}
        mock_client.access_secret_version.assert_called_once()

        # Verify the resource path is correct
        call_args = mock_client.access_secret_version.call_args
        assert (
            call_args.kwargs["request"]["name"]
            == "projects/my-project/secrets/MY_SECRET/versions/latest"
        )

    @patch("datahub.secret.gcp_secret_store.secretmanager.SecretManagerServiceClient")
    def test_get_secret_values_not_found(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.access_secret_version.side_effect = Exception("NOT_FOUND")

        store = GcpSecretManagerStore.create({"project_id": "my-project"})
        result = store.get_secret_values(["NONEXISTENT"])

        assert result == {"NONEXISTENT": None}

    @patch("datahub.secret.gcp_secret_store.secretmanager.SecretManagerServiceClient")
    def test_get_secret_values_mixed(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        def side_effect(request, retry):
            name = request["name"]
            if "EXISTS" in name:
                return _mock_secret_response("value1")
            raise Exception("NOT_FOUND")

        mock_client.access_secret_version.side_effect = side_effect

        store = GcpSecretManagerStore.create({"project_id": "my-project"})
        result = store.get_secret_values(["EXISTS", "MISSING"])

        assert result == {"EXISTS": "value1", "MISSING": None}

    @patch("datahub.secret.gcp_secret_store.secretmanager.SecretManagerServiceClient")
    def test_get_secret_value_single(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.access_secret_version.return_value = _mock_secret_response(
            "single-value"
        )

        store = GcpSecretManagerStore.create({"project_id": "my-project"})
        result = store.get_secret_value("SINGLE")

        assert result == "single-value"

    @patch("datahub.secret.gcp_secret_store.secretmanager.SecretManagerServiceClient")
    def test_get_secret_value_not_found(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.access_secret_version.side_effect = Exception("NOT_FOUND")

        store = GcpSecretManagerStore.create({"project_id": "my-project"})
        result = store.get_secret_value("NONEXISTENT")

        assert result is None

    @patch("datahub.secret.gcp_secret_store.secretmanager.SecretManagerServiceClient")
    def test_get_secret_values_empty_list(self, mock_client_cls):
        store = GcpSecretManagerStore.create({"project_id": "my-project"})
        result = store.get_secret_values([])

        assert result == {}

    def test_close(self):
        store = GcpSecretManagerStore.create({"project_id": "my-project"})
        store.close()
        assert store._client is None

    def test_config_required_project_id(self):
        import pytest
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            GcpSecretManagerStoreConfig()  # project_id is required, no default

    @patch("datahub.secret.gcp_secret_store.secretmanager.SecretManagerServiceClient")
    def test_resource_path_includes_versions_latest(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.access_secret_version.return_value = _mock_secret_response("val")

        store = GcpSecretManagerStore.create({"project_id": "yahoo-prod"})
        store.get_secret_values(["DB_PASS"])

        call_args = mock_client.access_secret_version.call_args
        resource_path = call_args.kwargs["request"]["name"]
        assert resource_path == "projects/yahoo-prod/secrets/DB_PASS/versions/latest"

    @patch("datahub.secret.gcp_secret_store.secretmanager.SecretManagerServiceClient")
    def test_client_reused_across_calls(self, mock_client_cls):
        store = GcpSecretManagerStore.create({"project_id": "my-project"})

        store._get_client()
        store._get_client()

        # Client constructor called only once
        assert mock_client_cls.call_count == 1

    @patch("datahub.secret.gcp_secret_store.secretmanager.SecretManagerServiceClient")
    def test_retry_is_passed(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.access_secret_version.return_value = _mock_secret_response("val")

        store = GcpSecretManagerStore.create({"project_id": "my-project"})
        store.get_secret_values(["SECRET"])

        call_args = mock_client.access_secret_version.call_args
        assert "retry" in call_args.kwargs
