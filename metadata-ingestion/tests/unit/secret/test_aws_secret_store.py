import boto3
from moto import mock_aws

from datahub.secret.aws_secret_store import (
    AwsSecretsManagerStore,
    AwsSecretsManagerStoreConfig,
)


class TestAwsSecretsManagerStore:
    def test_create(self):
        store = AwsSecretsManagerStore.create({"region": "us-east-1"})
        assert isinstance(store, AwsSecretsManagerStore)
        assert store.config.region == "us-east-1"

    def test_get_id(self):
        store = AwsSecretsManagerStore.create({"region": "us-east-1"})
        assert store.get_id() == "aws-sm"

    @mock_aws
    def test_get_secret_values_found(self):
        # Create a secret in mocked AWS
        client = boto3.client("secretsmanager", region_name="us-east-1")
        client.create_secret(Name="MY_SECRET", SecretString="my-value")

        store = AwsSecretsManagerStore.create({"region": "us-east-1"})
        result = store.get_secret_values(["MY_SECRET"])

        assert result == {"MY_SECRET": "my-value"}

    @mock_aws
    def test_get_secret_values_not_found(self):
        store = AwsSecretsManagerStore.create({"region": "us-east-1"})
        result = store.get_secret_values(["NONEXISTENT"])

        assert result == {"NONEXISTENT": None}

    @mock_aws
    def test_get_secret_values_mixed(self):
        client = boto3.client("secretsmanager", region_name="us-east-1")
        client.create_secret(Name="EXISTS", SecretString="value1")

        store = AwsSecretsManagerStore.create({"region": "us-east-1"})
        result = store.get_secret_values(["EXISTS", "MISSING"])

        assert result == {"EXISTS": "value1", "MISSING": None}

    @mock_aws
    def test_get_secret_value_single(self):
        client = boto3.client("secretsmanager", region_name="us-east-1")
        client.create_secret(Name="SINGLE", SecretString="single-value")

        store = AwsSecretsManagerStore.create({"region": "us-east-1"})
        result = store.get_secret_value("SINGLE")

        assert result == "single-value"

    @mock_aws
    def test_get_secret_value_not_found(self):
        store = AwsSecretsManagerStore.create({"region": "us-east-1"})
        result = store.get_secret_value("NONEXISTENT")

        assert result is None

    @mock_aws
    def test_get_secret_values_empty_list(self):
        store = AwsSecretsManagerStore.create({"region": "us-east-1"})
        result = store.get_secret_values([])

        assert result == {}

    def test_close(self):
        store = AwsSecretsManagerStore.create({"region": "us-east-1"})
        store.close()
        assert store._client is None

    def test_config_defaults(self):
        config = AwsSecretsManagerStoreConfig(region="eu-west-1")
        assert config.region == "eu-west-1"

    @mock_aws
    def test_client_reused_across_calls(self):
        store = AwsSecretsManagerStore.create({"region": "us-east-1"})

        client1 = store._get_client()
        client2 = store._get_client()

        assert client1 is client2
