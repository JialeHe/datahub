from unittest.mock import Mock, patch

from datahub.ingestion.graph.client import (
    DatahubClientConfig,
    DataHubGraph,
    entity_type_to_graphql,
)
from datahub.metadata.schema_classes import CorpUserEditableInfoClass


@patch("datahub.emitter.rest_emitter.DataHubRestEmitter.test_connection")
def test_get_aspect(mock_test_connection):
    mock_test_connection.return_value = {}
    graph = DataHubGraph(DatahubClientConfig(server="http://fake-domain.local"))
    user_urn = "urn:li:corpuser:foo"
    with patch("requests.Session.get") as mock_get:
        mock_response = Mock()
        mock_response.json = Mock(
            return_value={
                "version": 0,
                "aspect": {"com.linkedin.identity.CorpUserEditableInfo": {}},
            }
        )
        mock_get.return_value = mock_response
        editable = graph.get_aspect(user_urn, CorpUserEditableInfoClass)
        assert editable is not None


def test_graphql_entity_types() -> None:
    # FIXME: This is a subset of all the types, but it's enough to get us ok coverage.

    known_mappings = {
        "domain": "DOMAIN",
        "dataset": "DATASET",
        "dashboard": "DASHBOARD",
        "chart": "CHART",
        "corpuser": "CORP_USER",
        "corpGroup": "CORP_GROUP",
        "dataFlow": "DATA_FLOW",
        "dataJob": "DATA_JOB",
        "glossaryNode": "GLOSSARY_NODE",
        "glossaryTerm": "GLOSSARY_TERM",
        "dataProduct": "DATA_PRODUCT",
        "dataHubExecutionRequest": "EXECUTION_REQUEST",
        "document": "DOCUMENT",
    }

    for entity_type, graphql_type in known_mappings.items():
        assert entity_type_to_graphql(entity_type) == graphql_type


def test_data_process_maps_to_data_process_instance() -> None:
    # "dataProcess" is a deprecated entity type (the PDL model is annotated
    # @deprecated = "Use DataJob instead."). It was never added to the GraphQL
    # EntityType enum in entity.graphql -- DATA_PROCESS does not exist as an
    # enum value, only DATA_PROCESS_INSTANCE does.
    #
    # entity_type_to_graphql() converts camelCase mechanically, producing
    # "DATA_PROCESS" for "dataProcess". Passing that to searchAcrossEntities
    # causes GMS to return a GraphQL ValidationError:
    #
    #   Variable 'types' has an invalid value: Invalid input for enum
    #   'EntityType'. No value found for name 'DATA_PROCESS'
    #
    # This is triggered in practice when mcp-server-datahub's search tool
    # receives a filter like entity_type=dataProcess from an LLM agent
    # querying for pipeline run entities. The mapping to DATA_PROCESS_INSTANCE
    # is intentional: LLM agents using the deprecated name are almost certainly
    # looking for run instances (traces/spans), not workflow definitions
    # (DataJob). See entity.graphql enum EntityType for the authoritative list.
    assert entity_type_to_graphql("dataProcess") == "DATA_PROCESS_INSTANCE"
