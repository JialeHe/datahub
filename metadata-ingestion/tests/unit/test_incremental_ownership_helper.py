import json

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.incremental_ownership_helper import (
    auto_incremental_ownership,
)
from datahub.metadata.schema_classes import (
    MetadataChangeProposalClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)

DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:databricks,catalog.schema.table,PROD)"
)
OWNERS = [
    OwnerClass(owner="urn:li:corpuser:user1", type=OwnershipTypeClass.DATAOWNER),
    OwnerClass(owner="urn:li:corpuser:user2", type=OwnershipTypeClass.DEVELOPER),
]


def _ownership_wu(urn: str = DATASET_URN, entity_type: str = "dataset"):
    return MetadataChangeProposalWrapper(
        entityUrn=urn, entityType=entity_type, aspect=OwnershipClass(owners=OWNERS)
    ).as_workunit()


def test_disabled_passes_through_as_upsert():
    """When incremental_ownership=False, ownership emits as standard UPSERT."""
    result = list(auto_incremental_ownership(False, [_ownership_wu()]))

    assert len(result) == 1
    assert isinstance(result[0].metadata, MetadataChangeProposalWrapper)
    assert result[0].metadata.changeType == "UPSERT"


def test_enabled_converts_dataset_ownership_to_patch():
    """When incremental_ownership=True, dataset ownership becomes a JSON Patch MCP
    with the correct owner URNs in the payload."""
    result = list(auto_incremental_ownership(True, [_ownership_wu()]))

    assert len(result) == 1
    mcp = result[0].metadata
    assert isinstance(mcp, MetadataChangeProposalClass)
    assert mcp.changeType == "PATCH"
    assert mcp.aspectName == "ownership"

    payload = json.loads(mcp.aspect.value)
    paths = [op["path"] for op in payload]
    assert any("user1" in p for p in paths)
    assert any("user2" in p for p in paths)


def test_enabled_does_not_convert_container_ownership():
    """Container ownership passes through as UPSERT even when incremental is on —
    only dataset entities should be converted to patches."""
    result = list(
        auto_incremental_ownership(
            True,
            [_ownership_wu(urn="urn:li:container:abc123", entity_type="container")],
        )
    )

    assert len(result) == 1
    assert isinstance(result[0].metadata, MetadataChangeProposalWrapper)
    assert result[0].metadata.changeType == "UPSERT"
