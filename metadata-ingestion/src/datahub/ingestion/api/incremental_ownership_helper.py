import logging
from typing import Iterable, Optional

from pydantic.fields import Field

from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.source_helpers import create_dataset_owners_patch_builder
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import OwnershipClass, SystemMetadataClass

logger = logging.getLogger(__name__)


def convert_ownership_to_patch(
    urn: str,
    aspect: OwnershipClass,
    system_metadata: Optional[SystemMetadataClass] = None,
) -> MetadataWorkUnit:
    patch_builder = create_dataset_owners_patch_builder(urn, aspect, system_metadata)
    mcp = next(iter(patch_builder.build()))
    return MetadataWorkUnit(id=MetadataWorkUnit.generate_workunit_id(mcp), mcp_raw=mcp)


def auto_incremental_ownership(
    incremental_ownership: bool,
    stream: Iterable[MetadataWorkUnit],
) -> Iterable[MetadataWorkUnit]:
    if not incremental_ownership:
        yield from stream
        return  # early exit

    for wu in stream:
        urn = wu.get_urn()

        if (
            isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(wu.metadata.aspect, OwnershipClass)
            and wu.metadata.entityType == "dataset"
        ):
            ownership_aspect = wu.metadata.aspect
            if ownership_aspect.owners:
                yield convert_ownership_to_patch(
                    urn, ownership_aspect, wu.metadata.systemMetadata
                )
        else:
            yield wu


class IncrementalOwnershipConfigMixin(ConfigModel):
    incremental_ownership: bool = Field(
        default=False,
        description="When enabled, emits ownership as incremental to existing ownership "
        "in DataHub. When disabled, re-states ownership on each run.",
    )
