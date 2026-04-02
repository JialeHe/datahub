import logging
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_group_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.s3_util import make_s3_urn_for_lineage
from datahub.ingestion.source.common.subtypes import (
    DataJobSubTypes,
    FlowContainerSubTypes,
)
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDataDictionary,
    SnowflakePipe,
)
from datahub.ingestion.source.snowflake.snowflake_stages import (
    SnowflakeStagesExtractor,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass,
    SubTypesClass,
)

logger: logging.Logger = logging.getLogger(__name__)

# Regex to parse COPY INTO statement
# Handles: COPY INTO [db.][schema.]table FROM @[db.][schema.]stage[/path/...]
# Supports quoted identifiers and optional trailing path/options after the stage name.
_COPY_INTO_TARGET_PATTERN = re.compile(
    r"COPY\s+INTO\s+"
    r"(?:\"?(\w+)\"?\.)?(?:\"?(\w+)\"?\.)?\"?(\w+)\"?",
    re.IGNORECASE,
)
_COPY_INTO_STAGE_PATTERN = re.compile(
    r"FROM\s+"
    r"@(?:\"?(\w+)\"?\.)?(?:\"?(\w+)\"?\.)?\"?(\w+)\"?"
    r"(?:/[^\s]*)?",  # optional /path after stage name
    re.IGNORECASE,
)


def parse_copy_into(
    definition: str, default_db: str, default_schema: str
) -> Tuple[Optional[str], Optional[str]]:
    """Parse a COPY INTO statement to extract target table and stage reference.

    Handles common Snowflake COPY INTO variants:
    - COPY INTO table FROM @stage
    - COPY INTO db.schema.table FROM @db.schema.stage/path/
    - COPY INTO "DB"."SCHEMA"."TABLE" FROM @"STAGE"

    Returns:
        (target_table_fqn, stage_fqn) as uppercase fully-qualified names,
        or (None, None) if parsing fails.
    """
    target_match = _COPY_INTO_TARGET_PATTERN.search(definition)
    stage_match = _COPY_INTO_STAGE_PATTERN.search(definition)

    if not target_match or not stage_match:
        return None, None

    # Target table
    target_db = target_match.group(1) or default_db
    target_schema = target_match.group(2) or default_schema
    target_table = target_match.group(3)
    target_fqn = f"{target_db}.{target_schema}.{target_table}".upper()

    # Stage reference
    stage_db = stage_match.group(1) or default_db
    stage_schema = stage_match.group(2) or default_schema
    stage_name = stage_match.group(3)
    stage_fqn = f"{stage_db}.{stage_schema}.{stage_name}".upper()

    return target_fqn, stage_fqn


@dataclass
class SnowflakePipesExtractor:
    config: SnowflakeV2Config
    report: SnowflakeV2Report
    data_dictionary: SnowflakeDataDictionary
    identifiers: SnowflakeIdentifierBuilder
    stages_extractor: SnowflakeStagesExtractor

    def get_workunits(
        self,
        db_name: str,
        schema_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        pipes = self.data_dictionary.get_pipes_for_schema(db_name, schema_name)
        if not pipes:
            return

        # Filter pipes first to avoid emitting empty DataFlows
        allowed_pipes = [
            pipe
            for pipe in pipes
            if self.config.pipe_pattern.allowed(
                f"{db_name}.{schema_name}.{pipe.name}".upper()
            )
        ]
        if not allowed_pipes:
            return

        flow_id = self.identifiers.snowflake_identifier(
            f"{db_name}.{schema_name}.pipes"
        )
        flow_urn = make_data_flow_urn(
            orchestrator="snowflake",
            flow_id=flow_id,
            cluster=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        # Emit the DataFlow (per-schema grouping)
        yield from self._gen_data_flow(flow_urn, db_name, schema_name)

        for pipe in allowed_pipes:
            self.report.pipes_scanned += 1

            yield from self._gen_data_job(
                pipe=pipe,
                flow_urn=flow_urn,
                db_name=db_name,
                schema_name=schema_name,
            )

    def _gen_data_flow(
        self,
        flow_urn: str,
        db_name: str,
        schema_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=DataFlowInfoClass(
                name=f"{db_name}.{schema_name} Pipes",
                description=f"Snowflake Snowpipe objects in {db_name}.{schema_name}",
                customProperties={
                    "database": db_name,
                    "schema": schema_name,
                    "object_type": "SNOWFLAKE_PIPES",
                },
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=SubTypesClass(
                typeNames=[FlowContainerSubTypes.SNOWFLAKE_PIPE_GROUP],
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

    def _gen_data_job(
        self,
        pipe: SnowflakePipe,
        flow_urn: str,
        db_name: str,
        schema_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        job_id = self.identifiers.snowflake_identifier(pipe.name)
        job_urn = make_data_job_urn_with_flow(flow_urn, job_id)

        # Parse COPY INTO to get target table and stage
        target_fqn, stage_fqn = parse_copy_into(pipe.definition, db_name, schema_name)
        if not target_fqn and pipe.definition:
            self.report.warning(
                "Failed to parse COPY INTO statement for pipe lineage",
                f"{db_name}.{schema_name}.{pipe.name}",
            )

        custom_properties: Dict[str, str] = {}
        if pipe.auto_ingest:
            custom_properties["auto_ingest"] = "true"
        if pipe.notification_channel:
            custom_properties["notification_channel"] = pipe.notification_channel
        if pipe.definition:
            custom_properties["definition"] = pipe.definition[:4000]
        if stage_fqn:
            custom_properties["stage_name"] = stage_fqn

        # Determine stage type for custom properties
        stage_entry = (
            self.stages_extractor.get_stage_lookup_entry(stage_fqn)
            if stage_fqn
            else None
        )
        if stage_entry:
            custom_properties["stage_type"] = stage_entry.stage.stage_type

        # DataJobInfo
        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInfoClass(
                name=pipe.name,
                description=pipe.comment,
                type="COMMAND",
                customProperties=custom_properties,
            ),
        ).as_workunit()

        # SubTypes
        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=SubTypesClass(
                typeNames=[DataJobSubTypes.SNOWFLAKE_PIPE],
            ),
        ).as_workunit()

        # Status
        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()

        # DataJobInputOutput — lineage
        input_datasets: List[str] = []
        output_datasets: List[str] = []

        # Downstream: target table
        if target_fqn:
            target_dataset_identifier = self.identifiers.snowflake_identifier(
                target_fqn
            )
            target_urn = self.identifiers.gen_dataset_urn(target_dataset_identifier)
            output_datasets.append(target_urn)

        # Upstream: depends on stage type
        if stage_entry:
            if stage_entry.stage.stage_type.upper() == "INTERNAL":
                # Internal stage: use placeholder dataset
                if stage_entry.dataset_urn:
                    input_datasets.append(stage_entry.dataset_urn)
            elif stage_entry.stage.url:
                # External stage: resolve URL to S3/GCS/Azure dataset URN
                upstream_urn = self._resolve_external_stage_url(stage_entry.stage.url)
                if upstream_urn:
                    input_datasets.append(upstream_urn)

        if input_datasets or output_datasets:
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataJobInputOutputClass(
                    inputDatasets=input_datasets,
                    outputDatasets=output_datasets,
                    inputDatajobs=[],
                ),
            ).as_workunit()

        # Ownership
        if pipe.owner:
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=OwnershipClass(
                    owners=[
                        OwnerClass(
                            owner=make_group_urn(pipe.owner),
                            type=OwnershipTypeClass.TECHNICAL_OWNER,
                        )
                    ]
                ),
            ).as_workunit()

    def _resolve_external_stage_url(self, url: str) -> Optional[str]:
        """Resolve an external stage URL to a dataset URN.

        Consistent with existing SnowflakeLineageExtractor.get_external_upstreams()
        which always resolves S3 URLs via make_s3_urn_for_lineage().
        """
        if url.startswith("s3://"):
            return make_s3_urn_for_lineage(url, self.config.env)
        # TODO: Add GCS and Azure support when utilities are available
        logger.debug(f"Unsupported external stage URL scheme: {url}")
        return None
