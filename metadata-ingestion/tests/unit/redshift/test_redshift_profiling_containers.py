"""Tests for Redshift profiling + container interaction when include_tables=False.

Verifies two fixes for ING-2018:
1. Schema containers are not generated when nothing will populate them
   (include_tables=False, include_views=False, profiling disabled).
2. When profiling IS enabled with include_tables=False, profiled datasets
   get container and dataPlatformInstance aspects so they appear correctly
   in the UI hierarchy.
"""

from typing import Dict, List, Optional
from unittest.mock import patch

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.profile import RedshiftProfiler
from datahub.ingestion.source.redshift.redshift_schema import (
    RedshiftTable,
)
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.metadata.schema_classes import (
    ContainerClass,
)


def make_profiler(
    include_tables: bool = False,
    platform_instance: Optional[str] = None,
    profiling_enabled: bool = True,
) -> RedshiftProfiler:
    config = RedshiftConfig(
        host_port="localhost:5439",
        database="test_db",
        include_tables=include_tables,
        platform_instance=platform_instance,
        profiling=GEProfilingConfig(enabled=profiling_enabled),
    )
    report = RedshiftReport()
    return RedshiftProfiler(config=config, report=report, state_handler=None)


def make_tables(
    db: str = "test_db", schema: str = "public"
) -> Dict[str, Dict[str, List[RedshiftTable]]]:
    return {
        db: {
            schema: [
                RedshiftTable(
                    name="test_table",
                    schema=schema,
                    columns=[],
                    created=None,
                    comment="",
                    type="BASE TABLE",
                    rows_count=100,
                    size_in_bytes=1024,
                ),
            ]
        }
    }


def collect_aspect_names(workunits: list[MetadataWorkUnit]) -> list[str]:
    return [
        wu.metadata.aspectName
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName is not None
    ]


def test_profiler_emits_container_aspect_when_include_tables_false():
    """When include_tables=False, the profiler should emit container aspects
    so profiled datasets link to their schema containers."""
    profiler = make_profiler(
        include_tables=False,
        platform_instance="my-instance",
    )
    tables = make_tables()

    # Patch generate_profile_workunits to avoid needing a real DB connection
    with patch.object(profiler, "generate_profile_workunits", return_value=iter([])):
        workunits = list(profiler.get_workunits(tables))

    aspect_names = collect_aspect_names(workunits)
    assert "container" in aspect_names, (
        "Profiler should emit container aspect when include_tables=False"
    )


def test_profiler_emits_dataplatforminstance_when_include_tables_false():
    """When include_tables=False with a platform_instance, profiled datasets
    should get the dataPlatformInstance aspect for correct UI grouping."""
    profiler = make_profiler(
        include_tables=False,
        platform_instance="my-instance",
    )
    tables = make_tables()

    with patch.object(profiler, "generate_profile_workunits", return_value=iter([])):
        workunits = list(profiler.get_workunits(tables))

    aspect_names = collect_aspect_names(workunits)
    assert "dataPlatformInstance" in aspect_names, (
        "Profiler should emit dataPlatformInstance aspect when include_tables=False"
    )


def test_profiler_does_not_emit_container_when_include_tables_true():
    """When include_tables=True, the profiler should NOT emit container aspects
    because gen_dataset_workunits already does that."""
    profiler = make_profiler(
        include_tables=True,
        platform_instance="my-instance",
    )
    tables = make_tables()

    with patch.object(profiler, "generate_profile_workunits", return_value=iter([])):
        workunits = list(profiler.get_workunits(tables))

    aspect_names = collect_aspect_names(workunits)
    assert "container" not in aspect_names, (
        "Profiler should not emit container aspect when include_tables=True"
    )
    assert "dataPlatformInstance" not in aspect_names, (
        "Profiler should not emit dataPlatformInstance aspect when include_tables=True"
    )


def test_profiler_container_links_to_correct_schema():
    """The container aspect should reference the correct schema container key."""
    profiler = make_profiler(
        include_tables=False,
        platform_instance="my-instance",
    )
    tables = make_tables(db="test_db", schema="seed")

    with patch.object(profiler, "generate_profile_workunits", return_value=iter([])):
        workunits = list(profiler.get_workunits(tables))

    container_wus = [
        wu
        for wu in workunits
        if hasattr(wu.metadata, "aspectName") and wu.metadata.aspectName == "container"
    ]
    assert len(container_wus) == 1
    mcp = container_wus[0].metadata
    assert isinstance(mcp, MetadataChangeProposalWrapper)
    container_aspect = mcp.aspect
    assert isinstance(container_aspect, ContainerClass)
    # The container URN should be a valid container reference (GUID-based)
    assert container_aspect.container.startswith("urn:li:container:")


def test_profiler_no_container_without_platform_instance():
    """Even without platform_instance, container aspects should be emitted
    when include_tables=False (the dataPlatformInstance aspect won't be emitted)."""
    profiler = make_profiler(
        include_tables=False,
        platform_instance=None,
    )
    tables = make_tables()

    with patch.object(profiler, "generate_profile_workunits", return_value=iter([])):
        workunits = list(profiler.get_workunits(tables))

    aspect_names = collect_aspect_names(workunits)
    assert "container" in aspect_names, (
        "Profiler should emit container aspect even without platform_instance"
    )
    # dataPlatformInstance should NOT be emitted when there's no platform_instance
    assert "dataPlatformInstance" not in aspect_names
