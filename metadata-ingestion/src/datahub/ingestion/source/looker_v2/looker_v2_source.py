"""
Looker V2 Combined Source.

A unified source combining functionality from both looker and lookml sources.
Provides hybrid extraction: API-based for BI assets, file-based for views.
"""

from __future__ import annotations

import json
import logging
import re
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from json import JSONDecodeError
from pathlib import Path
from typing import Any, Dict, FrozenSet, Iterable, List, Optional, Set, Tuple, Union

from looker_sdk.error import SDKError
from looker_sdk.sdk.api40.models import (
    Dashboard as LookerAPIDashboard,
    DashboardBase as LookerDashboardBase,
    DashboardElement,
    Folder,
    FolderBase,
    Look,
    LookmlModelExplore,
    LookmlModelExploreField,
    LookWithQuery,
    Query,
    WriteQuery,
)

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigurationError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    BIAssetSubTypes,
    BIContainerSubTypes,
    DatasetSubTypes,
    SourceCapabilityModifier,
)
from datahub.ingestion.source.git.git_import import GitClone
from datahub.ingestion.source.looker.looker_common import (
    InputFieldElement,
    LookerExploreRegistry,
    LookerFolderKey,
    LookerUserRegistry,
    LookerUtil,
    LookerViewId,
    ViewField,
    ViewFieldType,
    gen_model_key,
    gen_project_key,
    get_urn_looker_dashboard_id,
    get_urn_looker_element_id,
)
from datahub.ingestion.source.looker.looker_lib_wrapper import LookerAPI
from datahub.ingestion.source.looker_v2 import looker_v2_usage as looker_usage
from datahub.ingestion.source.looker_v2.looker_v2_config import (
    LookerConnectionDefinition,
    LookerV2Config,
    LookerV2GitInfo,
)
from datahub.ingestion.source.looker_v2.looker_v2_context import LookerV2Context
from datahub.ingestion.source.looker_v2.looker_v2_folder_processor import (
    LookerFolderProcessor,
)
from datahub.ingestion.source.looker_v2.looker_v2_look_processor import (
    LookerLookProcessor,
)
from datahub.ingestion.source.looker_v2.looker_v2_pdt_graph_parser import (
    parse_pdt_graph,
)
from datahub.ingestion.source.looker_v2.looker_v2_report import LookerV2SourceReport
from datahub.ingestion.source.looker_v2.looker_v2_usage_extractor import (
    LookerUsageExtractor,
)
from datahub.ingestion.source.looker_v2.lookml_manifest_parser import ManifestParser
from datahub.ingestion.source.looker_v2.lookml_parser import (
    LookMLParser,
    ParsedView,
)
from datahub.ingestion.source.looker_v2.lookml_view_discovery import (
    ViewDiscovery,
    ViewDiscoveryResult,
)
from datahub.ingestion.source.looker_v2.lookml_view_refinement_handler import (
    RefinementHandler,
    merge_additive_parameters,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    ChartTypeClass,
    EmbedClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    InputFieldClass,
    InputFieldsClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)
from datahub.metadata.urns import CorpUserUrn, SchemaFieldUrn, TagUrn
from datahub.sdk.chart import Chart
from datahub.sdk.container import Container
from datahub.sdk.dashboard import Dashboard
from datahub.sdk.dataset import Dataset, UpstreamInputType
from datahub.sdk.entity import Entity
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    SqlParsingResult,
    create_lineage_sql_parsed_result,
)
from datahub.utilities.backpressure_aware_executor import BackpressureAwareExecutor
from datahub.utilities.sentinels import Unset, unset
from datahub.utilities.url_util import remove_port_from_url

logger = logging.getLogger(__name__)

_REQUIRED_API_PERMISSIONS: FrozenSet[str] = frozenset(
    {
        "access_data",
        "explore",
        "see_lookml",
        "see_lookml_dashboards",
        "see_looks",
        "see_user_dashboards",
    }
)


def _platform_names_have_2_parts(platform: str) -> bool:
    """Return True for platforms whose fully-qualified names use only 2 parts (db.table)."""
    return platform in {"hive", "mysql", "athena"}


def _generate_fully_qualified_name(
    sql_table_name: str,
    connection_def: LookerConnectionDefinition,
    reporter: LookerV2SourceReport,
    view_name: str,
) -> str:
    """Expand a sql_table_name to a fully qualified name using the connection defaults.

    Looker sql_table_name values may have 1, 2, or 3 dot-separated parts. This
    function fills in missing db/schema segments from the connection definition and
    normalises the result to lowercase.

    - 3-part names: returned as-is (or 2-part for 2-part platforms).
    - 2-part names: db prefix added for 3-part platforms.
    - 1-part names: db + schema prefix added (or just db for 2-part platforms).
    - 4+ parts: a warning is emitted and the name is returned lowercased unchanged.

    Args:
        sql_table_name: Raw table name from LookML (e.g. "my_table" or "db.schema.tbl").
        connection_def: Connection definition providing default_db and default_schema.
        reporter: Used to emit warnings when the name is malformed.
        view_name: View name for warning context only.

    Returns:
        Lowercase fully qualified table name.
    """
    parts = len(sql_table_name.split("."))

    if parts == 3:
        if _platform_names_have_2_parts(connection_def.platform):
            sql_table_name = ".".join(sql_table_name.split(".")[1:])
        return sql_table_name.lower()

    if parts == 1:
        if _platform_names_have_2_parts(connection_def.platform):
            dataset_name = f"{connection_def.default_db}.{sql_table_name}"
        else:
            dataset_name = f"{connection_def.default_db}.{connection_def.default_schema}.{sql_table_name}"
        return dataset_name.lower()

    if parts == 2:
        if _platform_names_have_2_parts(connection_def.platform):
            return sql_table_name.lower()
        dataset_name = f"{connection_def.default_db}.{sql_table_name}"
        return dataset_name.lower()

    reporter.report_warning(
        title="Malformed Table Name",
        message="Table name has more than 3 parts.",
        context=f"view-name: {view_name}, table-name: {sql_table_name}",
    )
    return sql_table_name.lower()


@dataclass
class DashboardProcessingResult:
    """All output produced while processing a single dashboard.

    Attributes:
        entities: Dashboard and chart entities ready for emission.
        extra_mcps: Additional MCPs (InputFields, EmbedUrl, etc.) not in entity.as_mcps().
        dashboard_usage: Usage tracking payload for the usage stats stage, or None.
    """

    entities: List[Entity]
    extra_mcps: List[MetadataChangeProposalWrapper]
    dashboard_usage: Optional[looker_usage.LookerDashboardForUsage]
    dashboard_id: str
    start_time: datetime
    end_time: datetime


@dataclass
class ViewProcessingResult:
    """All output produced while processing a single LookML view.

    Attributes:
        entities: View Dataset entities (may include refinement chain nodes).
        lineage_extracted: True if at least one upstream lineage edge was resolved.
        is_unreachable: True for views not referenced by any explore.
    """

    entities: List[Entity]
    view_id: LookerViewId
    lineage_extracted: bool = False
    is_unreachable: bool = False


@platform_name("Looker")
@support_status(SupportStatus.INCUBATING)
@config_class(LookerV2Config)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Use the `platform_instance` field")
@capability(
    SourceCapability.OWNERSHIP, "Enabled by default, configured using `extract_owners`"
)
@capability(SourceCapability.LINEAGE_COARSE, "Supported by default")
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default, configured using `extract_column_level_lineage`",
)
@capability(
    SourceCapability.USAGE_STATS,
    "Enabled by default, configured using `extract_usage_history`",
)
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
@capability(
    SourceCapability.CONTAINERS,
    "Enabled by default",
    subtype_modifier=[
        SourceCapabilityModifier.LOOKML_MODEL,
        SourceCapabilityModifier.LOOKML_PROJECT,
        SourceCapabilityModifier.LOOKER_FOLDER,
    ],
)
class LookerV2Source(TestableSource, StatefulIngestionSourceBase):
    """
    Looker V2 Combined Source.

    This plugin extracts the following:
    - Looker dashboards, charts, and explores (via Looker API)
    - LookML views including reachable, unreachable, and refined views (via file parsing)
    - Lineage between views and warehouse tables
    - Multi-project dependencies
    - Ownership and usage statistics

    This is a combined source that replaces both the `looker` and `lookml` sources.
    """

    platform = "looker"
    # V1's lookml source uses platform "looker" for views (via LookerCommonConfig.platform_name).
    # We must match this for backward-compatible URNs.
    VIEW_PLATFORM = "looker"

    def __init__(self, config: LookerV2Config, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.reporter = LookerV2SourceReport()
        self.ctx = ctx

        self.looker_api: LookerAPI = LookerAPI(self.config)

        # Build shared context (will be fully populated during _initialize)
        self._ctx = LookerV2Context(
            config=self.config,
            looker_api=self.looker_api,
            reporter=self.reporter,
            pipeline_ctx=self.ctx,
            platform=self.platform,
        )

        self._folder_proc = LookerFolderProcessor(self._ctx)

        # Registries for caching
        self.user_registry: LookerUserRegistry = LookerUserRegistry(
            self.looker_api,
            self.reporter,
        )
        # LookerV2Config has the same bases as LookerDashboardSourceConfig; structurally compatible.
        self.explore_registry: LookerExploreRegistry = LookerExploreRegistry(
            self.looker_api,
            self.reporter,
            self.config,  # type: ignore[arg-type]
        )

        # State tracking
        self.reachable_look_registry: Set[str] = set()
        self.processed_folders: List[str] = []
        self.chart_urns: Set[str] = set()

        self._look_proc = LookerLookProcessor(
            ctx=self._ctx,
            folder_proc=self._folder_proc,
            user_registry=self.user_registry,
            reachable_look_registry=self.reachable_look_registry,
        )

        self._usage_extractor = LookerUsageExtractor(self._ctx, self.user_registry)

        # View tracking
        self._view_discovery_result: Optional[ViewDiscoveryResult] = None
        self._parsed_views: Dict[str, ParsedView] = {}

        # Cloned git repos (temp directories)
        self._temp_dirs: List[str] = []

        # Cached refinement handler (created once during initialization)
        self._refinement_handler: Optional[RefinementHandler] = None

    @property
    def _dashboard_chart_platform_instance(self) -> Optional[str]:
        """Return the platform_instance for dashboard/chart URNs.

        Only non-None when include_platform_instance_in_urns is True, because most
        deployments migrating from V1 did not have platform_instance set in dashboard URNs.
        """
        if self.config.include_platform_instance_in_urns:
            return self.config.platform_instance
        return None

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        """Test connection to Looker API and LookML repository."""
        test_report = TestConnectionReport()

        try:
            config = LookerV2Config.parse_obj_allow_extras(config_dict)

            # Test Looker API connection
            api = LookerAPI(config)
            permissions = api.get_available_permissions()

            test_report.basic_connectivity = CapabilityReport(capable=True)
            test_report.capability_report = {}

            if _REQUIRED_API_PERMISSIONS.issubset(permissions):
                test_report.capability_report[SourceCapability.DESCRIPTIONS] = (
                    CapabilityReport(capable=True)
                )
            else:
                missing = _REQUIRED_API_PERMISSIONS - permissions
                test_report.capability_report[SourceCapability.DESCRIPTIONS] = (
                    CapabilityReport(
                        capable=False,
                        failure_reason=f"Missing permissions: {', '.join(missing)}",
                    )
                )

            # Test LookML access if configured
            if config.base_folder:
                lookml_path = Path(config.base_folder)
                if lookml_path.exists():
                    test_report.capability_report[SourceCapability.LINEAGE_COARSE] = (
                        CapabilityReport(capable=True)
                    )
                else:
                    test_report.capability_report[SourceCapability.LINEAGE_COARSE] = (
                        CapabilityReport(
                            capable=False,
                            failure_reason=f"LookML path not found: {config.base_folder}",
                        )
                    )

        except Exception as e:
            logger.exception(f"Failed to test connection: {e}")
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )

        return test_report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """Main entry point for generating work units."""
        try:
            # Stage 1: Initialize and populate registries
            with self._stage_timer("initialize"):
                yield from self._initialize()

            # Stage 2: Process dashboards and charts
            if self.config.extract_dashboards:
                with self._stage_timer("process_dashboards"):
                    yield from self._process_dashboards()

            # Stage 3: Process standalone looks
            if self.config.extract_looks:
                with self._stage_timer("process_looks"):
                    yield from self._look_proc.process()

            # Stage 4: Process explores
            if self.config.extract_explores:
                with self._stage_timer("process_explores"):
                    yield from self._process_explores()

            # Stage 5: Process views (reachable and unreachable)
            if self.config.extract_views:
                with self._stage_timer("process_views"):
                    yield from self._process_views()
                # Free parsed view cache after view processing
                self._parsed_views.clear()

            # Free caches after views are done
            self._ctx.explore_cache.clear()
            self._ctx.parsed_view_file_cache.clear()

            # Stage 6: Emit tag entities (Dimension, Measure, Temporal, group_label)
            if self.config.tag_measures_and_dimensions:
                for tag_urn, tag_props in LookerUtil.tag_definitions.items():
                    yield MetadataChangeProposalWrapper(
                        entityUrn=tag_urn,
                        aspect=tag_props,
                    ).as_workunit()

            # Stage 7: Emit Looker user ID → email platform resource
            with self._stage_timer("user_id_mapping"):
                yield from auto_workunit(
                    self.user_registry.to_platform_resource(
                        self.config.platform_instance
                    )
                )

            # Stage 8: Extract usage statistics
            if self.config.extract_usage_history:
                with self._stage_timer("usage_extraction"):
                    yield from self._usage_extractor.process()
                # Free usage data after extraction
                self._ctx.dashboards_for_usage.clear()

        finally:
            # Cleanup temp directories
            self._cleanup_temp_dirs()

    def _stage_timer(self, stage_name: str) -> Any:
        """Context manager that records wall-clock duration for a named pipeline stage."""
        from contextlib import contextmanager

        @contextmanager
        def timer() -> Any:
            start = time.time()
            try:
                yield
            finally:
                duration = time.time() - start
                self.reporter.stage_timings_seconds[stage_name] = round(duration, 3)
                logger.info(f"Stage '{stage_name}' completed in {duration:.2f}s")

        return timer()

    def _initialize(self) -> Iterable[MetadataWorkUnit]:
        """Initialize source: clone repos, populate registries."""
        # Clone git repos if needed
        if self.config.git_info:
            with self._stage_timer("init.clone_main"):
                self._clone_main_project()

        # Clone dependency repos from config
        for project_name, dep in self.config.project_dependencies.items():
            if isinstance(dep, LookerV2GitInfo):
                with self._stage_timer(f"init.clone_dep.{project_name}"):
                    self._clone_dependency(project_name, dep)
            else:
                self._ctx.resolved_project_paths[project_name] = dep

        # Parse manifest.lkml and resolve additional dependencies
        if self.config.base_folder:
            with self._stage_timer("init.manifest"):
                self._resolve_manifest_dependencies()

        # Populate registries
        with self._stage_timer("init.populate_registries"):
            self._populate_registries()

        # Auto-discover connections from Looker API
        with self._stage_timer("init.discover_connections"):
            self._auto_discover_connections()

        # Fetch PDT lineage graphs
        with self._stage_timer("init.pdt_lineage"):
            self._fetch_pdt_lineage()

        # Discover views (includes parallel explore API calls)
        if self.config.extract_views and self.config.base_folder:
            with self._stage_timer("init.discover_views"):
                self._discover_views()

        # Initialize refinement handler once for all views
        if self.config.process_refinements and self.config.base_folder:
            with self._stage_timer("init.refinement_handler"):
                self._refinement_handler = RefinementHandler(
                    base_folder=self.config.base_folder,
                    project_name=self.config.project_name or "default",
                    project_dependencies=self._ctx.resolved_project_paths or None,
                )

        # Emit project container
        if self.config.project_name and self.config.base_folder:
            yield from self._emit_project_container()

    def _resolve_manifest_dependencies(self) -> None:
        """Parse manifest.lkml and resolve project dependencies."""
        if not self.config.base_folder:
            return

        manifest_parser = ManifestParser(
            base_folder=self.config.base_folder,
            project_name=self.config.project_name,
            project_dependencies=self._ctx.resolved_project_paths,
            git_info=self.config.git_info,
        )

        try:
            # Parse and resolve all dependencies
            resolved = manifest_parser.parse_and_resolve()

            # Update resolved paths
            for name, path in resolved.items():
                if name not in self._ctx.resolved_project_paths:
                    self._ctx.resolved_project_paths[name] = str(path)

            # Update project name if discovered from manifest
            if manifest_parser.project_name and not self.config.project_name:
                self.config.project_name = manifest_parser.project_name

            # Store constants for later use in LookML parsing
            for name, const in manifest_parser.constants.items():
                if name not in self.config.lookml_constants:
                    self.config.lookml_constants[name] = const.value

            # Track temp dirs for cleanup
            self._temp_dirs.extend(manifest_parser.consume_temp_dirs())

            logger.info(
                f"Manifest resolution complete: "
                f"projects={list(resolved.keys())}, "
                f"constants={len(manifest_parser.constants)}"
            )

        except (OSError, ValueError, KeyError) as e:
            self.reporter.report_warning(
                title="Manifest Resolution Failed",
                message="Failed to parse manifest.lkml",
                context=f"{self.config.base_folder}: {e}",
            )

    def _clone_main_project(self) -> None:
        """Clone the main LookML project from git."""
        if not self.config.git_info:
            return

        git_info = self.config.git_info
        temp_dir = tempfile.mkdtemp(prefix="looker_v2_main_")
        self._temp_dirs.append(temp_dir)

        try:
            git_clone = GitClone(str(temp_dir))
            checkout_dir = git_clone.clone(
                ssh_key=git_info.deploy_key,  # Already SecretStr
                repo_url=git_info.repo,
                branch=git_info.branch,
            )
            self.config.base_folder = str(checkout_dir)
            logger.info(f"Cloned main project to {checkout_dir}")
        except Exception as e:
            self.reporter.report_failure(
                title="Git Clone Failed",
                message="Failed to clone main LookML project from git.",
                context=f"{git_info.repo}: {e}",
            )
            raise

    def _clone_dependency(self, project_name: str, git_info: LookerV2GitInfo) -> None:
        """Clone a dependency project from git."""
        temp_dir = tempfile.mkdtemp(prefix=f"looker_v2_dep_{project_name}_")
        self._temp_dirs.append(temp_dir)

        try:
            git_clone = GitClone(str(temp_dir))
            checkout_dir = git_clone.clone(
                ssh_key=git_info.deploy_key,
                repo_url=git_info.repo,
                branch=git_info.branch,
            )
            self._ctx.resolved_project_paths[project_name] = str(checkout_dir)
            logger.info(f"Cloned dependency '{project_name}' to {checkout_dir}")
        except Exception as e:
            self.reporter.report_warning(
                title="Dependency Clone Failed",
                message="Failed to clone LookML dependency project from git. Views from this dependency will be missing.",
                context=f"project={project_name}, repo={git_info.repo}: {e}",
            )

    def _cleanup_temp_dirs(self) -> None:
        """Cleanup temporary directories."""
        import shutil

        for temp_dir in self._temp_dirs:
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                logger.warning(f"Failed to cleanup temp dir {temp_dir}: {e}")

    def _populate_registries(self) -> None:
        """Bulk fetch all reference data upfront."""
        logger.info("Populating registries...")

        self._folder_proc.build_registry()

        # Fetch all models
        try:
            models = self.looker_api.all_lookml_models()
            for model in models:
                if model.name:
                    self._ctx.model_registry[model.name] = model
                    self.reporter.models_discovered += 1
        except SDKError as e:
            self.reporter.report_failure(
                title="Model Fetch Failed",
                message="Could not fetch LookML models from Looker API. No explores, views, or lineage will be extracted.",
                context=str(e),
            )

    def _auto_discover_connections(self) -> None:
        """Auto-discover connection_to_platform_map from Looker API."""
        try:
            connections = self.looker_api.all_connections()
        except SDKError as e:
            logger.warning(f"Failed to fetch connections for auto-discovery: {e}")
            return

        failed_connections: List[str] = []

        for conn in connections:
            if not conn.name:
                continue
            # Manual config entries take precedence
            if conn.name in self.config.connection_to_platform_map:
                continue
            try:
                connection_def = LookerConnectionDefinition.from_looker_connection(conn)
                self.config.connection_to_platform_map[conn.name] = connection_def
                logger.info(
                    f"Auto-discovered connection '{conn.name}' -> "
                    f"platform='{connection_def.platform}', "
                    f"db='{connection_def.default_db}'"
                )
            except ConfigurationError as e:
                failed_connections.append(conn.name)
                logger.warning(f"Could not auto-discover connection '{conn.name}': {e}")

        if failed_connections:
            self.reporter.report_warning(
                title="Connection Auto-Discovery Incomplete",
                message="Could not auto-discover some connections; add them manually to connection_to_platform_map if needed.",
                context=f"{len(failed_connections)} failed: {', '.join(failed_connections)}",
            )

    def _fetch_pdt_lineage(self) -> None:
        """Fetch PDT dependency graphs for all models via API."""
        if not self.config.enable_api_sql_lineage:
            return

        for model_name in self._ctx.model_registry:
            if not self.config.model_pattern.allowed(model_name):
                continue
            try:
                graph = self.looker_api.graph_derived_tables_for_model(model_name)
                if graph and isinstance(graph.graph_text, str) and graph.graph_text:
                    edges = parse_pdt_graph(graph.graph_text)
                    for edge in edges:
                        self._ctx.pdt_upstream_map.setdefault(
                            edge.view_name, []
                        ).append(edge)
                    self.reporter.pdt_graphs_fetched += 1
                    self.reporter.pdt_edges_discovered += len(edges)
            except Exception as e:
                logger.warning(
                    f"Failed to fetch PDT graph for model '{model_name}': {e}"
                )
                self.reporter.report_warning(
                    title="PDT Graph Fetch Failed",
                    message="Could not fetch PDT graph for model",
                    context=f"{model_name}: {e}",
                )

    def _build_view_to_explore_map(self) -> None:
        """Build a view_name → (model_name, explore_name) mapping from the explore cache.

        Used by the API SQL lineage path to know which explore to query when calling
        generate_sql_query for a given view. The first explore that references a view wins.
        """
        for (model_name, explore_name), explore in self._ctx.explore_cache.items():
            # Primary view of the explore
            primary_view = explore.view_name or explore.name
            if primary_view and primary_view not in self._ctx.view_to_explore_map:
                self._ctx.view_to_explore_map[primary_view] = (model_name, explore_name)
            # Joined views
            if explore.joins:
                for join in explore.joins:
                    join_view = join.from_ or join.name
                    if join_view and join_view not in self._ctx.view_to_explore_map:
                        self._ctx.view_to_explore_map[join_view] = (
                            model_name,
                            explore_name,
                        )

        logger.debug(
            f"Built view_to_explore_map with {len(self._ctx.view_to_explore_map)} entries"
        )

    def _get_view_fields_for_api(
        self, view_name: str, model_name: str, explore_name: str
    ) -> List[str]:
        """Return field names in Looker API format (<view>.<field>) for a given view.

        Uses the already-cached explore data so no extra API calls are made.
        Only dimensions and measures belonging to this view are included;
        one representative field per dimension group is used to avoid redundancy.
        """
        cache_key = (model_name, explore_name)
        explore = self._ctx.explore_cache.get(cache_key)
        if not explore or not explore.fields:
            return []

        fields: List[str] = []
        seen_dim_groups: Set[str] = set()

        if explore.fields.dimensions:
            for dim in explore.fields.dimensions:
                if not dim.name or not hasattr(dim, "view") or dim.view != view_name:
                    continue
                if dim.dimension_group:
                    if dim.dimension_group in seen_dim_groups:
                        continue
                    seen_dim_groups.add(dim.dimension_group)
                fields.append(dim.name)

        if explore.fields.measures:
            for measure in explore.fields.measures:
                if (
                    measure.name
                    and hasattr(measure, "view")
                    and measure.view == view_name
                ):
                    fields.append(measure.name)

        return fields

    def _generate_sql_for_chunk(
        self,
        view_name: str,
        model_name: str,
        explore_name: str,
        fields: List[str],
    ) -> Optional[str]:
        """Call generate_sql_query for a subset of fields and return the SQL string."""
        query = WriteQuery(
            model=model_name,
            view=explore_name,
            fields=fields,
            filters={},
            limit="1",
        )
        try:
            return self.looker_api.generate_sql_query(query)
        except Exception as e:
            logger.warning(
                f"generate_sql_query failed for view '{view_name}' "
                f"(explore={explore_name}, {len(fields)} fields): {e}"
            )
            return None

    def _parse_sql_to_lineage(
        self,
        sql: str,
        conn_def: LookerConnectionDefinition,
        view_name: str,
    ) -> Optional[SqlParsingResult]:
        """Parse rendered SQL into a SqlParsingResult. Returns None on failure."""
        try:
            spr = create_lineage_sql_parsed_result(
                query=sql,
                default_schema=conn_def.default_schema,
                default_db=conn_def.default_db,
                platform=conn_def.platform,
                platform_instance=conn_def.platform_instance,
                env=self.config.env,
                graph=self.ctx.graph,
            )
            if spr.debug_info.table_error:
                logger.debug(
                    f"SQL parse table error for view '{view_name}': {spr.debug_info.table_error}"
                )
            return spr
        except Exception as e:
            logger.warning(f"SQL parse failed for view '{view_name}': {e}")
            return None

    def _resolve_view_lineage_via_api(
        self,
        view_name: str,
        conn_def: LookerConnectionDefinition,
    ) -> Optional[List[UpstreamInputType]]:
        """Resolve lineage for a reachable derived-table view using the Looker API.

        Calls generate_sql_query to get fully-rendered SQL, parses it for table-level
        and column-level lineage, then merges with any PDT graph edges already in
        _pdt_upstream_map. Returns a list of UpstreamInputType items (dataset URNs +
        FineGrainedLineageClass objects) or None if the view is not in the explore map.
        """
        explore_entry = self._ctx.view_to_explore_map.get(view_name)
        if not explore_entry:
            return None

        model_name, explore_name = explore_entry
        fields = self._get_view_fields_for_api(view_name, model_name, explore_name)
        if not fields:
            logger.debug(
                f"No API fields found for view '{view_name}'; skipping API SQL lineage"
            )
            return None

        chunk_size = self.config.api_sql_lineage_field_chunk_size
        chunks = [fields[i : i + chunk_size] for i in range(0, len(fields), chunk_size)]

        all_in_tables: Set[str] = set()
        all_cll: List[FineGrainedLineageClass] = []

        for chunk in chunks:
            sql = self._generate_sql_for_chunk(
                view_name, model_name, explore_name, chunk
            )
            if sql is None:
                if not self.config.api_sql_lineage_individual_field_fallback:
                    continue
                # Individual field fallback
                for field in chunk:
                    field_sql = self._generate_sql_for_chunk(
                        view_name, model_name, explore_name, [field]
                    )
                    if field_sql:
                        spr = self._parse_sql_to_lineage(field_sql, conn_def, view_name)
                        if spr:
                            all_in_tables.update(spr.in_tables)
                            if spr.column_lineage:
                                all_cll.extend(
                                    self._cll_to_fine_grained(spr.column_lineage)
                                )
                continue

            spr = self._parse_sql_to_lineage(sql, conn_def, view_name)
            if spr:
                all_in_tables.update(spr.in_tables)
                if spr.column_lineage:
                    all_cll.extend(self._cll_to_fine_grained(spr.column_lineage))

        # Merge PDT graph edges (table-level only, adds PDT-to-PDT view URNs not
        # captured by the SQL parser when the PDT name differs from the raw table name)
        pdt_edges = self._ctx.pdt_upstream_map.get(view_name, [])
        if pdt_edges:
            self.reporter.lineage_via_pdt_graph += 1
        for edge in pdt_edges:
            if edge.is_database_table:
                try:
                    fqn = _generate_fully_qualified_name(
                        sql_table_name=edge.upstream_name,
                        connection_def=conn_def,
                        reporter=self.reporter,
                        view_name=view_name,
                    )
                    all_in_tables.add(
                        builder.make_dataset_urn_with_platform_instance(
                            platform=conn_def.platform,
                            name=fqn,
                            platform_instance=conn_def.platform_instance,
                            env=self.config.env,
                        )
                    )
                except (ValueError, KeyError):
                    pass
            else:
                upstream_model = edge.upstream_model or edge.model_name
                view_urn = self._get_view_urn(edge.upstream_name, upstream_model)
                if view_urn:
                    all_in_tables.add(str(view_urn))

        if not all_in_tables and not all_cll:
            return None

        result: List[UpstreamInputType] = list(all_in_tables)
        result.extend(all_cll)
        return result

    def _cll_to_fine_grained(
        self, column_lineage: List[ColumnLineageInfo]
    ) -> List[FineGrainedLineageClass]:
        """Convert SqlParsingResult.column_lineage to FineGrainedLineageClass objects."""
        fgl_list: List[FineGrainedLineageClass] = []
        for cll in column_lineage:
            try:
                if not cll.downstream.table or not cll.downstream.column:
                    continue
                downstream_urn = SchemaFieldUrn(
                    cll.downstream.table, cll.downstream.column
                ).urn()
                upstream_urns = [
                    SchemaFieldUrn(u.table, u.column).urn()
                    for u in cll.upstreams
                    if u.table and u.column
                ]
                if upstream_urns:
                    fgl_list.append(
                        FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                            upstreams=upstream_urns,
                            downstreams=[downstream_urn],
                        )
                    )
            except Exception as e:
                logger.debug(f"Failed to convert column lineage entry: {e}")
        return fgl_list

    def _discover_views(self) -> None:
        """Discover and categorize views from LookML files."""
        if not self.config.base_folder:
            return

        # Get explore view names from API (parallel)
        with self._stage_timer("init.explore_view_names"):
            explore_views = self._get_explore_view_names()

        # Build view→explore map for API SQL lineage (uses the now-populated _explore_cache)
        if self.config.enable_api_sql_lineage:
            self._build_view_to_explore_map()

        # Run view discovery
        discovery = ViewDiscovery(
            base_folder=self.config.base_folder,
            project_name=self.config.project_name or "default",
            project_dependencies=self._ctx.resolved_project_paths,
        )

        self._view_discovery_result = discovery.discover(explore_views)

        # Update report
        self.reporter.views_discovered = len(
            self._view_discovery_result.reachable_views
        ) + len(self._view_discovery_result.unreachable_views)
        self.reporter.views_reachable = len(self._view_discovery_result.reachable_views)
        self.reporter.views_unreachable = len(
            self._view_discovery_result.unreachable_views
        )

        # Report orphaned files
        for file_path in self._view_discovery_result.orphaned_files:
            self.reporter.report_orphaned_file(file_path)

        logger.info(
            f"View discovery: reachable={self.reporter.views_reachable}, "
            f"unreachable={self.reporter.views_unreachable}, "
            f"orphaned={self.reporter.orphaned_view_files_count}"
        )

    def _get_explore_view_names(self) -> FrozenSet[str]:
        """Fetch all explore details in parallel and collect the referenced view names.

        Results are cached in _explore_cache to avoid duplicate API calls during
        the explore-processing stage.
        """
        explore_pairs: List[Tuple[str, str]] = [
            (model.name, explore_basic.name)
            for model in self._ctx.model_registry.values()
            if model.explores and model.name
            for explore_basic in model.explores
            if explore_basic.name
        ]

        def fetch_explore(
            model_name: str, explore_name: str
        ) -> Optional[Tuple[str, str, LookmlModelExplore]]:
            try:
                explore = self.looker_api.lookml_model_explore(model_name, explore_name)
                return (model_name, explore_name, explore)
            except SDKError as e:
                logger.warning(
                    f"Failed to fetch explore {model_name}.{explore_name}: {e}"
                )
                return None

        view_names: Set[str] = set()
        for future in BackpressureAwareExecutor.map(
            fn=fetch_explore,
            args_list=[(m, e) for m, e in explore_pairs],
            max_workers=self.config.max_concurrent_requests,
            max_pending=self.config.max_concurrent_requests * 2,
        ):
            result = future.result()
            if result is None:
                continue
            model_name, explore_name, explore = result
            self._ctx.explore_cache[(model_name, explore_name)] = explore
            if explore.view_name:
                view_names.add(explore.view_name)
            elif explore.name:
                view_names.add(explore.name)
            if explore.joins:
                for join in explore.joins:
                    if join.from_:
                        view_names.add(join.from_)
                    elif join.name:
                        view_names.add(join.name)

        return frozenset(view_names)

    def _emit_project_container(self) -> Iterable[MetadataWorkUnit]:
        """Emit the LookML project as a container."""
        project_name = self.config.project_name or "default"

        container = Container(
            container_key=gen_project_key(self.config, project_name),
            subtype=BIContainerSubTypes.LOOKML_PROJECT,
            display_name=project_name,
        )

        for mcp in container.as_mcps():
            yield mcp.as_workunit()

        self.reporter.projects_processed += 1

    def _process_dashboards(self) -> Iterable[MetadataWorkUnit]:
        """Process all dashboards."""
        logger.info("Processing dashboards...")

        try:
            dashboards: List[Union[LookerDashboardBase, LookerAPIDashboard]] = list(
                self.looker_api.all_dashboards(fields="id,title,folder")
            )
        except SDKError as e:
            self.reporter.report_failure("fetch_dashboards", str(e))
            return

        # Also fetch deleted dashboards if configured
        if self.config.include_deleted:
            try:
                deleted_dashboards = self.looker_api.search_dashboards(
                    fields="id,title,folder", deleted="true"
                )
                existing_ids = {d.id for d in dashboards}
                for d in deleted_dashboards:
                    if d.id not in existing_ids:
                        dashboards.append(d)
            except SDKError as e:
                logger.warning(f"Failed to fetch deleted dashboards: {e}")

        self.reporter.dashboards_discovered = len(dashboards)

        # Filter dashboards
        dashboard_ids = []
        for dashboard in dashboards:
            if dashboard.id and dashboard.title:
                if self.config.dashboard_pattern.allowed(dashboard.title):
                    dashboard_ids.append(dashboard.id)
                else:
                    self.reporter.dashboards_filtered.append(dashboard.title)

        # Process dashboards in parallel
        def process_dashboard(dashboard_id: str) -> Optional[DashboardProcessingResult]:
            return self._process_single_dashboard(dashboard_id)

        for future in BackpressureAwareExecutor.map(
            fn=process_dashboard,
            args_list=[(did,) for did in dashboard_ids],
            max_workers=self.config.max_concurrent_requests,
            max_pending=self.config.max_concurrent_requests * 2,
        ):
            try:
                result = future.result()
                if result:
                    for entity in result.entities:
                        for mcp in entity.as_mcps():
                            yield mcp.as_workunit()
                    # Emit any extra MCPs (input fields, etc.)
                    for extra_mcp in result.extra_mcps:
                        yield extra_mcp.as_workunit()
                    # Collect usage data
                    if result.dashboard_usage:
                        self._ctx.dashboards_for_usage.append(result.dashboard_usage)
                    self.reporter.dashboards_scanned += 1
            except (SDKError, ValueError, KeyError, TypeError) as e:
                self.reporter.report_warning(
                    title="Dashboard Processing Failed",
                    message="Error processing dashboard",
                    context=str(e),
                )

    def _process_single_dashboard(
        self, dashboard_id: str
    ) -> Optional[DashboardProcessingResult]:
        """Process a single dashboard."""
        start_time = datetime.now(timezone.utc)
        entities: List[Entity] = []
        extra_mcps: List[MetadataChangeProposalWrapper] = []

        dashboard_fields = [
            "id",
            "title",
            "description",
            "user_id",
            "folder",
            "dashboard_elements",
            "dashboard_elements.look_id",
            "dashboard_elements.look.id",
            "dashboard_elements.look.view_count",
            "dashboard_filters",
            "created_at",
            "updated_at",
            "deleted",
            "deleted_at",
            "deleter_id",
            "hidden",
            "last_updater_id",
        ]

        if self.config.extract_usage_history:
            dashboard_fields.extend(["favorite_count", "view_count", "last_viewed_at"])

        try:
            api_dashboard = self.looker_api.dashboard(
                dashboard_id, fields=dashboard_fields
            )
        except SDKError as e:
            logger.warning(f"Failed to fetch dashboard {dashboard_id}: {e}")
            self.reporter.report_warning(
                title="Dashboard Fetch Failed",
                message="Could not fetch dashboard from Looker API.",
                context=f"dashboard_id={dashboard_id}: {e}",
            )
            return None

        if not api_dashboard:
            return None

        # Skip hidden dashboards
        if getattr(api_dashboard, "hidden", False) and not self.config.include_deleted:
            return None

        # Skip personal folders
        if self._folder_proc.should_skip_personal_folder(api_dashboard.folder):
            return None

        # Check folder path pattern
        if api_dashboard.folder:
            folder_path = self._folder_proc.get_folder_path(api_dashboard.folder)
            if not self.config.folder_path_pattern.allowed(folder_path):
                self.reporter.dashboards_filtered.append(
                    f"{api_dashboard.title} (folder: {folder_path})"
                )
                return None

        # Create dashboard entity and process its elements
        dashboard = self._create_dashboard_entity(api_dashboard)
        if dashboard:
            entities.append(dashboard)
            self._process_dashboard_elements(
                api_dashboard, dashboard, entities, extra_mcps
            )

        # Process folder container
        if api_dashboard.folder and api_dashboard.folder.id:
            folder_entities = self._folder_proc.get_folder_container(
                api_dashboard.folder
            )
            entities.extend(folder_entities)

        dashboard_usage = self._build_dashboard_usage(api_dashboard)

        end_time = datetime.now(timezone.utc)
        return DashboardProcessingResult(
            entities=entities,
            extra_mcps=extra_mcps,
            dashboard_usage=dashboard_usage,
            dashboard_id=dashboard_id,
            start_time=start_time,
            end_time=end_time,
        )

    def _process_dashboard_elements(
        self,
        api_dashboard: LookerAPIDashboard,
        dashboard: "Dashboard",
        entities: List[Entity],
        extra_mcps: List[MetadataChangeProposalWrapper],
    ) -> None:
        """Process dashboard elements (charts) and collect input fields."""
        if not api_dashboard.dashboard_elements:
            return

        for element in api_dashboard.dashboard_elements:
            if element.id and not self.config.chart_pattern.allowed(str(element.id)):
                self.reporter.charts_dropped += 1
                continue
            chart = self._create_chart_entity(element, api_dashboard)
            if chart:
                self._add_chart_explore_lineage(chart, element)
                entities.append(chart)
                self.reporter.charts_scanned += 1
                extra_mcps.extend(self._extract_chart_input_fields(element, chart))

        self.reporter.charts_discovered += len(api_dashboard.dashboard_elements)

        all_input_fields: List[InputFieldClass] = []
        for element in api_dashboard.dashboard_elements:
            all_input_fields.extend(self._get_enriched_input_fields(element))
        if all_input_fields:
            extra_mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=str(dashboard.urn),
                    aspect=InputFieldsClass(fields=all_input_fields),
                )
            )

        if self.config.extract_embed_urls and self.config.external_base_url:
            base_url = remove_port_from_url(self.config.external_base_url)
            embed_url = f"{base_url}/embed/dashboards/{api_dashboard.id}"
            extra_mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=str(dashboard.urn),
                    aspect=EmbedClass(renderUrl=embed_url),
                )
            )

    def _build_dashboard_usage(
        self, api_dashboard: LookerAPIDashboard
    ) -> Optional[looker_usage.LookerDashboardForUsage]:
        """Build usage tracking data for a dashboard."""
        if not self.config.extract_usage_history:
            return None

        last_viewed_ms = None
        last_viewed_at = getattr(api_dashboard, "last_viewed_at", None)
        if last_viewed_at is not None:
            try:
                last_viewed_ms = round(last_viewed_at.timestamp() * 1000)
            except (AttributeError, OSError):
                pass

        usage_looks: List[looker_usage.LookerChartForUsage] = []
        if api_dashboard.dashboard_elements:
            for element in api_dashboard.dashboard_elements:
                if element.look_id and element.look:
                    usage_looks.append(
                        looker_usage.LookerChartForUsage(
                            id=str(element.look_id),
                            view_count=getattr(element.look, "view_count", None),
                        )
                    )
                    self.reachable_look_registry.add(str(element.look_id))

        return looker_usage.LookerDashboardForUsage(
            id=str(api_dashboard.id),
            view_count=getattr(api_dashboard, "view_count", None),
            favorite_count=getattr(api_dashboard, "favorite_count", None),
            last_viewed_at=last_viewed_ms,
            looks=usage_looks,
        )

    def _create_dashboard_entity(
        self, api_dashboard: LookerAPIDashboard
    ) -> Optional[Dashboard]:
        """Create a Dashboard entity from API response."""
        if not api_dashboard.id or not api_dashboard.title:
            return None

        # Get chart URNs
        chart_urns = []
        if api_dashboard.dashboard_elements:
            for element in api_dashboard.dashboard_elements:
                if element.id:
                    chart_urn = builder.make_chart_urn(
                        platform=self.platform,
                        name=get_urn_looker_element_id(str(element.id)),
                        platform_instance=self._dashboard_chart_platform_instance,
                    )
                    chart_urns.append(chart_urn)

        # Build dashboard - name is used for URN, display_name for title
        # Use get_urn_looker_dashboard_id to match V1 URN pattern: dashboards.{id}
        parent_container: Union[LookerFolderKey, Unset] = unset
        if api_dashboard.folder and api_dashboard.folder.id:
            parent_container = self._folder_proc.gen_folder_key(api_dashboard.folder.id)

        dashboard_url = None
        if self.config.external_base_url:
            base_url = remove_port_from_url(self.config.external_base_url)
            dashboard_url = f"{base_url}/dashboards/{api_dashboard.id}"

        dashboard = Dashboard(
            name=get_urn_looker_dashboard_id(str(api_dashboard.id)),
            display_name=api_dashboard.title,
            platform=self.platform,
            platform_instance=self._dashboard_chart_platform_instance,
            description=api_dashboard.description,
            external_url=api_dashboard.link if hasattr(api_dashboard, "link") else None,
            dashboard_url=dashboard_url,
            subtype=BIAssetSubTypes.DASHBOARD,
            charts=chart_urns,
            parent_container=parent_container,
        )

        # Add ownership
        if api_dashboard.user_id and self.config.extract_owners:
            user = self.user_registry.get_by_id(str(api_dashboard.user_id))
            if user and user.email:
                dashboard.add_owner((CorpUserUrn(user.email), "DATAOWNER"))

        return dashboard

    def _get_chart_query(self, element: DashboardElement) -> Optional[Query]:
        """Get query for a chart element with fallbacks (matching V1)."""
        query = element.query
        if not query and element.look and hasattr(element.look, "query"):
            query = element.look.query
        if not query and element.result_maker and element.result_maker.query:
            query = element.result_maker.query
        return query

    def _compute_upstream_fields(self, element: DashboardElement) -> Set[str]:
        """Compute upstream_fields from query fields, filters, and dynamic fields."""
        upstream_fields: Set[str] = set()
        query = self._get_chart_query(element)
        if query:
            for f in query.fields or []:
                if f:
                    upstream_fields.add(f)
            for f in query.filters or {}:
                if f:
                    upstream_fields.add(f)
            try:
                dynamic_fields = json.loads(query.dynamic_fields or "[]")
            except (JSONDecodeError, TypeError):
                dynamic_fields = []
            for field in dynamic_fields:
                for key in ("table_calculation", "measure", "dimension"):
                    if key in field and field[key]:
                        upstream_fields.add(field[key])
                if "measure" in field:
                    based_on = field.get("based_on")
                    if based_on:
                        upstream_fields.add(based_on)
        return upstream_fields

    def _get_chart_url(self, element: DashboardElement) -> Optional[str]:
        """Generate chart URL with query slug fallbacks (matching V1)."""
        if not self.config.external_base_url:
            return None
        base_url = remove_port_from_url(self.config.external_base_url)
        if element.look_id:
            return f"{base_url}/looks/{element.look_id}"
        slug = None
        if element.query and element.query.slug:
            slug = element.query.slug
        elif (
            element.look
            and hasattr(element.look, "query")
            and element.look.query
            and element.look.query.slug
        ):
            slug = element.look.query.slug
        elif (
            element.result_maker
            and element.result_maker.query
            and element.result_maker.query.slug
        ):
            slug = element.result_maker.query.slug
        if slug:
            return f"{base_url}/x/{slug}"
        return None

    def _create_chart_entity(
        self, element: DashboardElement, parent_dashboard: LookerAPIDashboard
    ) -> Optional[Chart]:
        """Create a Chart entity from a dashboard element."""
        if not element.id:
            return None

        chart_id = get_urn_looker_element_id(str(element.id))

        # Determine chart type
        chart_type_value = None
        if element.type:
            chart_type_value = self._map_chart_type(element.type)

        # Charts are children of the same folder as their parent dashboard
        parent_container: Union[LookerFolderKey, Unset] = unset
        if parent_dashboard.folder and parent_dashboard.folder.id:
            parent_container = self._folder_proc.gen_folder_key(
                parent_dashboard.folder.id
            )

        chart_url = self._get_chart_url(element)
        upstream_fields = self._compute_upstream_fields(element)

        chart = Chart(
            name=chart_id,
            display_name=element.title or f"Chart {element.id}",
            platform=self.platform,
            platform_instance=self._dashboard_chart_platform_instance,
            description=element.subtitle_text,
            subtype=BIAssetSubTypes.LOOKER_LOOK,
            chart_type=chart_type_value,
            chart_url=chart_url,
            custom_properties={
                "upstream_fields": ",".join(sorted(upstream_fields))
                if upstream_fields
                else ""
            },
            parent_container=parent_container,
        )

        # Inherit ownership from parent dashboard
        if (
            parent_dashboard.user_id
            and hasattr(self.config, "extract_owners")
            and self.config.extract_owners
        ):
            user = self.user_registry.get_by_id(str(parent_dashboard.user_id))
            if user and user.email:
                chart.add_owner((CorpUserUrn(user.email), "DATAOWNER"))

        # Track for usage
        self.chart_urns.add(str(chart.urn))

        return chart

    def _add_chart_explore_lineage(
        self, chart: Chart, element: DashboardElement
    ) -> None:
        """Add chart-to-explore lineage based on the element's query."""
        query = self._get_chart_query(element)

        added_explores: Set[str] = set()
        if query and query.model and query.view:
            explore_urn = builder.make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=f"{query.model}.explore.{query.view}",
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )
            chart.add_input_dataset(explore_urn)
            added_explores.add(explore_urn)

        # Also check result_maker filterables for additional explores
        if element.result_maker and element.result_maker.filterables:
            for filterable in element.result_maker.filterables:
                if filterable.view and filterable.model:
                    explore_urn = builder.make_dataset_urn_with_platform_instance(
                        platform=self.platform,
                        name=f"{filterable.model}.explore.{filterable.view}",
                        platform_instance=self.config.platform_instance,
                        env=self.config.env,
                    )
                    if explore_urn not in added_explores:
                        chart.add_input_dataset(explore_urn)
                        added_explores.add(explore_urn)

    def _map_chart_type(self, looker_type: str) -> Optional[str]:
        """Map Looker chart type to DataHub chart type."""
        type_mapping = {
            "looker_column": ChartTypeClass.BAR,
            "looker_scatter": ChartTypeClass.SCATTER,
            "looker_line": ChartTypeClass.LINE,
            "looker_area": ChartTypeClass.AREA,
            "looker_pie": ChartTypeClass.PIE,
            "looker_donut_multiples": ChartTypeClass.PIE,
            "looker_funnel": ChartTypeClass.BAR,
            "looker_timeline": ChartTypeClass.BAR,
            "looker_waterfall": ChartTypeClass.BAR,
            "looker_single_record": ChartTypeClass.TABLE,
            "looker_grid": ChartTypeClass.TABLE,
            "looker_boxplot": ChartTypeClass.BOX_PLOT,
            "area": ChartTypeClass.AREA,
            "bar": ChartTypeClass.BAR,
            "column": ChartTypeClass.BAR,
            "line": ChartTypeClass.LINE,
            "pie": ChartTypeClass.PIE,
            "scatter": ChartTypeClass.SCATTER,
            "table": ChartTypeClass.TABLE,
            "text": ChartTypeClass.TEXT,
            "single_value": ChartTypeClass.TEXT,
        }
        return type_mapping.get(looker_type.lower() if looker_type else "")

    def _gen_folder_key(self, folder_id: str) -> LookerFolderKey:
        """Generate a folder key with all required parameters."""
        return LookerFolderKey(
            folder_id=folder_id,
            env=self.config.env,
            platform=self.config.platform_name,
            instance=self.config.platform_instance,
        )

    def _get_folder_ancestors(self, folder_id: str) -> List[FolderBase]:
        """Get ancestor folders using the pre-fetched registry when available."""
        if self._ctx.folder_registry:
            # Walk up the tree using the registry
            ancestors: List[FolderBase] = []
            current_folder = self._ctx.folder_registry.get(folder_id)
            if current_folder is None:
                return []
            visited: set = set()
            parent_id = current_folder.parent_id
            while parent_id and parent_id not in visited:
                visited.add(parent_id)
                parent = self._ctx.folder_registry.get(parent_id)
                if parent is None:
                    break
                ancestors.insert(0, parent)
                parent_id = parent.parent_id
            return ancestors
        # Fall back to API when registry is empty (e.g., pre-fetch failed)
        try:
            return [
                FolderBase(
                    id=f.id,
                    name=f.name,
                    parent_id=f.parent_id,
                    is_personal=f.is_personal,
                    is_personal_descendant=f.is_personal_descendant,
                )
                for f in self.looker_api.folder_ancestors(folder_id=folder_id)
            ]
        except SDKError as e:
            logger.warning(f"Failed to fetch folder ancestors for {folder_id}: {e}")
            self.reporter.report_warning(
                title="Folder Ancestors Fetch Failed",
                message="Could not fetch folder ancestors. Folder container hierarchy may be incomplete.",
                context=f"folder_id={folder_id}: {e}",
            )
            return []

    def _get_folder_container(self, folder: Union[FolderBase, Folder]) -> List[Entity]:
        """Get or create folder container entity with full ancestor chain."""
        if not folder.id:
            return []

        entities: List[Entity] = []

        # Emit ancestor chain
        ancestors = self._folder_proc.get_folder_ancestors(folder.id)
        for ancestor in ancestors:
            if ancestor.id and ancestor.id not in self.processed_folders:
                self.processed_folders.append(ancestor.id)
                parent_key = None
                if ancestor.parent_id:
                    parent_key = self._folder_proc.gen_folder_key(ancestor.parent_id)
                container = Container(
                    container_key=self._folder_proc.gen_folder_key(ancestor.id),
                    subtype=BIContainerSubTypes.LOOKER_FOLDER,
                    display_name=ancestor.name or f"Folder {ancestor.id}",
                    parent_container=parent_key,
                )
                entities.append(container)

        # Emit the folder itself
        if folder.id not in self.processed_folders:
            self.processed_folders.append(folder.id)
            parent_key = None
            if hasattr(folder, "parent_id") and folder.parent_id:
                parent_key = self._folder_proc.gen_folder_key(folder.parent_id)
            container = Container(
                container_key=self._folder_proc.gen_folder_key(folder.id),
                subtype=BIContainerSubTypes.LOOKER_FOLDER,
                display_name=folder.name or f"Folder {folder.id}",
                parent_container=parent_key,
            )
            entities.append(container)

        return entities

    def _should_skip_personal_folder(
        self, folder: Optional[Union[FolderBase, Folder]]
    ) -> bool:
        """Check if a folder is personal and should be skipped."""
        if not self.config.skip_personal_folders:
            return False
        if folder is None or not folder.id:
            return False

        # First check the folder object directly
        if getattr(folder, "is_personal", None) or getattr(
            folder, "is_personal_descendant", None
        ):
            return True

        # Check ancestor chain for personal folder flags
        for ancestor in self._folder_proc.get_folder_ancestors(folder.id):
            if getattr(ancestor, "is_personal", None) or getattr(
                ancestor, "is_personal_descendant", None
            ):
                return True

        return False

    def _get_folder_path(self, folder: Union[FolderBase, Folder]) -> str:
        """Get full folder path using ancestor chain."""
        if not folder.id:
            return folder.name or ""
        ancestors = self._folder_proc.get_folder_ancestors(folder.id)
        path_parts = [a.name for a in ancestors if a.name]
        if folder.name:
            path_parts.append(folder.name)
        return "/".join(path_parts) if path_parts else (folder.name or "")

    def _process_looks(self) -> Iterable[MetadataWorkUnit]:
        """Process standalone looks."""
        logger.info("Processing looks...")

        look_fields = ["id", "title", "description", "user_id", "folder", "query"]

        # Respect include_deleted config
        deleted_param = None if self.config.include_deleted else False

        try:
            looks = self.looker_api.search_looks(
                fields=look_fields, deleted=deleted_param
            )
        except SDKError as e:
            self.reporter.report_failure("fetch_looks", str(e))
            return

        self.reporter.looks_discovered = len(looks)

        for look in looks:
            if look.id:
                # Skip looks already processed via dashboards
                if str(look.id) in self.reachable_look_registry:
                    continue

                # Skip personal folders
                folder = getattr(look, "folder", None)
                if self._folder_proc.should_skip_personal_folder(folder):
                    continue

                chart = self._create_look_entity(look)
                if chart:
                    for mcp in chart.as_mcps():
                        yield mcp.as_workunit()
                    self.reporter.looks_scanned += 1

    def _create_look_entity(self, look: Union[Look, LookWithQuery]) -> Optional[Chart]:
        """Create a Chart entity from a Look."""
        if not look.id:
            return None

        # Use V1-compatible URN pattern: dashboard_elements.looks_{id}
        chart = Chart(
            name=get_urn_looker_element_id(f"looks_{look.id}"),
            display_name=look.title or f"Look {look.id}",
            platform=self.platform,
            platform_instance=self._dashboard_chart_platform_instance,
            description=look.description,
            subtype=BIAssetSubTypes.LOOKER_LOOK,
        )

        # Add ownership
        if look.user_id and self.config.extract_owners:
            user = self.user_registry.get_by_id(str(look.user_id))
            if user and user.email:
                chart.add_owner((CorpUserUrn(user.email), "DATAOWNER"))

        return chart

    def _process_explores(self) -> Iterable[MetadataWorkUnit]:
        """Process LookML explores."""
        logger.info("Processing explores...")

        explores_to_process: List[Tuple[str, str]] = []

        for model_name, model in self._ctx.model_registry.items():
            if not self.config.model_pattern.allowed(model_name):
                continue

            if model.explores:
                for explore in model.explores:
                    if not explore.name:
                        continue
                    if not self.config.explore_pattern.allowed(explore.name):
                        continue
                    if (
                        self.config.emit_used_explores_only
                        and (
                            model_name,
                            explore.name,
                        )
                        not in self._ctx.reachable_explores
                    ):
                        self.reporter.explores_skipped += 1
                        continue
                    explores_to_process.append((model_name, explore.name))

        self.reporter.explores_discovered = len(explores_to_process)

        # Emit model containers (matching V1 behavior)
        emitted_models: Set[str] = set()
        for model_name, _ in explores_to_process:
            if model_name not in emitted_models:
                model_key = gen_model_key(self.config, model_name)
                model_container = Container(
                    container_key=model_key,
                    display_name=model_name,
                    subtype=BIContainerSubTypes.LOOKML_MODEL,
                )
                for mcp in model_container.as_mcps():
                    yield mcp.as_workunit()
                emitted_models.add(model_name)

        # Process explores in parallel
        def process_explore(
            model: str, explore: str
        ) -> Optional[Tuple[Entity, List[MetadataChangeProposalWrapper]]]:
            return self._process_single_explore(model, explore)

        for future in BackpressureAwareExecutor.map(
            fn=process_explore,
            args_list=explores_to_process,
            max_workers=self.config.max_concurrent_requests,
            max_pending=self.config.max_concurrent_requests * 2,
        ):
            try:
                result = future.result()
                if result:
                    entity, extra_mcps = result
                    for mcp in entity.as_mcps():
                        yield mcp.as_workunit()
                    for extra_mcp in extra_mcps:
                        yield extra_mcp.as_workunit()
                    self.reporter.explores_scanned += 1
            except (SDKError, ValueError, KeyError, TypeError) as e:
                self.reporter.report_warning(
                    title="Explore Processing Failed",
                    message="Error processing explore",
                    context=str(e),
                )

    def _process_single_explore(
        self, model_name: str, explore_name: str
    ) -> Optional[Tuple[Entity, List[MetadataChangeProposalWrapper]]]:
        """Process a single explore with schema fields."""
        # Check cache first (populated during view discovery)
        cache_key = (model_name, explore_name)
        if cache_key in self._ctx.explore_cache:
            explore = self._ctx.explore_cache[cache_key]
            self.reporter.explore_cache_hits += 1
        else:
            self.reporter.explore_cache_misses += 1
            try:
                explore = self.looker_api.lookml_model_explore(model_name, explore_name)
            except SDKError as e:
                logger.warning(
                    f"Failed to fetch explore {model_name}.{explore_name}: {e}"
                )
                return None

        if not explore:
            return None

        explore_id = f"{model_name}.explore.{explore_name}"

        # Get model key for container
        model_key = gen_model_key(self.config, model_name)

        dataset = Dataset(
            name=explore_id,
            display_name=explore.label or explore_name,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            description=explore.description,
            subtype=DatasetSubTypes.LOOKER_EXPLORE,
            parent_container=model_key,
        )

        # Build upstream lineage: explore → views
        upstream_view_urns = self._get_explore_upstream_view_urns(explore, model_name)
        if upstream_view_urns:
            dataset.set_upstreams(upstream_view_urns)

        # Apply explore-level tags (from LookML `tags` property on explore)
        if explore.tags:
            dataset.set_tags([TagUrn(tag) for tag in explore.tags])

        extra_mcps: List[MetadataChangeProposalWrapper] = []

        # Extract schema fields from explore API response
        view_fields = self._extract_explore_fields(explore)
        if view_fields:
            fields, primary_keys = LookerUtil._get_fields_and_primary_keys(
                view_fields=view_fields,
                reporter=self.reporter,
                tag_measures_and_dimensions=self.config.tag_measures_and_dimensions,
            )
            if fields:
                schema = SchemaMetadataClass(
                    schemaName=explore_id,
                    platform=f"urn:li:dataPlatform:{self.platform}",
                    version=0,
                    fields=fields,
                    primaryKeys=primary_keys,
                    hash="",
                    platformSchema=OtherSchemaClass(rawSchema=""),
                )
                extra_mcps.append(
                    MetadataChangeProposalWrapper(
                        entityUrn=str(dataset.urn),
                        aspect=schema,
                    )
                )

        # Emit embed URL for explore
        if self.config.extract_embed_urls and self.config.external_base_url:
            base_url = remove_port_from_url(self.config.external_base_url)
            embed_url = f"{base_url}/embed/explore/{model_name}/{explore_name}"
            extra_mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=str(dataset.urn),
                    aspect=EmbedClass(renderUrl=embed_url),
                )
            )

        return (dataset, extra_mcps)

    def _extract_explore_fields(self, explore: LookmlModelExplore) -> List[ViewField]:
        """Extract ViewField objects from explore API response fields."""
        view_fields: List[ViewField] = []

        if not explore.fields:
            return view_fields

        if explore.fields.dimensions:
            for dim in explore.fields.dimensions:
                if not dim.name:
                    continue
                view_fields.append(
                    ViewField(
                        name=dim.name,
                        label=dim.label_short,
                        type=dim.type or "string",
                        description=dim.description or "",
                        field_type=(
                            ViewFieldType.DIMENSION_GROUP
                            if dim.dimension_group
                            else ViewFieldType.DIMENSION
                        ),
                        is_primary_key=dim.primary_key or False,
                        tags=list(dim.tags) if dim.tags else [],
                        group_label=dim.field_group_label,
                    )
                )

        if explore.fields.measures:
            for measure in explore.fields.measures:
                if not measure.name:
                    continue
                view_fields.append(
                    ViewField(
                        name=measure.name,
                        label=measure.label_short,
                        type=measure.type or "string",
                        description=measure.description or "",
                        field_type=ViewFieldType.MEASURE,
                        tags=list(measure.tags) if measure.tags else [],
                        group_label=measure.field_group_label,
                    )
                )

        if explore.fields.parameters:
            for param in explore.fields.parameters:
                if not param.name:
                    continue
                view_fields.append(
                    ViewField(
                        name=param.name,
                        label=param.label_short,
                        type=param.type or "string",
                        description=param.description or "",
                        field_type=ViewFieldType.UNKNOWN,
                    )
                )

        return view_fields

    def _get_enriched_input_fields(
        self, element: DashboardElement
    ) -> List[InputFieldClass]:
        """Get enriched input fields from a dashboard element, using explore metadata."""
        query = self._get_chart_query(element)
        if not query:
            return []

        input_fields = self._get_input_fields_from_query(query)
        if not input_fields:
            return []

        # Look up explore for field enrichment
        explore = None
        explore_urn = None
        explore_fields_map: Dict[str, Any] = {}
        if query.model and query.view:
            cache_key = (query.model, query.view)
            explore = self._ctx.explore_cache.get(cache_key)
            explore_urn = builder.make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=f"{query.model}.explore.{query.view}",
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )
            if explore and explore.fields:
                for dim in explore.fields.dimensions or []:
                    if dim.name:
                        explore_fields_map[dim.name] = dim
                for meas in explore.fields.measures or []:
                    if meas.name:
                        explore_fields_map[meas.name] = meas

        input_field_classes = []
        for field in input_fields:
            # Try to enrich with explore metadata
            if explore_urn and field.name in explore_fields_map:
                api_field = explore_fields_map[field.name]
                view_field = self._api_field_to_view_field(api_field)
                schema_field = LookerUtil.view_field_to_schema_field(
                    view_field,
                    self.reporter,
                    self.config.tag_measures_and_dimensions,
                )
                parent_urn = explore_urn
            elif field.view_field:
                schema_field = LookerUtil.view_field_to_schema_field(
                    field.view_field, self.reporter
                )
                parent_urn = explore_urn or ""
            else:
                schema_field = SchemaFieldClass(
                    fieldPath=field.name,
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="string",
                )
                parent_urn = explore_urn or ""

            if parent_urn:
                input_field_classes.append(
                    InputFieldClass(
                        schemaFieldUrn=builder.make_schema_field_urn(
                            parent_urn=parent_urn,
                            field_path=field.name,
                        ),
                        schemaField=schema_field,
                    )
                )

        return input_field_classes

    def _api_field_to_view_field(self, api_field: LookmlModelExploreField) -> ViewField:
        """Convert a LookML API explore field to a ViewField."""
        field_type = ViewFieldType.DIMENSION
        if api_field.category == "measure":
            field_type = ViewFieldType.MEASURE
        elif api_field.dimension_group:
            field_type = ViewFieldType.DIMENSION_GROUP

        return ViewField(
            name=api_field.name or "",
            label=api_field.label_short,
            description=api_field.description or "",
            type=api_field.type or "",
            field_type=field_type,
            is_primary_key=api_field.primary_key or False,
            tags=list(api_field.tags) if api_field.tags else [],
            group_label=api_field.field_group_label,
        )

    def _extract_chart_input_fields(
        self, element: DashboardElement, chart: Chart
    ) -> List[MetadataChangeProposalWrapper]:
        """Extract input fields from a dashboard element's query."""
        # Track explores reached via chart queries (with fallbacks)
        query = self._get_chart_query(element)
        if query and query.model and query.view:
            key = (query.model, query.view)
            if key not in self._ctx.reachable_explores:
                self._ctx.reachable_explores[key] = []
            self._ctx.reachable_explores[key].append(f"chart:{element.id}")
        # Also track from result_maker filterables
        if element.result_maker and element.result_maker.filterables:
            for filterable in element.result_maker.filterables:
                if filterable.view and filterable.model:
                    key = (filterable.model, filterable.view)
                    if key not in self._ctx.reachable_explores:
                        self._ctx.reachable_explores[key] = []
                    self._ctx.reachable_explores[key].append(f"chart:{element.id}")

        input_field_classes = self._get_enriched_input_fields(element)
        if not input_field_classes:
            return []

        return [
            MetadataChangeProposalWrapper(
                entityUrn=str(chart.urn),
                aspect=InputFieldsClass(fields=input_field_classes),
            )
        ]

    def _get_input_fields_from_query(self, query: Query) -> List[InputFieldElement]:
        """Extract input fields from a Looker query."""
        result: List[InputFieldElement] = []

        # Parse dynamic fields (table calculations, custom measures/dimensions)
        try:
            dynamic_fields = json.loads(query.dynamic_fields or "[]")
        except (JSONDecodeError, TypeError):
            dynamic_fields = []

        for field in dynamic_fields:
            if "table_calculation" in field:
                result.append(
                    InputFieldElement(
                        name=field["table_calculation"],
                        view_field=ViewField(
                            name=field["table_calculation"],
                            label=field.get("label"),
                            field_type=ViewFieldType.UNKNOWN,
                            type="string",
                            description="",
                        ),
                    )
                )
            if "measure" in field:
                based_on = field.get("based_on")
                if based_on is not None:
                    result.append(
                        InputFieldElement(
                            name=based_on,
                            view_field=None,
                            model=query.model or "",
                            explore=query.view or "",
                        )
                    )
                result.append(
                    InputFieldElement(
                        name=field["measure"],
                        view_field=ViewField(
                            name=field["measure"],
                            label=field.get("label"),
                            field_type=ViewFieldType.MEASURE,
                            type="string",
                            description="",
                        ),
                    )
                )
            if "dimension" in field:
                result.append(
                    InputFieldElement(
                        name=field["dimension"],
                        view_field=ViewField(
                            name=field["dimension"],
                            label=field.get("label"),
                            field_type=ViewFieldType.DIMENSION,
                            type="string",
                            description="",
                        ),
                    )
                )

        # Query fields
        for f in query.fields or []:
            if f:
                result.append(
                    InputFieldElement(
                        name=f,
                        view_field=None,
                        model=query.model or "",
                        explore=query.view or "",
                    )
                )

        # Filter fields
        for f in query.filters or {}:
            if f:
                result.append(
                    InputFieldElement(
                        name=f,
                        view_field=None,
                        model=query.model or "",
                        explore=query.view or "",
                    )
                )

        return result

    def _extract_usage_stats(self) -> Iterable[MetadataWorkUnit]:
        """Extract usage statistics for dashboards and charts."""
        if not self._ctx.dashboards_for_usage:
            return

        stat_generator_config = looker_usage.StatGeneratorConfig(
            looker_api_wrapper=self.looker_api,
            looker_user_registry=self.user_registry,
            interval=self.config.extract_usage_history_for_interval,
            strip_user_ids_from_email=self.config.strip_user_ids_from_email,
            max_threads=self.config.max_concurrent_requests,
        )

        # Dashboard usage stats
        try:
            dashboard_stat_generator = looker_usage.create_dashboard_stat_generator(
                config=stat_generator_config,
                report=self.reporter,
                urn_builder=self._make_dashboard_urn,
                looker_dashboards=self._ctx.dashboards_for_usage,
            )
            emitted = 0
            for mcp in dashboard_stat_generator.generate_usage_stat_mcps():
                yield mcp.as_workunit()
                emitted += 1
            if emitted > 0:
                self.reporter.dashboards_with_usage += 1
        except (SDKError, ValueError, KeyError) as e:
            self.reporter.report_warning(
                title="Dashboard Usage Stats Failed",
                message="Error generating dashboard usage stats",
                context=str(e),
            )

        # Chart/Look usage stats
        looks: List[looker_usage.LookerChartForUsage] = []
        for dashboard in self._ctx.dashboards_for_usage:
            if hasattr(dashboard, "looks") and dashboard.looks:
                looks.extend(dashboard.looks)

        # Deduplicate looks
        seen_ids: Set[str] = set()
        unique_looks: List[looker_usage.LookerChartForUsage] = []
        for look in looks:
            if look.id and str(look.id) not in seen_ids:
                seen_ids.add(str(look.id))
                unique_looks.append(look)

        if unique_looks:
            try:
                chart_stat_generator = looker_usage.create_chart_stat_generator(
                    config=stat_generator_config,
                    report=self.reporter,
                    urn_builder=self._make_chart_urn,
                    looker_looks=unique_looks,
                )
                emitted = 0
                for mcp in chart_stat_generator.generate_usage_stat_mcps():
                    yield mcp.as_workunit()
                    emitted += 1
                if emitted > 0:
                    self.reporter.charts_with_usage += 1
            except (SDKError, ValueError, KeyError) as e:
                self.reporter.report_warning(
                    title="Chart Usage Stats Failed",
                    message="Error generating chart usage stats",
                    context=str(e),
                )

    def _make_dashboard_urn(self, dashboard_id: str) -> str:
        """Build dashboard URN, respecting include_platform_instance_in_urns."""
        platform_instance: Optional[str] = None
        if self.config.include_platform_instance_in_urns:
            platform_instance = self.config.platform_instance
        return builder.make_dashboard_urn(
            name=dashboard_id,
            platform=self.platform,
            platform_instance=platform_instance,
        )

    def _make_chart_urn(self, element_id: str) -> str:
        """Build chart URN, respecting include_platform_instance_in_urns."""
        platform_instance: Optional[str] = None
        if self.config.include_platform_instance_in_urns:
            platform_instance = self.config.platform_instance
        return builder.make_chart_urn(
            name=element_id,
            platform=self.platform,
            platform_instance=platform_instance,
        )

    def _get_explore_upstream_view_urns(
        self, explore: LookmlModelExplore, model_name: str
    ) -> List[UpstreamInputType]:
        """Get upstream view URNs for an explore (explore→view lineage)."""
        views: Set[str] = set()

        # Base view
        if explore.view_name and explore.view_name != explore.name:
            views.add(explore.view_name)
        elif explore.name:
            views.add(explore.name)

        # Joined views
        if explore.joins:
            for join in explore.joins:
                if join.from_:
                    views.add(join.from_)
                elif join.name:
                    views.add(join.name)

        urns: List[UpstreamInputType] = []
        for view_name in sorted(views):
            urn = self._get_view_urn(view_name, model_name)
            if urn:
                urns.append(urn)
        return urns

    def _get_view_urn(self, view_name: str, model_name: str) -> Optional[str]:
        """Resolve a view name to its dataset URN."""
        if self._view_discovery_result:
            file_path = self._view_discovery_result.view_to_file.get(view_name)
            project = self._view_discovery_result.view_to_project.get(
                view_name, self.config.project_name or "default"
            )
            if file_path:
                dataset_name = self._generate_view_name(view_name, file_path, project)
                return builder.make_dataset_urn_with_platform_instance(
                    platform=self.VIEW_PLATFORM,
                    name=dataset_name,
                    platform_instance=self.config.platform_instance,
                    env=self.config.env,
                )

        # Fallback: generate URN without file path info
        project = self.config.project_name or "default"
        view_id = LookerViewId(
            project_name=project,
            model_name=model_name,
            view_name=view_name,
            file_path="",
        )
        mapping = view_id.get_mapping(self.config)
        mapping.file_path = view_id.preprocess_file_path(mapping.file_path)
        dataset_name = self.config.view_naming_pattern.replace_variables(mapping)
        return builder.make_dataset_urn_with_platform_instance(
            platform=self.VIEW_PLATFORM,
            name=dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def _get_connection_for_view(
        self, view_name: str
    ) -> Optional[LookerConnectionDefinition]:
        """Find the Looker connection for a view by scanning the explore cache.

        The connection is needed to qualify table names for lineage (db, schema defaults).
        Returns None if no explore references this view or the connection is not mapped.
        """
        # Search explores that reference this view
        for (_model_name, _explore_name), explore in self._ctx.explore_cache.items():
            # Check if this explore's base view matches
            base_view = explore.view_name or explore.name
            matched = base_view == view_name

            # Check joined views
            if not matched and explore.joins:
                for join in explore.joins:
                    join_view = join.from_ or join.name
                    if join_view == view_name:
                        matched = True
                        break

            if matched and explore.connection_name:
                conn_def = self.config.connection_to_platform_map.get(
                    explore.connection_name
                )
                if conn_def:
                    return conn_def

        return None

    def _resolve_view_upstream_lineage(
        self, dataset: Dataset, parsed_view: ParsedView
    ) -> bool:
        """Resolve upstream lineage for a view and attach it to the dataset entity.

        Resolution priority:
        - sql_table_name views: direct table URN (no API needed).
        - derived_table_sql views (reachable, enable_api_sql_lineage=True):
            Combined PDT graph API + generate_sql_query → table-level + CLL.
        - derived_table_sql views (unreachable or enable_api_sql_lineage=False):
            SQL regex fallback (table-level only, no CLL).

        Returns:
            True if at least one upstream was attached; False otherwise.
        """
        conn_def = self._get_connection_for_view(parsed_view.name)
        if not conn_def:
            logger.debug(
                f"No connection found for view '{parsed_view.name}', skipping lineage"
            )
            return False

        upstream_urns: List[UpstreamInputType] = []

        if parsed_view.sql_table_name:
            table_name = parsed_view.sql_table_name.strip().rstrip(";").strip()
            if table_name and not table_name.startswith("$"):
                try:
                    fqn = _generate_fully_qualified_name(
                        sql_table_name=table_name,
                        connection_def=conn_def,
                        reporter=self.reporter,
                        view_name=parsed_view.name,
                    )
                    upstream_urns.append(
                        builder.make_dataset_urn_with_platform_instance(
                            platform=conn_def.platform,
                            name=fqn,
                            platform_instance=conn_def.platform_instance,
                            env=self.config.env,
                        )
                    )
                except (ValueError, KeyError) as e:
                    logger.warning(
                        f"Failed to resolve lineage for view '{parsed_view.name}' "
                        f"table '{table_name}': {e}"
                    )
                    self.reporter.report_warning(
                        title="Lineage Resolution Failed",
                        message="Could not resolve upstream table for view.",
                        context=f"view={parsed_view.name}, table={table_name}: {e}",
                    )

        elif parsed_view.derived_table_sql:
            api_result: Optional[List[UpstreamInputType]] = None
            if self.config.enable_api_sql_lineage:
                api_result = self._resolve_view_lineage_via_api(
                    parsed_view.name, conn_def
                )

            if api_result is not None:
                upstream_urns.extend(api_result)
                self.reporter.lineage_via_api += 1
            else:
                # Fallback: SQL regex (unreachable views or API disabled/failed)
                table_refs = LookMLParser.extract_table_refs_from_sql(
                    parsed_view.derived_table_sql
                )
                for table_ref in table_refs:
                    try:
                        fqn = _generate_fully_qualified_name(
                            sql_table_name=table_ref,
                            connection_def=conn_def,
                            reporter=self.reporter,
                            view_name=parsed_view.name,
                        )
                        upstream_urns.append(
                            builder.make_dataset_urn_with_platform_instance(
                                platform=conn_def.platform,
                                name=fqn,
                                platform_instance=conn_def.platform_instance,
                                env=self.config.env,
                            )
                        )
                    except (ValueError, KeyError) as e:
                        logger.warning(
                            f"Failed to resolve lineage for view '{parsed_view.name}' "
                            f"derived table ref '{table_ref}': {e}"
                        )
                        self.reporter.report_warning(
                            title="Derived Table Lineage Resolution Failed",
                            message="Could not resolve derived table upstream reference.",
                            context=f"view={parsed_view.name}, ref={table_ref}: {e}",
                        )
                self.reporter.lineage_via_file_parse += 1

            # Resolve ${view.SQL_TABLE_NAME} references in derived SQL
            upstream_urns.extend(
                self._resolve_liquid_view_refs(
                    parsed_view.derived_table_sql, conn_def, parsed_view.name
                )
            )

        if upstream_urns:
            dataset.set_upstreams(upstream_urns)
            self.reporter.lineage_edges_extracted += len(upstream_urns)
            return True

        return False

    _LIQUID_VIEW_REF_PATTERN = re.compile(r"\$\{(\w+)\.SQL_TABLE_NAME\}", re.IGNORECASE)

    def _resolve_liquid_view_refs(
        self,
        sql: str,
        conn_def: LookerConnectionDefinition,
        source_view_name: str,
    ) -> List[UpstreamInputType]:
        """Resolve ${view.SQL_TABLE_NAME} references in SQL to upstream URNs."""
        ref_view_names = self._LIQUID_VIEW_REF_PATTERN.findall(sql)
        if not ref_view_names or not self._view_discovery_result:
            return []

        upstream_urns: List[UpstreamInputType] = []
        for ref_view_name in ref_view_names:
            ref_file = self._view_discovery_result.view_to_file.get(ref_view_name)
            if not ref_file:
                continue
            ref_project = self._view_discovery_result.view_to_project.get(
                ref_view_name, self.config.project_name or "default"
            )
            try:
                cache_key = (ref_file, ref_project)
                ref_views = self._ctx.parsed_view_file_cache.get(cache_key)
                if ref_views is None:
                    parser = LookMLParser(
                        template_variables=self.config.liquid_variables,
                        constants=self.config.lookml_constants,
                        environment=self.config.looker_environment,
                    )
                    ref_views = parser.parse_view_file(ref_file, ref_project)
                    self._ctx.parsed_view_file_cache[cache_key] = ref_views
                for rv in ref_views:
                    if rv.name == ref_view_name and rv.sql_table_name:
                        table_name = rv.sql_table_name.strip().rstrip(";").strip()
                        if table_name and not table_name.startswith("$"):
                            fqn = _generate_fully_qualified_name(
                                sql_table_name=table_name,
                                connection_def=conn_def,
                                reporter=self.reporter,
                                view_name=ref_view_name,
                            )
                            upstream_urns.append(
                                builder.make_dataset_urn_with_platform_instance(
                                    platform=conn_def.platform,
                                    name=fqn,
                                    platform_instance=conn_def.platform_instance,
                                    env=self.config.env,
                                )
                            )
                        break
            except (OSError, ValueError, KeyError) as e:
                logger.warning(
                    f"Failed to resolve ${{'{ref_view_name}'.SQL_TABLE_NAME}} "
                    f"in view '{source_view_name}': {e}"
                )
        return upstream_urns

    def _process_views(self) -> Iterable[MetadataWorkUnit]:
        """Process LookML views (reachable and unreachable)."""
        logger.info("Processing views...")

        if not self._view_discovery_result:
            return

        # Process reachable views (use API for lineage)
        for view_name in self._view_discovery_result.reachable_views:
            if not self.config.view_pattern.allowed(view_name):
                self.reporter.views_filtered.append(view_name)
                continue

            result = self._process_reachable_view(view_name)
            if result:
                for entity in result.entities:
                    for mcp in entity.as_mcps():
                        yield mcp.as_workunit()
                self.reporter.views_emitted += 1
                if result.lineage_extracted:
                    self.reporter.lineage_via_api += 1

        # Process unreachable views (parse LookML)
        if self.config.emit_unreachable_views:
            for view_name in self._view_discovery_result.unreachable_views:
                if not self.config.view_pattern.allowed(view_name):
                    self.reporter.views_filtered.append(view_name)
                    continue

                result = self._process_unreachable_view(view_name)
                if result:
                    for entity in result.entities:
                        for mcp in entity.as_mcps():
                            yield mcp.as_workunit()
                    self.reporter.views_emitted += 1
                    if result.lineage_extracted:
                        self.reporter.lineage_via_file_parse += 1

    def _process_reachable_view(self, view_name: str) -> Optional[ViewProcessingResult]:
        """Process a reachable view (referenced by explores)."""
        if not self._view_discovery_result:
            return None

        file_path = self._view_discovery_result.view_to_file.get(view_name)
        project = self._view_discovery_result.view_to_project.get(view_name, "default")

        view_id = LookerViewId(
            project_name=project,
            model_name="",
            view_name=view_name,
            file_path=file_path or "",
        )

        project_key = gen_project_key(self.config, project)

        # Handle refinements
        entities, merged_view = self._process_view_with_refinements(
            view_name, file_path or "", project, project_key
        )
        if not entities:
            # Fallback: emit base view without refinements
            dataset = Dataset(
                name=self._generate_view_name(view_name, file_path or "", project),
                display_name=view_name,
                platform=self.VIEW_PLATFORM,
                platform_instance=self.config.platform_instance,
                subtype=DatasetSubTypes.VIEW,
                parent_container=project_key,
            )
            entities = [dataset]

        # Resolve upstream lineage from LookML file
        lineage_extracted = False
        if file_path:
            # Use merged_view if available (has refinement changes applied),
            # otherwise parse the raw file
            resolved_view = merged_view
            if not resolved_view:
                parser = LookMLParser(
                    template_variables=self.config.liquid_variables,
                    constants=self.config.lookml_constants,
                    environment=self.config.looker_environment,
                )
                parsed_views = parser.parse_view_file(file_path, project)
                for pv in parsed_views:
                    if pv.name == view_name and not pv.is_refinement:
                        resolved_view = pv
                        break

            if resolved_view:
                last_dataset = entities[-1]
                if isinstance(last_dataset, Dataset):
                    lineage_extracted = self._resolve_view_upstream_lineage(
                        last_dataset, resolved_view
                    )

        return ViewProcessingResult(
            entities=entities,
            view_id=view_id,
            lineage_extracted=lineage_extracted,
            is_unreachable=False,
        )

    def _process_unreachable_view(
        self, view_name: str
    ) -> Optional[ViewProcessingResult]:
        """Process an unreachable view (parse from LookML)."""
        if not self._view_discovery_result:
            return None

        file_path = self._view_discovery_result.view_to_file.get(view_name)
        project = self._view_discovery_result.view_to_project.get(view_name, "default")

        if not file_path:
            return None

        # Parse the view file
        parser = LookMLParser(
            template_variables=self.config.liquid_variables,
            constants=self.config.lookml_constants,
            environment=self.config.looker_environment,
        )
        parsed_views = parser.parse_view_file(file_path, project)

        parsed_view = None
        for pv in parsed_views:
            if pv.name == view_name:
                parsed_view = pv
                break

        if not parsed_view:
            return None

        view_id = LookerViewId(
            project_name=project,
            model_name="",
            view_name=view_name,
            file_path=file_path,
        )

        project_key = gen_project_key(self.config, project)

        # Handle refinements
        entities, _ = self._process_view_with_refinements(
            view_name, file_path, project, project_key
        )
        if not entities:
            # Fallback: emit base view
            dataset = Dataset(
                name=self._generate_view_name(view_name, file_path, project),
                display_name=view_name,
                platform=self.VIEW_PLATFORM,
                platform_instance=self.config.platform_instance,
                subtype=DatasetSubTypes.VIEW,
                parent_container=project_key,
            )
            entities = [dataset]

        # Resolve upstream lineage from parsed view
        lineage_extracted = False
        last_dataset = entities[-1]
        if isinstance(last_dataset, Dataset):
            lineage_extracted = self._resolve_view_upstream_lineage(
                last_dataset, parsed_view
            )

        return ViewProcessingResult(
            entities=entities,
            view_id=view_id,
            lineage_extracted=lineage_extracted,
            is_unreachable=True,
        )

    def _process_view_with_refinements(
        self,
        view_name: str,
        file_path: str,
        project: str,
        project_key: Any,
    ) -> Tuple[List[Entity], Optional[ParsedView]]:
        """
        Process a view with optional refinement expansion.

        Returns:
            Tuple of (entities, merged_view). merged_view is the final ParsedView
            with all refinements applied (in merged mode), or None.
        """
        if not self.config.process_refinements or not self._refinement_handler:
            return [], None

        try:
            chain = self._refinement_handler.find_refinements_for_view(view_name)
        except (OSError, ValueError, KeyError) as e:
            logger.warning(f"Refinement processing failed for {view_name}: {e}")
            self.reporter.report_warning(
                title="Refinement Processing Failed",
                message="Failed to process refinements for view",
                context=f"{view_name}: {e}",
            )
            return [], None

        if not chain or not chain.refinements:
            return [], None

        entities: List[Entity] = []
        merged_view: Optional[ParsedView] = None

        if self.config.expand_refinement_lineage:
            # Expanded mode: each refinement node is a separate entity
            prev_name: Optional[str] = None
            for node in chain.nodes:
                node_view_name = node.urn_suffix
                dataset_name = self._generate_view_name(
                    node_view_name, node.file_path, node.project_name
                )

                dataset = Dataset(
                    name=dataset_name,
                    display_name=node.display_name,
                    platform=self.VIEW_PLATFORM,
                    platform_instance=self.config.platform_instance,
                    subtype=DatasetSubTypes.VIEW,
                    parent_container=project_key,
                )

                # Add lineage to previous node in chain
                if prev_name is not None:
                    prev_urn = builder.make_dataset_urn_with_platform_instance(
                        platform=self.VIEW_PLATFORM,
                        name=prev_name,
                        platform_instance=self.config.platform_instance,
                    )
                    dataset.set_upstreams([prev_urn])

                entities.append(dataset)
                prev_name = dataset_name

                self.reporter.report_refinement(
                    view_name=node_view_name,
                    project=node.project_name,
                    fields_added=len(node.added_dimensions) + len(node.added_measures),
                    fields_modified=len(node.modified_fields),
                )
        else:
            # Merged mode (V1 compatible): merge refinements into base view
            base_node = chain.base
            if base_node and base_node.parsed_view:
                merged_view = base_node.parsed_view
                for ref_node in chain.refinements:
                    if ref_node.parsed_view:
                        merged_view = merge_additive_parameters(
                            merged_view, ref_node.parsed_view
                        )
                    self.reporter.report_refinement(
                        view_name=view_name,
                        project=ref_node.project_name,
                        fields_added=(
                            len(ref_node.added_dimensions)
                            + len(ref_node.added_measures)
                        ),
                        fields_modified=len(ref_node.modified_fields),
                    )

            # Emit single merged entity
            dataset = Dataset(
                name=self._generate_view_name(view_name, file_path, project),
                display_name=view_name,
                platform=self.VIEW_PLATFORM,
                platform_instance=self.config.platform_instance,
                subtype=DatasetSubTypes.VIEW,
                parent_container=project_key,
            )
            entities.append(dataset)

        return entities, merged_view

    def _generate_view_name(self, view_name: str, file_path: str, project: str) -> str:
        """Build the dataset name for a view using the configured view_naming_pattern.

        Converts the absolute file_path to a relative path within its owning project
        before substituting into the naming pattern, so URNs are stable regardless of
        where git repos are cloned.
        """
        # Convert absolute file path to relative path within the project
        relative_path = file_path
        if self.config.base_folder and file_path:
            try:
                relative_path = str(
                    Path(file_path).relative_to(self.config.base_folder)
                )
            except ValueError:
                # Check project dependencies
                if self._ctx.resolved_project_paths:
                    for dep_path in self._ctx.resolved_project_paths.values():
                        try:
                            relative_path = str(Path(file_path).relative_to(dep_path))
                            break
                        except ValueError:
                            continue

        view_id = LookerViewId(
            project_name=project,
            model_name="",
            view_name=view_name,
            file_path=relative_path,
        )
        mapping = view_id.get_mapping(self.config)
        mapping.file_path = view_id.preprocess_file_path(mapping.file_path)
        return self.config.view_naming_pattern.replace_variables(mapping)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        """Get work unit processors for auto work unit handling."""
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self,
                self.config,
                self.ctx,
            ).workunit_processor,
        ]

    def get_report(self) -> LookerV2SourceReport:
        """Get the source report."""
        return self.reporter

    def close(self) -> None:
        """Cleanup resources."""
        self._cleanup_temp_dirs()
        super().close()
