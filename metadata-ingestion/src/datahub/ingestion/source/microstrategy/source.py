"""
DataHub source for MicroStrategy.

Extracts metadata including:
- Projects (as containers)
- Folders (as nested containers)
- Dashboards/Dossiers  — subtype-routed: 14081 legacy doc, 14336 modern dossier
- Reports (as charts)
- Intelligent Cubes (as datasets)
- Datasets
- Ownership information
- Column-level lineage
"""

import logging
import re
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from dateutil import parser as date_parser

from datahub.emitter.mce_builder import (
    make_chart_urn,
    make_dashboard_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey, gen_containers
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
    SourceCapability,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    BIAssetSubTypes,
    BIContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.microstrategy.client import (
    MicroStrategyClient,
    MicroStrategyProjectUnavailableError,
)
from datahub.ingestion.source.microstrategy.config import MicroStrategyConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import ChangeAuditStamps
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BooleanTypeClass,
    GlobalTagAssociationClass,
    GlobalTagsClass,
    TagAssociationClass,
    ChartInfoClass,
    ContainerClass,
    DashboardInfoClass,
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    DateTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    SubTypesClass,
    TimeTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
from datahub.utilities.registries.domain_registry import DomainRegistry

logger = logging.getLogger(__name__)

# ── Dossier/Document subtype constants ───────────────────────────────────────
# Confirmed via live API testing (jcpenney-qa.cloud.strategy.com)
SUBTYPE_LEGACY_DOCUMENT = 14081  # → GET /api/documents/{id}/definition
SUBTYPE_MODERN_DOSSIER = 14336  # → GET /api/v2/dossiers/{id}/definition
SUBTYPE_SKIP = {14082, 14087, 14088}  # themes, agent templates — no content

# iServerCode error constants confirmed via live testing
ISERVER_PROJECT_UNAVAILABLE = -2147209151  # project not loaded  → fail fast
ISERVER_CUBE_NOT_PUBLISHED = -2147072488  # cube not in memory  → definition only
ISERVER_DYNAMIC_SOURCING_CUBE = -2147212800  # attr form cache cube → definition only
# ─────────────────────────────────────────────────────────────────────────────


def _extract_tables_from_sql(sql: str) -> List[str]:
    """
    Parse source warehouse table names from MicroStrategy-generated SQL.

    Handles three quoting styles confirmed in production:
      "SCHEMA"."TABLE"  — Snowflake / DB2 / Teradata (JCP uses this)
      `schema`.`table`  — MySQL / MSTR demo default
      schema.table      — bare (no quoting)

    Skips MicroStrategy volatile temp tables (TD*, T4*, TVIP*, etc.) which
    appear in multi-pass Teradata SQL as CREATE VOLATILE TABLE ... AS.
    """
    if not isinstance(sql, str) or not sql.strip():
        return []

    pattern = (
        r"(?:from|join)\s+"
        r"(?:"
        r'"(\w+)"\."(\w+)"'  # "SCHEMA"."TABLE"   groups 1,2
        r"|`(\w+)`\.`(\w+)`"  # `schema`.`table`   groups 3,4
        r"|(\w+)\.(\w+)"  # schema.table        groups 5,6
        r'|"(\w+)"'  # "TABLE"             group 7
        r"|`(\w+)`"  # `table`             group 8
        r"|(\w+)"  # table               group 9
        r")"
    )
    keywords = {
        "select",
        "where",
        "group",
        "order",
        "having",
        "on",
        "set",
        "into",
        "update",
        "delete",
        "with",
        "as",
        "inner",
        "outer",
        "left",
        "right",
        "cross",
        "full",
    }
    # Volatile temp table pattern — uppercase random names like TD7U1ZQ9CSP000
    volatile_pattern = re.compile(r"^T[A-Z0-9]{10,}$")

    matches = re.findall(pattern, sql, re.IGNORECASE)
    tables: Set[str] = set()
    for m in matches:
        if m[0] and m[1]:
            tables.add(f"{m[0]}.{m[1]}")
        elif m[2] and m[3]:
            tables.add(f"{m[2]}.{m[3]}")
        elif m[4] and m[5]:
            if m[4].lower() not in keywords and m[5].lower() not in keywords:
                tables.add(f"{m[4]}.{m[5]}")
        else:
            bare = m[6] or m[7] or m[8]
            if (
                bare
                and bare.lower() not in keywords
                and not volatile_pattern.match(bare.upper())
            ):
                tables.add(bare)
    return sorted(tables)


def _is_iserver_error(response_body: Dict[str, Any], code: int) -> bool:
    return response_body.get("iServerCode") == code


def _is_classcast_error(response_body: Dict[str, Any]) -> bool:
    msg = response_body.get("message", "")
    return "cannot be cast" in msg or "ClassCast" in msg


# Custom ContainerKey subclasses for MicroStrategy hierarchy
class ProjectKey(ContainerKey):
    """Container key for MicroStrategy projects."""

    project: str


class FolderKey(ContainerKey):
    """Container key for MicroStrategy folders."""

    project: str
    folder: str


@platform_name("MicroStrategy", id="microstrategy")
@config_class(MicroStrategyConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default", supported=True)
@capability(
    SourceCapability.DOMAINS, "Supported via the `domain` config field", supported=True
)
@capability(SourceCapability.CONTAINERS, "Enabled by default", supported=True)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default", supported=True)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Dashboard/report to cube lineage via `include_lineage`; "
    "warehouse table lineage via `include_warehouse_lineage` using sqlView SQL parsing",
    supported=True,
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Column-level lineage via SqlParsingAggregator when `include_column_lineage` is enabled",
    supported=True,
)
@capability(
    SourceCapability.OWNERSHIP,
    "Enabled by default via `include_ownership`",
    supported=True,
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled via stateful ingestion",
    supported=True,
)
class MicroStrategySource(StatefulIngestionSourceBase, TestableSource):
    """
    Ingests metadata from MicroStrategy (Strategy ONE / Cloud).

    Full lineage chain supported:
      Warehouse table → Intelligent Cube  (via cube sqlView SQL parsing)
      Warehouse table → Report            (via report sqlView with resolve_prompts)
      Warehouse table → Document/Dossier  (via datasets/sqlView on document instance)
      Cube → Report                       (via report.dataSource registry)
      Report/Cube → Dashboard             (via dashboard definition chapters/datasets)

    Subtype routing:
      14081 (legacy document) → /api/documents/* for creation, /api/dossiers/* for sqlView
      14336 (modern dossier)  → /api/dossiers/* throughout
      14082/14087/14088       → skipped (themes, agent templates — not content objects)
    """

    platform = "microstrategy"

    def __init__(self, config: MicroStrategyConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = StaleEntityRemovalSourceReport()

        self.client = MicroStrategyClient(self.config.connection)
        self.stale_entity_removal_handler = StaleEntityRemovalHandler.create(
            self, self.config, self.ctx
        )

        # Global registries for cross-project lineage resolution
        self.cube_registry: Dict[str, Dict[str, Any]] = {}
        self.dataset_registry: Dict[str, Dict[str, Any]] = {}
        self._datasets_by_project: Dict[str, List[Dict[str, Any]]] = {}
        self._cubes_by_project: Dict[str, List[Dict[str, Any]]] = {}

        domain_config = getattr(self.config, "domain", {})
        self.domain_registry = DomainRegistry(
            cached_domains=[
                domain_id
                for domain_id in (domain_config.values() if domain_config else [])
            ],
            graph=self.ctx.graph,
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "MicroStrategySource":
        config = MicroStrategyConfig.parse_obj(config_dict)
        return cls(config, ctx)

    # ── Main extraction ───────────────────────────────────────────────────────

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        with self.client:
            try:
                projects = self.client.get_projects()
            except MicroStrategyProjectUnavailableError as e:
                logger.error("Cannot list MicroStrategy projects: %s", e)
                raise

            pattern_matched = [
                p
                for p in projects
                if self.config.project_pattern.allowed(p.get("name", ""))
            ]
            filtered_projects = [
                p
                for p in pattern_matched
                if self.config.include_unloaded_projects or p.get("status") == 0
            ]
            skipped = len(pattern_matched) - len(filtered_projects)
            if skipped:
                logger.info(
                    "Skipping %s unloaded project(s). Set include_unloaded_projects: true to include.",
                    skipped,
                )
            logger.info(
                "Processing %s of %s projects", len(filtered_projects), len(projects)
            )

            for project in filtered_projects:
                self._build_registries(project)

            for project in filtered_projects:
                try:
                    yield from self._process_project(project)
                except MicroStrategyProjectUnavailableError as e:
                    logger.warning(
                        "Skipping project %s — IServer unavailable: %s",
                        project.get("name", project.get("id")),
                        e,
                    )

    # ── Registry building ─────────────────────────────────────────────────────

    def _needs_cube_search(self) -> bool:
        return self.config.include_cubes or (
            self.config.include_lineage and self.config.include_reports
        )

    def _needs_dataset_fetch(self) -> bool:
        return self.config.include_datasets or (
            self.config.include_lineage and self.config.include_reports
        )

    def _build_registries(self, project: Dict[str, Any]) -> None:
        project_id = project["id"]
        self._cubes_by_project.setdefault(project_id, [])

        if self._needs_cube_search():
            try:
                cubes = list(
                    self.client.search_objects(
                        project_id,
                        object_type=self.config.cube_search_object_type,
                    )
                )
                self._cubes_by_project[project_id] = cubes
                for cube in cubes:
                    self.cube_registry[cube["id"]] = {**cube, "project_id": project_id}
                logger.debug(
                    "Registered %s cubes from %s", len(cubes), project.get("name")
                )
            except MicroStrategyProjectUnavailableError as e:
                logger.warning(
                    "Skipping cube registry for %s: %s", project.get("name"), e
                )
                self._cubes_by_project[project_id] = []
            except Exception as e:
                logger.warning(
                    "Failed cube registry for %s: %s", project.get("name"), e
                )
                self._cubes_by_project[project_id] = []
        else:
            self._cubes_by_project[project_id] = []

        if self._needs_dataset_fetch():
            try:
                datasets = self.client.get_datasets(project_id)
                self._datasets_by_project[project_id] = datasets
                for ds in datasets:
                    self.dataset_registry[ds["id"]] = {**ds, "project_id": project_id}
            except Exception as e:
                logger.warning(
                    "Failed dataset registry for %s: %s", project.get("name"), e
                )
                self._datasets_by_project[project_id] = []
        else:
            self._datasets_by_project[project_id] = []

    # ── Project processing ────────────────────────────────────────────────────

    def _process_project(self, project: Dict[str, Any]) -> Iterable[MetadataWorkUnit]:
        project_id = project["id"]
        project_name = project.get("name", project_id)
        logger.info("Processing project: %s", project_name)

        yield from self._emit_project_container(project)

        if self.config.include_folders:
            yield from self._yield_folder_workunits(project, project_id, project_name)

        if self.config.include_dashboards:
            yield from self._yield_dashboard_workunits(
                project, project_id, project_name
            )

        if self.config.include_reports:
            yield from self._yield_report_workunits(project, project_id, project_name)

        if self.config.include_cubes:
            yield from self._yield_cube_workunits(project, project_id, project_name)

        if self.config.include_datasets:
            yield from self._yield_library_dataset_workunits(
                project, project_id, project_name
            )

    def _yield_folder_workunits(self, project, project_id, project_name):
        try:
            folders = self.client.get_folders(project_id)
            for folder in folders:
                if self.config.folder_pattern.allowed(folder.get("name", "")):
                    yield from self._emit_folder_container(folder, project)
        except Exception as e:
            logger.warning("Failed to get folders for %s: %s", project_name, e)

    def _yield_dashboard_workunits(self, project, project_id, project_name):
        try:
            all_type55 = list(self.client.search_objects(project_id, object_type=55))
            # Filter to actual content objects — skip themes and templates
            dashboards = [
                d for d in all_type55 if d.get("subtype", 0) not in SUBTYPE_SKIP
            ]
            logger.info(
                "Found %s dashboards/documents in %s (skipped %s non-content objects)",
                len(dashboards),
                project_name,
                len(all_type55) - len(dashboards),
            )
            for dashboard in dashboards:
                if self.config.dashboard_pattern.allowed(dashboard.get("name", "")):
                    yield from self._process_dashboard(dashboard, project)
        except Exception as e:
            logger.warning("Failed to get dashboards for %s: %s", project_name, e)

    def _yield_report_workunits(self, project, project_id, project_name):
        try:
            reports = list(self.client.search_objects(project_id, object_type=3))
            logger.info("Found %s reports in %s", len(reports), project_name)
            for report in reports:
                if self.config.report_pattern.allowed(report.get("name", "")):
                    yield from self._process_report(report, project)
        except Exception as e:
            logger.warning("Failed to get reports for %s: %s", project_name, e)

    def _yield_cube_workunits(self, project, project_id, project_name):
        try:
            cubes = self._cubes_by_project.get(project_id) or list(
                self.client.search_objects(
                    project_id, object_type=self.config.cube_search_object_type
                )
            )
            self._cubes_by_project[project_id] = cubes
            logger.info("Found %s cubes in %s", len(cubes), project_name)
            for cube in cubes:
                if self.config.cube_pattern.allowed(cube.get("name", "")):
                    yield from self._process_cube(cube, project)
        except Exception as e:
            logger.warning("Failed to get cubes for %s: %s", project_name, e)

    def _yield_library_dataset_workunits(self, project, project_id, project_name):
        try:
            for ds in self._datasets_by_project.get(project_id, []):
                yield from self._process_dataset(ds, project)
        except Exception as e:
            logger.warning("Failed to emit datasets for %s: %s", project_name, e)

    # ── Container emission ────────────────────────────────────────────────────

    def _emit_project_container(
        self, project: Dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        project_key = ProjectKey(
            project=project["id"],
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )
        yield from gen_containers(
            container_key=project_key,
            name=project.get("name", project["id"]),
            sub_types=[BIContainerSubTypes.TABLEAU_PROJECT],
            description=project.get("description"),
        )

    def _emit_folder_container(self, folder, project) -> Iterable[MetadataWorkUnit]:
        folder_key = FolderKey(
            folder=folder["id"],
            project=project["id"],
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )
        yield from gen_containers(
            container_key=folder_key,
            name=folder.get("name", folder["id"]),
            sub_types=[BIContainerSubTypes.LOOKER_FOLDER],
            description=folder.get("description"),
            parent_container_key=ProjectKey(
                project=project["id"],
                platform=self.platform,
                instance=self.config.platform_instance,
                env=self.config.env,
            ),
        )

    # ── Dashboard processing ──────────────────────────────────────────────────

    def _process_dashboard(
        self, dashboard: Dict[str, Any], project: Dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        """
        Process dashboard or document.

        Routing by subtype:
          14081 (legacy document) → /api/documents/{id}/definition  → datasets[]
          14336 (modern dossier)  → /api/v2/dossiers/{id}/definition → chapters[].pages[].visualizations[]
          Other                   → try dossier endpoint as default
        """
        project_id = project["id"]
        subtype = dashboard.get("subtype", 0)
        is_legacy = subtype == SUBTYPE_LEGACY_DOCUMENT
        is_modern = subtype == SUBTYPE_MODERN_DOSSIER

        if self.config.preflight_dashboard_exists:
            existence = self.client.get_object(dashboard["id"], 55, project_id)
            if not existence.get("id"):
                logger.debug(
                    "Skipping dashboard %s: not found (preflight)",
                    dashboard.get("name"),
                )
                return

        dashboard_urn = make_dashboard_urn(
            platform=self.platform,
            name=dashboard["id"],
            platform_instance=self.config.platform_instance,
        )

        chart_urns: List[str] = []

        if self.config.include_lineage:
            try:
                if is_legacy:
                    defn = self.client.get_document_definition(
                        dashboard["id"], project_id
                    )
                    chart_urns = self._extract_viz_ids_from_document(defn)
                else:
                    defn = self.client.get_dossier_definition(
                        dashboard["id"], project_id
                    )
                    chart_urns = self._extract_viz_ids_from_dossier(defn)
            except Exception as e:
                logger.warning(
                    "Failed to get definition for %s (%s): %s",
                    dashboard.get("name"),
                    subtype,
                    e,
                )

        dash_props = self._build_common_custom_props(dashboard, project)
        dash_props["dashboard_id"] = dashboard["id"]
        dash_props["subtype"]      = str(subtype)
        dash_props["object_type"]  = (
            "legacy_document" if is_legacy else ("modern_dossier" if is_modern else "dossier")
        )

        dashboard_info = DashboardInfoClass(
            title=dashboard.get("name", dashboard["id"]),
            description=dashboard.get("description") or "",
            charts=chart_urns,
            lastModified=self._build_audit_stamps(
                dashboard.get("dateCreated"),
                dashboard.get("dateModified"),
                dashboard.get("owner"),
            ),
            dashboardUrl=self._build_dashboard_url(dashboard["id"], project_id),
            customProperties=dash_props,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn, aspect=dashboard_info
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn, aspect=StatusClass(removed=False)
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn, aspect=self._make_platform_instance()
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=ContainerClass(container=self._project_key(project).as_urn()),
        ).as_workunit()

        cert = dashboard.get("certifiedInfo", {})
        if cert.get("certified"):
            yield MetadataChangeProposalWrapper(
                entityUrn=dashboard_urn, aspect=self._make_certified_tag()
            ).as_workunit()

        if self.config.include_ownership and dashboard.get("owner"):
            certifier = cert.get("certifier") if cert.get("certified") else None
            yield from self._emit_ownership(dashboard_urn, dashboard["owner"], certifier)

    def _extract_viz_ids_from_dossier(self, defn: Dict[str, Any]) -> List[str]:
        """
        Extract visualization IDs from modern dossier definition.
        Structure: chapters[] → pages[] → visualizations[]
        Note: pages layer was missing in the original implementation.
        """
        viz_ids: List[str] = []
        try:
            for chapter in defn.get("chapters", []):
                for page in chapter.get("pages", []):
                    for viz in page.get("visualizations", []):
                        viz_id = viz.get("key") or viz.get("id")
                        if viz_id:
                            viz_ids.append(str(viz_id))
        except Exception as e:
            logger.warning("Failed to extract dossier viz IDs: %s", e)
        return [
            make_chart_urn(
                platform=self.platform,
                name=cid,
                platform_instance=self.config.platform_instance,
            )
            for cid in viz_ids
        ]

    def _extract_viz_ids_from_document(self, defn: Dict[str, Any]) -> List[str]:
        """
        Extract chart URNs from legacy document definition.
        Structure: datasets[] → each dataset has an id that maps to a report/chart.
        """
        chart_ids: List[str] = []
        try:
            for dataset in defn.get("datasets", []):
                ds_id = dataset.get("id")
                if ds_id:
                    chart_ids.append(ds_id)
        except Exception as e:
            logger.warning("Failed to extract document chart IDs: %s", e)
        return [
            make_chart_urn(
                platform=self.platform,
                name=cid,
                platform_instance=self.config.platform_instance,
            )
            for cid in chart_ids
        ]

    # ── Report processing ─────────────────────────────────────────────────────

    def _process_report(
        self, report: Dict[str, Any], project: Dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        """
        Process a report (emitted as a DataHub chart entity).

        When include_report_definitions is true, fetches the full report definition
        (GET /api/v2/reports/{id}) to unlock:
          - dataSource.id  → report→cube lineage (the missing registry edge)
          - availableObjects → report schema (attributes and metrics used)
          - prompts / filter → enriched customProperties
          - richer description from the full definition

        The definition fetch is one extra API call per matched report, so this flag
        defaults to false and should only be enabled for scoped runs.
        """
        chart_urn = make_chart_urn(
            platform=self.platform,
            name=report["id"],
            platform_instance=self.config.platform_instance,
        )

        # Optionally fetch full report definition
        report_defn: Optional[Dict[str, Any]] = None
        if self.config.include_report_definitions:
            try:
                report_defn = self.client.get_report(report["id"], project["id"])
            except Exception as e:
                logger.debug("Could not fetch definition for report %s: %s", report.get("name"), e)

        # Merge: full definition fields override shallow search-result fields where available
        effective = {**report}
        if report_defn:
            effective["description"] = report_defn.get("description") or report.get("description") or ""

        inputs: List[str] = []
        sql_for_column_lineage: Optional[str] = None

        if self.config.include_lineage:
            # When we have the full definition, dataSource.id is reliably populated
            source_for_registry = report_defn if report_defn else report
            registry_inputs = self._get_report_registry_inputs(source_for_registry, project)
            inputs.extend(registry_inputs)

            # Fall back to warehouse lineage via sqlView for live reports (no cube backing)
            if self.config.include_warehouse_lineage and not registry_inputs:
                warehouse_inputs, sql = self._get_report_warehouse_upstreams(
                    report["id"], project["id"], report.get("name", "")
                )
                inputs.extend(warehouse_inputs)
                sql_for_column_lineage = sql

        # Build enriched customProperties
        subtype = report.get("subtype", 0)
        custom_props = self._build_common_custom_props(report, project)
        custom_props["report_id"]        = report["id"]
        custom_props["report_type"]      = str(subtype)
        custom_props["report_type_name"] = self._report_subtype_name(subtype)
        if report_defn:
            prompts = report_defn.get("prompts", [])
            if prompts:
                custom_props["prompt_count"] = str(len(prompts))
                custom_props["has_prompts"]  = "true"
            if report_defn.get("definition", {}).get("filter"):
                custom_props["has_filter"] = "true"

        chart_info = ChartInfoClass(
            title=effective.get("name", report["id"]),
            description=effective.get("description") or "",
            inputs=inputs,
            lastModified=self._build_audit_stamps(
                effective.get("dateCreated"),
                effective.get("dateModified"),
                effective.get("owner"),
            ),
            externalUrl=self._build_report_url(report["id"], project["id"]),
            customProperties=custom_props,
        )

        yield MetadataChangeProposalWrapper(entityUrn=chart_urn, aspect=chart_info).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=chart_urn, aspect=SubTypesClass(typeNames=[BIAssetSubTypes.REPORT])
        ).as_workunit()
        yield MetadataChangeProposalWrapper(entityUrn=chart_urn, aspect=StatusClass(removed=False)).as_workunit()
        yield MetadataChangeProposalWrapper(entityUrn=chart_urn, aspect=self._make_platform_instance()).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=ContainerClass(container=self._project_key(project).as_urn()),
        ).as_workunit()

        # Certification tag
        cert = report.get("certifiedInfo", {})
        if cert.get("certified"):
            yield MetadataChangeProposalWrapper(
                entityUrn=chart_urn, aspect=self._make_certified_tag()
            ).as_workunit()

        # Ownership — pass certifier separately so they get TECHNICAL_OWNER role
        if self.config.include_ownership and effective.get("owner"):
            certifier = cert.get("certifier") if cert.get("certified") else None
            yield from self._emit_ownership(chart_urn, effective["owner"], certifier)

        # Report schema from full definition
        if report_defn and self.config.include_cube_schema:
            yield from self._emit_report_schema(chart_urn, report["id"], report_defn)

        # Column-level lineage from SQL
        if sql_for_column_lineage and self.config.include_column_lineage:
            yield from self._emit_column_lineage_from_sql(
                sql=sql_for_column_lineage,
                downstream_urn=chart_urn,
                platform=self.config.warehouse_lineage_platform or self.platform,
            )

    def _get_report_registry_inputs(
        self, report: Dict[str, Any], project: Dict[str, Any]
    ) -> List[str]:
        """Resolve report → cube/dataset lineage via registry."""
        inputs: List[str] = []
        try:
            data_source_id = report.get("dataSource", {}).get("id") or report.get(
                "sourceId"
            )
            if not data_source_id:
                return []
            if data_source_id in self.cube_registry:
                info = self.cube_registry[data_source_id]
                inputs.append(
                    make_dataset_urn_with_platform_instance(
                        platform=self.platform,
                        name=f"{info['project_id']}.{data_source_id}",
                        env=self.config.env,
                        platform_instance=self.config.platform_instance,
                    )
                )
            elif data_source_id in self.dataset_registry:
                info = self.dataset_registry[data_source_id]
                inputs.append(
                    make_dataset_urn_with_platform_instance(
                        platform=self.platform,
                        name=f"{info['project_id']}.{data_source_id}",
                        env=self.config.env,
                        platform_instance=self.config.platform_instance,
                    )
                )
        except Exception as e:
            logger.debug(
                "Failed to extract registry inputs for report %s: %s",
                report.get("id"),
                e,
            )
        return inputs

    def _get_report_warehouse_upstreams(
        self,
        report_id: str,
        project_id: str,
        name: str,
    ) -> Tuple[List[str], Optional[str]]:
        """
        Get warehouse table URNs for a live report via sqlView.

        Uses executionStage=resolve_prompts so the report's SQL plan is
        resolved without executing the query against the warehouse.
        Returns (upstream_urns, raw_sql_for_column_lineage).
        """
        if not self.config.warehouse_lineage_platform:
            return [], None

        try:
            instance_id = self.client.create_report_instance(report_id, project_id)
        except Exception as e:
            logger.debug("Could not create report instance for %s: %s", name, e)
            return [], None

        if not instance_id:
            return [], None

        sql: Optional[str] = None
        try:
            sql = self.client.get_report_sql_view(report_id, instance_id, project_id)
        except Exception as e:
            logger.debug("Failed to get sqlView for report %s: %s", name, e)
        finally:
            try:
                self.client.delete_report_instance(report_id, instance_id, project_id)
            except Exception:
                pass

        if not sql:
            return [], None

        tables = _extract_tables_from_sql(sql)
        return self._tables_to_urns(list(set(tables))), sql

    # ── Cube processing ───────────────────────────────────────────────────────

    def _process_cube(
        self, cube: Dict[str, Any], project: Dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=f"{project['id']}.{cube['id']}",
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        cube_props = self._build_common_custom_props(cube, project)
        cube_props["cube_id"]   = cube["id"]
        cube_props["cube_type"] = "intelligent_cube"

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(
                name=cube.get("name", cube["id"]),
                description=cube.get("description"),
                customProperties=cube_props,
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=[DatasetSubTypes.VIEW]),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=StatusClass(removed=False)
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=self._make_platform_instance()
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=ContainerClass(container=self._project_key(project).as_urn()),
        ).as_workunit()

        fetch_cube_sql = self.config.include_cube_view_sql or (
            self.config.include_lineage
            and self.config.include_warehouse_lineage
            and bool(self.config.warehouse_lineage_platform)
        )
        prefetched_sql: Optional[str] = None
        if fetch_cube_sql:
            try:
                raw_sql = self.client.get_cube_sql_view(cube["id"], project["id"])
                prefetched_sql = raw_sql if isinstance(raw_sql, str) else ""
            except Exception as e:
                logger.debug(
                    "Could not fetch sqlView for cube %s: %s",
                    cube.get("name", cube["id"]),
                    e,
                )
                prefetched_sql = ""

        if prefetched_sql and self.config.include_cube_view_sql:
            yield from self._emit_cube_view_definition(dataset_urn, prefetched_sql)

        if self.config.include_cube_schema:
            yield from self._emit_cube_schema(dataset_urn, cube, project["id"])

        cert = cube.get("certifiedInfo", {})
        if cert.get("certified"):
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=self._make_certified_tag()
            ).as_workunit()

        if self.config.include_ownership and cube.get("owner"):
            certifier = cert.get("certifier") if cert.get("certified") else None
            yield from self._emit_ownership(dataset_urn, cube["owner"], certifier)

        if self.config.include_lineage and self.config.include_warehouse_lineage:
            yield from self._emit_cube_warehouse_lineage(
                dataset_urn, cube, project, sql=prefetched_sql
            )

    # MSTR report subtype → human-readable label
    _REPORT_SUBTYPE_NAMES: Dict[int, str] = {
        768:  "grid",
        769:  "graph",
        770:  "grid_graph",
        774:  "non_interactive",
        776:  "intelligent_cube",
        777:  "multi_layer_grid",
    }

    @classmethod
    def _report_subtype_name(cls, subtype: int) -> str:
        return cls._REPORT_SUBTYPE_NAMES.get(subtype, f"report_{subtype}")

    def _make_certified_tag(self) -> GlobalTagsClass:
        """Emit a 'Certified' tag for objects with certifiedInfo.certified=true."""
        tag_urn = "urn:li:tag:Certified"
        return GlobalTagsClass(
            tags=[GlobalTagAssociationClass(tag=tag_urn)]
        )

    def _build_common_custom_props(self, obj: Dict[str, Any], project: Dict[str, Any]) -> Dict[str, str]:
        """
        Build enriched customProperties shared across cubes, reports, and dashboards.
        Adds certification status, certifier, version, and object dates on top of
        the base project_id/project_name/object_id fields.
        """
        props: Dict[str, str] = {
            "project_id":   project["id"],
            "project_name": project.get("name", ""),
        }
        cert = obj.get("certifiedInfo", {})
        if cert:
            props["certified"] = str(cert.get("certified", False)).lower()
            certifier = cert.get("certifier") or {}
            certifier_name = certifier.get("fullName") or certifier.get("username") or ""
            if certifier_name:
                props["certifier"] = certifier_name
        if obj.get("version"):
            props["version"] = obj["version"]
        if obj.get("dateCreated"):
            props["date_created"] = obj["dateCreated"]
        if obj.get("dateModified"):
            props["date_modified"] = obj["dateModified"]
        return props

    # MSTR dataType string → DataHub SchemaFieldDataType
    # Covers all types observed in JCP live testing plus common MSTR variants.
    _MSTR_TYPE_MAP = {
        "varchar": lambda: StringTypeClass(),
        "char": lambda: StringTypeClass(),
        "decimal": lambda: NumberTypeClass(),
        "integer": lambda: NumberTypeClass(),
        "int64": lambda: NumberTypeClass(),
        "double": lambda: NumberTypeClass(),
        "float": lambda: NumberTypeClass(),
        "bigdecimal": lambda: NumberTypeClass(),
        "date": lambda: DateTypeClass(),
        "timestamp": lambda: TimeTypeClass(),
        "time": lambda: TimeTypeClass(),
        "boolean": lambda: BooleanTypeClass(),
    }

    @classmethod
    def _mstr_type_to_datahub(cls, mstr_data_type: str) -> SchemaFieldDataTypeClass:
        """
        Map a MicroStrategy form dataType string to a DataHub SchemaFieldDataTypeClass.

        Types confirmed in JCP live API response:
          varChar, decimal, Int64, date, timeStamp

        Falls back to StringTypeClass for any unrecognised type.
        """
        key = mstr_data_type.lower().replace(" ", "")
        factory = cls._MSTR_TYPE_MAP.get(key)
        if factory:
            return SchemaFieldDataTypeClass(type=factory())
        logger.debug(
            "Unknown MSTR dataType %r — defaulting to StringType", mstr_data_type
        )
        return SchemaFieldDataTypeClass(type=StringTypeClass())

    def _emit_cube_schema(
        self, dataset_urn: str, cube: Dict[str, Any], project_id: str
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit schema metadata for a cube.

        MicroStrategy schema model:
          - Attribute  — a logical business concept (e.g. "Fiscal Week", "Division")
          - Form       — a physical column expression of that attribute (e.g. "DESC", "ID")
          - Metric     — a calculated measure (e.g. "Net Sales Retail Amt")

        A single attribute can have multiple forms, each backed by a different
        warehouse column with a different data type. This method emits one
        SchemaField per form, not per attribute, so DataHub reflects the real
        column structure.

        fieldPath convention (confirmed against JCP cube definition):
          Single-form attribute  → attribute name only     e.g. "Division Number"
          Multi-form attribute   → "AttributeName.FormName" e.g. "Fiscal Week.DESC"
          Metric                 → metric name              e.g. "Net Sales Retail Amt"

        This gives DataHub accurate:
          - Column count  (71 fields from 55 attributes on Attribute Form Caching Cube WBP)
          - Column types  (varChar → String, decimal/Int64 → Number, date → Date, etc.)
          - Native types  ("attribute:DESC", "attribute:NUMBER", "metric") for BI context
        """
        try:
            raw = self.client.get_cube_schema(cube["id"], project_id)
            avail = raw.get("definition", {}).get("availableObjects", {})
            attrs = avail.get("attributes", [])
            metrics = avail.get("metrics", [])

            if not attrs and not metrics:
                logger.debug(
                    "Cube %s has 0 attributes and 0 metrics — may be a system/caching cube",
                    cube.get("name"),
                )
                return

            fields: List[SchemaFieldClass] = []

            for attr in attrs:
                attr_name = attr.get("name") or attr.get("id", "")
                forms = attr.get("forms", [])
                multi = len(forms) > 1

                if not forms:
                    # No forms defined — emit attribute name as a bare string field
                    fields.append(
                        SchemaFieldClass(
                            fieldPath=attr_name,
                            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                            nativeDataType="attribute",
                            description=attr.get("description"),
                        )
                    )
                    continue

                for form in forms:
                    form_name = form.get("name", "")
                    data_type = form.get("dataType", "varChar")
                    form_cat = form.get("baseFormCategory", "")

                    # Qualify fieldPath only when multiple forms exist to avoid collisions
                    field_path = f"{attr_name}.{form_name}" if multi else attr_name

                    fields.append(
                        SchemaFieldClass(
                            fieldPath=field_path,
                            type=self._mstr_type_to_datahub(data_type),
                            nativeDataType=f"attribute:{form_cat}"
                            if form_cat
                            else "attribute",
                            description=attr.get("description"),
                        )
                    )

            for metric in metrics:
                fields.append(
                    SchemaFieldClass(
                        fieldPath=metric.get("name") or metric.get("id", ""),
                        type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                        nativeDataType="metric",
                        description=metric.get("description"),
                    )
                )

            if not fields:
                return

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SchemaMetadataClass(
                    schemaName=cube.get("name", cube["id"]),
                    platform=make_data_platform_urn(self.platform),
                    version=0,
                    hash="",
                    platformSchema=OtherSchemaClass(rawSchema=""),
                    fields=fields,
                ),
            ).as_workunit()

        except Exception as e:
            logger.warning("Failed to get schema for cube %s: %s", cube.get("name"), e)

    def _emit_report_schema(
        self,
        chart_urn: str,
        report_id: str,
        report_defn: Dict[str, Any],
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit schema metadata for a report from its full definition.

        Mirrors _emit_cube_schema but for chart entities. The report definition
        exposes definition.availableObjects with the same attributes/metrics/forms
        model as cubes, representing which fields the report actually uses.

        Only called when include_report_definitions=true.
        """
        try:
            avail   = report_defn.get("definition", {}).get("availableObjects", {})
            attrs   = avail.get("attributes", [])
            metrics = avail.get("metrics", [])

            if not attrs and not metrics:
                return

            fields: List[SchemaFieldClass] = []

            for attr in attrs:
                attr_name = attr.get("name") or attr.get("id", "")
                forms     = attr.get("forms", [])
                multi     = len(forms) > 1
                if not forms:
                    fields.append(SchemaFieldClass(
                        fieldPath=attr_name,
                        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                        nativeDataType="attribute",
                        description=attr.get("description"),
                    ))
                    continue
                for form in forms:
                    form_name  = form.get("name", "")
                    data_type  = form.get("dataType", "varChar")
                    form_cat   = form.get("baseFormCategory", "")
                    field_path = f"{attr_name}.{form_name}" if multi else attr_name
                    fields.append(SchemaFieldClass(
                        fieldPath=field_path,
                        type=self._mstr_type_to_datahub(data_type),
                        nativeDataType=f"attribute:{form_cat}" if form_cat else "attribute",
                        description=attr.get("description"),
                    ))

            for metric in metrics:
                fields.append(SchemaFieldClass(
                    fieldPath=metric.get("name") or metric.get("id", ""),
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="metric",
                    description=metric.get("description"),
                ))

            if fields:
                yield MetadataChangeProposalWrapper(
                    entityUrn=chart_urn,
                    aspect=SchemaMetadataClass(
                        schemaName=report_defn.get("name", report_id),
                        platform=make_data_platform_urn(self.platform),
                        version=0,
                        hash="",
                        platformSchema=OtherSchemaClass(rawSchema=""),
                        fields=fields,
                    ),
                ).as_workunit()
        except Exception as e:
            logger.warning("Failed to emit schema for report %s: %s", report_id, e)

    def _emit_cube_view_definition(
        self, dataset_urn: str, sql: str
    ) -> Iterable[MetadataWorkUnit]:
        if not sql.strip():
            return
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=ViewPropertiesClass(
                materialized=False,
                viewLanguage="SQL",
                viewLogic=sql,
            ),
        ).as_workunit()

    def _emit_cube_warehouse_lineage(
        self,
        dataset_urn: str,
        cube: Dict[str, Any],
        project: Dict[str, Any],
        sql: Optional[str] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit warehouse table → cube lineage via sqlView SQL parsing.

        FIX: Uses get_cube_sql_view() (confirmed working) instead of
        get_model_cube() which requires 'Use Architect Editors' privilege
        and returns 0 physicalTables on most service accounts.
        """
        if not self.config.warehouse_lineage_platform:
            return

        project_id = project["id"]
        cube_name = cube.get("name", cube["id"])

        resolved_sql: str = ""
        if sql is not None:
            resolved_sql = sql if isinstance(sql, str) else ""
        else:
            try:
                raw = self.client.get_cube_sql_view(cube["id"], project_id)
                resolved_sql = raw if isinstance(raw, str) else ""
            except Exception as e:
                # iServerCode -2147072488 = not published, -2147212800 = dynamic sourcing
                logger.debug("Skipping warehouse lineage for cube %s: %s", cube_name, e)
                return

        if not resolved_sql:
            return

        tables = _extract_tables_from_sql(resolved_sql)
        if not tables:
            logger.debug("No source tables parsed from sqlView for cube %s", cube_name)
            return

        upstream_urns = self._tables_to_urns(list(set(tables)))
        if not upstream_urns:
            return

        yield from self._emit_upstream_lineage(dataset_urn, upstream_urns)

        # Column-level lineage from cube SQL
        if self.config.include_column_lineage:
            yield from self._emit_column_lineage_from_sql(
                sql=resolved_sql,
                downstream_urn=dataset_urn,
                platform=self.config.warehouse_lineage_platform,
            )

    # ── Dataset processing ────────────────────────────────────────────────────

    def _process_dataset(
        self, dataset: Dict[str, Any], project: Dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=f"{project['id']}.{dataset['id']}",
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(
                name=dataset.get("name", dataset["id"]),
                description=dataset.get("description"),
                customProperties={
                    "project_id": project["id"],
                    "project_name": project.get("name", ""),
                    "dataset_id": dataset["id"],
                },
            ),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=[DatasetSubTypes.VIEW]),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=StatusClass(removed=False)
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=self._make_platform_instance()
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=ContainerClass(container=self._project_key(project).as_urn()),
        ).as_workunit()

        if self.config.include_ownership and dataset.get("owner"):
            yield from self._emit_ownership(dataset_urn, dataset["owner"])

    # ── Lineage helpers ───────────────────────────────────────────────────────

    def _tables_to_urns(self, tables: List[str]) -> List[str]:
        """Convert parsed table names to DataHub dataset URNs."""
        plat = self.config.warehouse_lineage_platform
        if not plat:
            return []
        urns = []
        for table in sorted(tables):
            # Optionally qualify with configured db/schema
            fqn = self._qualify_table_name(table)
            urns.append(
                make_dataset_urn_with_platform_instance(
                    platform=plat,
                    name=fqn,
                    env=self.config.env,
                    platform_instance=None,
                )
            )
        return urns

    def _qualify_table_name(self, table: str) -> str:
        """
        Optionally prepend configured database/schema to bare table names.
        If table already has a schema prefix (contains '.'), leave it alone.
        """
        if "." in table:
            return table
        db = (getattr(self.config, "warehouse_lineage_database", None) or "").strip()
        schema = (getattr(self.config, "warehouse_lineage_schema", None) or "").strip()
        if db and schema:
            return f"{db}.{schema}.{table}"
        if schema:
            return f"{schema}.{table}"
        return table

    def _emit_upstream_lineage(
        self,
        downstream_urn: str,
        upstream_urns: List[str],
    ) -> Iterable[MetadataWorkUnit]:
        """Emit UpstreamLineageClass for dataset entities (not dashboard — see entity-registry)."""
        upstreams = [
            UpstreamClass(dataset=u, type=DatasetLineageTypeClass.TRANSFORMED)
            for u in upstream_urns
        ]
        yield MetadataChangeProposalWrapper(
            entityUrn=downstream_urn,
            aspect=UpstreamLineageClass(upstreams=upstreams),
        ).as_workunit()

    def _emit_column_lineage_from_sql(
        self,
        sql: str,
        downstream_urn: str,
        platform: str,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit FineGrainedLineage (column-level) by passing raw SQL to
        DataHub's SqlParsingAggregator.

        MicroStrategy generates SQL using column aliases like:
          select a11.MCAL_DATE MCAL_DATE, a11.ROW_WID WK__WID
          from "XRBIA_DM"."DIM_W_MCAL_WEEK_D_CV" a11

        SqlParsingAggregator can parse these SELECT ... FROM patterns to
        produce column-level upstream mappings.

        Multi-statement SQL (Teradata CREATE VOLATILE ... AS patterns) is
        split on double-newlines so each statement is parsed independently.
        """
        try:
            # Split multi-statement SQL — MSTR Teradata SQL uses double-newline as separator
            statements = [s.strip() for s in re.split(r"\n\s*\n", sql) if s.strip()]

            aggregator = SqlParsingAggregator(
                platform=platform,
                env=self.config.env,
                graph=self.ctx.graph,
            )

            for stmt in statements:
                # Skip DDL statements (CREATE VOLATILE TABLE, DROP TABLE)
                upper = stmt.upper().lstrip()
                if upper.startswith("CREATE") or upper.startswith("DROP"):
                    continue
                # Skip the analytical engine comment block
                if upper.startswith("[ANALYTICAL"):
                    continue
                try:
                    aggregator.add_observed_query(
                        query=stmt,
                        default_db=getattr(
                            self.config, "warehouse_lineage_database", None
                        ),
                        default_schema=getattr(
                            self.config, "warehouse_lineage_schema", None
                        ),
                    )
                except Exception:
                    pass  # individual statement failure should not abort the whole thing

            for wu in aggregator.gen_metadata():
                yield wu

        except Exception as e:
            logger.debug(
                "Column lineage generation failed for %s: %s", downstream_urn, e
            )

    # ── Ownership helpers ─────────────────────────────────────────────────────

    def _emit_ownership(
        self, entity_urn: str, owner_info: Any, certifier_info: Optional[Dict[str, Any]] = None
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit ownership aspect.

        owner_info may come from two sources with different shapes:
          - Search result owner: {"id": "...", "name": "Paul Hanlon", "expired": false}
            → name is a display name, not a username. We use it directly.
          - Full definition owner: {"username": "phanlon", "email": "p@jcp.com", ...}
            → prefer username, fall back to email, fall back to name.

        certifier_info (optional): {"fullName": "svankaya", "username": "..."}
            → emit as TECHNICAL_OWNER alongside the data owner.
        """
        owners: List[OwnerClass] = []

        def _resolve_urn(info: Any) -> Optional[str]:
            if isinstance(info, str):
                return make_user_urn(info)
            if isinstance(info, dict):
                identifier = (
                    info.get("username")
                    or info.get("email")
                    or info.get("name")       # display name fallback (search result shape)
                    or info.get("fullName")
                )
                return make_user_urn(identifier) if identifier else None
            return None

        owner_urn = _resolve_urn(owner_info)
        if owner_urn:
            owners.append(OwnerClass(owner=owner_urn, type=OwnershipTypeClass.DATAOWNER))

        if certifier_info:
            cert_urn = _resolve_urn(certifier_info)
            if cert_urn and cert_urn != owner_urn:
                owners.append(OwnerClass(owner=cert_urn, type=OwnershipTypeClass.TECHNICAL_OWNER))

        if owners:
            yield MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=OwnershipClass(owners=owners),
            ).as_workunit()

    # ── Utility helpers ───────────────────────────────────────────────────────

    def _project_key(self, project: Dict[str, Any]) -> ProjectKey:
        return ProjectKey(
            project=project["id"],
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

    def _make_platform_instance(self) -> DataPlatformInstanceClass:
        return DataPlatformInstanceClass(
            platform=make_data_platform_urn(self.platform),
            instance=(
                make_dataplatform_instance_urn(
                    self.platform, self.config.platform_instance
                )
                if self.config.platform_instance
                else None
            ),
        )

    def _build_audit_stamps(
        self,
        created_time: Optional[str],
        modified_time: Optional[str],
        owner_info: Optional[Any],
    ) -> ChangeAuditStamps:
        actor_urn = "urn:li:corpuser:unknown"
        if owner_info:
            if isinstance(owner_info, str):
                actor_urn = make_user_urn(owner_info)
            elif isinstance(owner_info, dict):
                username = owner_info.get("username") or owner_info.get("email")
                if username:
                    actor_urn = make_user_urn(username)

        stamps = ChangeAuditStamps()
        try:
            if created_time:
                ts = int(date_parser.parse(created_time).timestamp() * 1000)
                stamps.created = AuditStampClass(time=ts, actor=actor_urn)
            if modified_time:
                ts = int(date_parser.parse(modified_time).timestamp() * 1000)
                stamps.lastModified = AuditStampClass(time=ts, actor=actor_urn)
        except Exception as e:
            logger.debug("Failed to parse timestamps: %s", e)
        return stamps

    def _build_dashboard_url(self, dashboard_id: str, project_id: str) -> Optional[str]:
        if not self.config.connection.base_url:
            return None
        base = self.config.connection.base_url.rstrip("/")
        return f"{base}/app/{project_id}/{dashboard_id}"

    def _build_report_url(self, report_id: str, project_id: str) -> Optional[str]:
        if not self.config.connection.base_url:
            return None
        base = self.config.connection.base_url.rstrip("/")
        return f"{base}/app/{project_id}/{report_id}"

    def get_report(self) -> SourceReport:
        return self.report

    # ── Test connection ───────────────────────────────────────────────────────

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        test_report.capability_report = {}
        try:
            config = MicroStrategyConfig.parse_obj(config_dict)
            client = MicroStrategyClient(config.connection)
            if client.test_connection():
                test_report.basic_connectivity = CapabilityReport(capable=True)
            else:
                test_report.basic_connectivity = CapabilityReport(
                    capable=False, failure_reason="Failed to connect to API"
                )
                return test_report
            try:
                with client:
                    projects = client.get_projects()
                    cap = CapabilityReport(capable=bool(projects))
                    if not projects:
                        cap = CapabilityReport(
                            capable=False,
                            failure_reason="No projects found — check permissions",
                        )
                    test_report.capability_report[SourceCapability.CONTAINERS] = cap
                    test_report.capability_report[SourceCapability.DESCRIPTIONS] = cap
            except Exception as e:
                test_report.capability_report[SourceCapability.CONTAINERS] = (
                    CapabilityReport(
                        capable=False, failure_reason=f"Failed to get projects: {e}"
                    )
                )
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report