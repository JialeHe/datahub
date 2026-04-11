import logging
import re
from collections import defaultdict
from datetime import datetime
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Set,
    TypedDict,
    Union,
)

from dateutil import parser as date_parser

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import (
    Aspect,
    make_chart_urn,
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
    make_user_urn,
)
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
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DatasetLineageTypeClass,
    DateTypeClass,
    GlobalTagsClass,
    InputFieldClass,
    InputFieldsClass,
    NumberTypeClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    TagAssociationClass,
    TimeTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.sdk.chart import Chart as SdkChart
from datahub.sdk.dashboard import Dashboard as SdkDashboard
from datahub.sdk.dataset import Dataset as SdkDataset
from datahub.sdk.entity import Entity
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
from datahub.utilities.registries.domain_registry import DomainRegistry

logger = logging.getLogger(__name__)

# ── MSTR database.type → DataHub platform ID ─────────────────────────────────
# Source: GET /api/datasources  →  each entry's  database.type  field.
# Values that have no DataHub platform equivalent map to a synthetic ID using
# the MSTR type string directly (e.g. "mstr:cloud_element").  DataHub will
# create unresolved upstream nodes, which is the correct behaviour for
# unsupported/SaaS source systems — lineage is preserved even if the platform
# has no registered DataHub connector.
_MSTR_DBTYPE_TO_DATAHUB: Dict[str, str] = {
    # Teradata
    "teradata": "teradata",
    "teradata_13": "teradata",
    "teradata_14": "teradata",
    "teradata_15": "teradata",
    "teradata_16": "teradata",
    # Snowflake
    "snow_flake": "snowflake",
    "snowflake": "snowflake",
    # SQL Server / Azure
    "sql_server": "mssql",
    "sql_server_2016": "mssql",
    "sql_server_2017": "mssql",
    "sql_server_2019": "mssql",
    "azure_sql_database": "mssql",
    "azure_synapse_analytics": "mssql",
    # Oracle
    "oracle": "oracle",
    "oracle_11gr2": "oracle",
    "oracle_12c": "oracle",
    "oracle_122": "oracle",
    "oracle_18c": "oracle",
    "oracle_19c": "oracle",
    "oracle_21c": "oracle",
    # PostgreSQL
    "postgre_sql": "postgres",
    "postgre_sql_90": "postgres",
    "postgre_sql_10": "postgres",
    # Redshift
    "amazon_redshift": "redshift",
    "redshift": "redshift",
    # BigQuery
    "big_query": "bigquery",
    "google_big_query": "bigquery",
    "google_big_query_ff_sql": "bigquery",
    # MySQL / MariaDB
    "my_sql": "mysql",
    "mysql": "mysql",
    "maria_db": "mysql",
    # Databricks / Spark
    "databricks": "databricks",
    "spark_sql": "spark",
    "spark_config": "spark",
    # Hive
    "hive": "hive",
    "cloudera_hive": "hive",
    "cloudera_impala": "hive",
    # SAP HANA
    "sap_hana": "saphana",
    # IBM DB2
    "db2": "db2",
    "db2_11": "db2",
    "ibm_db2": "db2",
    # IBM Informix (no dedicated DataHub platform — use synthetic ID)
    "informix": "informix",
    # ClickHouse / StarRocks
    "click_house": "clickhouse",
}


def _mstr_dbtype_to_platform(db_type: str, dbms_name: str = "") -> Optional[str]:
    """
    Map a MSTR database.type string to a DataHub platform ID.

    Priority:
      1. Exact match in _MSTR_DBTYPE_TO_DATAHUB (covers all confirmed MSTR types)
      2. Fuzzy substring match on dbms.name  (handles version-suffixed strings
         like 'Teradata Database 16.20' that aren't in the exact map)
      3. Synthetic platform ID using the raw db_type prefixed with 'mstr:'
         (preserves lineage for SaaS/cloud connectors with no DataHub equivalent)

    The synthetic fallback means lineage is never silently dropped — DataHub
    creates unresolved upstream nodes, which is preferable to missing edges.
    """
    key = (db_type or "").lower().replace("-", "_").replace(" ", "_")
    if key in _MSTR_DBTYPE_TO_DATAHUB:
        return _MSTR_DBTYPE_TO_DATAHUB[key]

    # Fuzzy match on dbms.name
    name = (dbms_name or "").lower()
    if "teradata" in name:
        return "teradata"
    if "snowflake" in name:
        return "snowflake"
    if "sql server" in name or "sqlserver" in name:
        return "mssql"
    if "synapse" in name:
        return "mssql"
    if "oracle" in name:
        return "oracle"
    if "postgres" in name or "postgresql" in name:
        return "postgres"
    if "redshift" in name:
        return "redshift"
    if "bigquery" in name or "big query" in name:
        return "bigquery"
    if "mysql" in name:
        return "mysql"
    if "mariadb" in name or "maria db" in name:
        return "mysql"
    if "databricks" in name:
        return "databricks"
    if "spark" in name:
        return "spark"
    if "hive" in name:
        return "hive"
    if "hana" in name:
        return "saphana"
    if "db2" in name:
        return "db2"
    if "informix" in name:
        return "informix"
    if "clickhouse" in name or "click house" in name:
        return "clickhouse"

    # Synthetic fallback — preserve lineage for unknown/SaaS types
    if key and key not in ("unknown", "url_auth", ""):
        return f"mstr:{key}"
    return None


# ─────────────────────────────────────────────────────────────────────────────


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


class _CubeRegistryEntry(TypedDict):
    """Entry in the per-cube registry populated by _build_registries."""

    project_id: str
    name: str


class _DatasetRegistryEntry(TypedDict):
    """Entry in the per-library-dataset registry populated by _build_registries."""

    project_id: str
    name: str


def _filter_real_platforms(
    mapping: Dict[str, List[str]],
) -> Dict[str, List[str]]:
    """Exclude MicroStrategy-internal (mstr:) pseudo-platform entries."""
    return {p: names for p, names in mapping.items() if not p.startswith("mstr:")}


class DatasourceRef(NamedTuple):
    """Datasource ID and display name returned by model-table resolution."""

    id: str
    name: str


class DocumentVizExtraction(NamedTuple):
    """Viz chart URNs and embedded dataset records extracted from a document definition."""

    chart_urns: List[str]
    datasets: List[Dict[str, Any]]


class ReportLineageResult(NamedTuple):
    """Warehouse upstream URNs and raw SQL from a report sqlView call."""

    upstream_urns: List[str]
    sql: Optional[str]


class CubeLineageResult(NamedTuple):
    """Upstream lineage aspect and SQL string from cube warehouse lineage building."""

    upstream_lineage: Optional[UpstreamLineageClass]
    sql: Optional[str]


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
    SourceCapability.DOMAINS,
    "Not supported — no domain config field or domain aspect emission",
    supported=False,
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
        self.cube_registry: Dict[str, _CubeRegistryEntry] = {}
        self.dataset_registry: Dict[str, _DatasetRegistryEntry] = {}
        self._datasets_by_project: Dict[str, List[Dict[str, Any]]] = {}
        self._cubes_by_project: Dict[str, List[Dict[str, Any]]] = {}

        # Detected warehouse platform — populated at ingestion startup by
        # _detect_warehouse_platform().  Used instead of the removed
        # warehouse_lineage_platform config field.
        self._warehouse_platform: Optional[str] = None

        # Dashboard-driven scoping: IDs of cubes/reports referenced by matched dashboards.
        # Populated during dashboard processing; consumed by cube/report yield methods.
        # Only active when dashboard_driven_mode() is True.
        self._dashboard_referenced_ids: Set[str] = set()

        # IDs of embedded datasets from legacy documents (subtype 14081).
        # These already have named chart stubs emitted by _emit_embedded_chart_stub
        # with per-dataset warehouse URNs in chartInfo.inputs.
        # They must NOT be routed through _process_report, which would duplicate
        # warehouse lineage edges.
        self._document_embedded_ids: Set[str] = set()

        self.domain_registry = DomainRegistry(
            cached_domains=[],
            graph=self.ctx.graph,
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "MicroStrategySource":
        config = MicroStrategyConfig.model_validate(config_dict)
        return cls(config, ctx)

    # ── Main extraction ───────────────────────────────────────────────────────

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, Entity]]:
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
                    project_name = project.get("name", project.get("id"))
                    logger.warning(
                        "Skipping project %s — IServer unavailable: %s",
                        project_name,
                        e,
                    )
                    self.report.report_warning(
                        "project-unavailable",
                        context=str(project_name),
                        exc=e,
                    )

    @staticmethod
    def _platforms_from_datasource_list(
        dss: List[Dict[str, Any]],
    ) -> Dict[str, List[str]]:
        """Map normal-type datasources to {platform: [names]}."""
        mapping: Dict[str, List[str]] = defaultdict(list)
        for ds in dss:
            if ds.get("datasourceType") == "normal":
                db_type = ds.get("database", {}).get("type", "")
                dbms_name = ds.get("dbms", {}).get("name", "")
                platform = _mstr_dbtype_to_platform(db_type, dbms_name)
                if platform:
                    mapping[platform].append(ds.get("name", "?"))
        return dict(mapping)

    def _detect_warehouse_platform(self, project_id: Optional[str] = None) -> None:
        """
        Detect the warehouse platform using a three-tier fallback strategy,
        stopping as soon as a definitive answer is found.

        Sets self._warehouse_platform (str) or leaves it None.
        Called once per project at the top of _process_project.
        """
        if self._warehouse_platform and not self._warehouse_platform.startswith(
            "mstr:"
        ):
            # Already resolved from a previous project — reuse.
            return

        if project_id and self._try_tier1_project_datasources(project_id):
            return
        if self._try_tier2_env_datasources():
            return
        if project_id:
            self._try_tier3_tables_api(project_id)

    def _try_tier1_project_datasources(self, project_id: str) -> bool:
        """
        Tier 1: GET /api/projects/{id}/datasources — project-scoped.

        Most targeted: returns ONLY the datasources attached to this project.
        Project ID goes in the URL path — NOT as X-MSTR-ProjectID header.
        Returns True if a single unambiguous platform was detected.
        """
        try:
            proj_dss = self.client.get_project_datasources(project_id)
        except Exception as e:
            logger.warning(
                "Tier 1 datasource lookup failed for project %s, falling back: %s",
                project_id,
                e,
            )
            self.report.report_warning(
                "Tier 1 datasource lookup failed; falling back to env-level detection.",
                context=project_id,
                title="warehouse-platform-detection-failed",
                exc=e,
            )
            return False

        real = _filter_real_platforms(self._platforms_from_datasource_list(proj_dss))
        if len(real) == 1:
            platform = next(iter(real))
            logger.info(
                "Warehouse platform detected from project datasources "
                "(GET /api/projects/%s/datasources): %s",
                project_id,
                platform,
            )
            self._warehouse_platform = platform
            return True
        if real:
            logger.info(
                "Multiple platforms in project datasources (%s) for project %s "
                "— falling back to Tables API.",
                list(real),
                project_id,
            )
        return False

    def _try_tier2_env_datasources(self) -> bool:
        """
        Tier 2: GET /api/datasources — env-level, no project header.

        Falls back to the environment-wide list. Works on single-platform
        environments where exactly one real warehouse platform is configured.
        Returns True if a single unambiguous platform was detected.
        """
        datasources = self.client.get_datasources()
        real = _filter_real_platforms(
            self._platforms_from_datasource_list(datasources)
        )
        if len(real) == 1:
            platform = next(iter(real))
            logger.info(
                "Warehouse platform detected from /api/datasources: %s", platform
            )
            self._warehouse_platform = platform
            return True
        if real:
            logger.info(
                "Multiple warehouse platforms in /api/datasources (%s) — "
                "using Tables API for project-scoped lookup.",
                len(real),
            )
        return False

    def _try_tier3_tables_api(self, project_id: str) -> bool:
        """
        Tier 3: Tables API + per-datasource lookup — project-scoped.

        Fetches a sample warehouse table, reads its primaryDataSource.objectId,
        then resolves the datasource. First tries GET /api/v2/tables/{id}; if
        that returns nothing, falls back to the model tables path.
        Returns True if a platform was detected.
        """
        try:
            tables = self.client.search_warehouse_tables(project_id, limit=5)
            for table in tables:
                table_id = table.get("id")
                table_name = table.get("name", table_id)
                if not table_id:
                    continue

                # 3a: try GET /api/v2/tables/{id} first
                defn = self.client.get_table_definition(table_id, project_id)
                ds_ref = defn.get("primaryDataSource") or {}
                ds_id = ds_ref.get("objectId")
                ds_name = ds_ref.get("name", "?")

                # 3b: if v2/tables returned nothing, fall back to model tables path
                if not ds_id and table_name:
                    ref = self._resolve_datasource_via_model_tables(
                        table_name, project_id
                    )
                    ds_id, ds_name = ref.id, ref.name

                if not ds_id:
                    continue

                ds_obj = self.client.get_datasource_by_id(ds_id, project_id)
                db_type = ds_obj.get("database", {}).get("type", "")
                dbms_name = ds_obj.get("dbms", {}).get("name", "")
                platform = _mstr_dbtype_to_platform(db_type, dbms_name)

                if platform and not platform.startswith("mstr:"):
                    logger.info(
                        "Warehouse platform detected from Tables API "
                        "(table '%s' → datasource '%s' → %s).",
                        table_name,
                        ds_name,
                        platform,
                    )
                    self._warehouse_platform = platform
                    return True

            logger.warning(
                "Tables API did not resolve a warehouse platform for project %s.",
                project_id,
            )
        except Exception as e:
            logger.warning(
                "Tables API platform lookup failed for project %s: %s",
                project_id,
                e,
            )
            self.report.report_warning(
                "Tables API platform lookup failed; warehouse platform not resolved.",
                context=project_id,
                title="warehouse-platform-detection-failed",
                exc=e,
            )
        return False

    def _resolve_datasource_via_model_tables(
        self, table_name: str, project_id: str
    ) -> DatasourceRef:
        """
        Find a table's primaryDataSource by searching the model tables list.

        Called from _try_tier3_tables_api when GET /api/v2/tables/{id} returns
        nothing. Paginates GET /api/model/tables to find the model Table ID
        matching table_name (case-insensitive), then fetches its definition.

        Returns a DatasourceRef with id="" and name="?" if not found.
        """
        PAGE = 200
        offset = 0
        total: Optional[int] = None
        while True:
            body = self.client.list_model_tables(project_id, limit=PAGE, offset=offset)
            if not body:
                break
            entries = body.get("tables", [])
            if total is None:
                total = body.get("total", len(entries))
            for entry in entries:
                info = entry.get("information", entry)
                if info.get("name", "").upper() == table_name.upper():
                    model_id = info.get("objectId")
                    if not model_id:
                        continue
                    defn = self.client.get_model_table_definition(model_id, project_id)
                    ds_ref = defn.get("primaryDataSource") or {}
                    ds_id = ds_ref.get("objectId", "")
                    ds_name = ds_ref.get("name", "?")
                    if ds_id:
                        logger.debug(
                            "Model tables path: '%s' → model ID %s → datasource '%s' (%s)",
                            table_name,
                            model_id,
                            ds_name,
                            ds_id,
                        )
                    return DatasourceRef(ds_id, ds_name)
            offset += len(entries)
            if not entries or (total is not None and offset >= total):
                break
        logger.debug(
            "Model tables path: '%s' not found across %s model tables in project %s",
            table_name,
            total,
            project_id,
        )
        return DatasourceRef("", "?")

    # ── Registry building ─────────────────────────────────────────────────────

    def _needs_cube_search(self) -> bool:
        return self.config.include_cubes or (
            self.config.include_lineage and self.config.include_reports
        )

    def _needs_dataset_fetch(self) -> bool:
        return self.config.include_datasets or (
            self.config.include_lineage and self.config.include_reports
        )

    @staticmethod
    def _is_default_pattern(pattern: AllowDenyPattern) -> bool:
        """True when pattern is the default allow-all (allow=[".*"], no deny rules)."""
        return list(pattern.allow) == [".*"] and not list(pattern.deny)

    def _dashboard_driven_mode(self) -> bool:
        """
        Dashboard-driven scoping is active when the user has scoped to specific
        dashboards (dashboard_pattern is non-default) but has NOT explicitly scoped
        cubes or reports (both patterns are still allow-all).

        In this mode, cubes and reports are only ingested if they are directly
        referenced by a matched dashboard — no explicit cube_pattern/report_pattern
        needed. This gives complete, coherent lineage graphs without requiring users
        to know which cube/report UUIDs their dashboards depend on.

        Mode B (explicit): cube_pattern or report_pattern is set → use those patterns,
        emit stubs for anything referenced but not matched.
        Mode C (allow-all): all patterns default → ingest everything.
        """
        return (
            self.config.include_dashboards
            and not self._is_default_pattern(self.config.dashboard_pattern)
            and self._is_default_pattern(self.config.cube_pattern)
            and self._is_default_pattern(self.config.report_pattern)
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
                    self.cube_registry[cube["id"]] = _CubeRegistryEntry(
                        project_id=project_id, name=cube.get("name", cube["id"])
                    )
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
                    self.dataset_registry[ds["id"]] = _DatasetRegistryEntry(
                        project_id=project_id, name=ds.get("name", ds["id"])
                    )
            except Exception as e:
                logger.warning(
                    "Failed dataset registry for %s: %s", project.get("name"), e
                )
                self._datasets_by_project[project_id] = []
        else:
            self._datasets_by_project[project_id] = []

    # ── Project processing ────────────────────────────────────────────────────

    def _process_project(
        self, project: Dict[str, Any]
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        project_id = project["id"]
        project_name = project.get("name", project_id)
        logger.info("Processing project: %s", project_name)

        # Resolve warehouse platform for this project before emitting any lineage.
        # Called per-project so the Tables API lookup has a project ID to work with.
        # _warehouse_platform is cached after first resolution — subsequent projects
        # reuse it unless they override (multi-project runs with mixed platforms are
        # uncommon and can be addressed with explicit warehouse_lineage_platform config).
        if self.config.include_warehouse_lineage:
            self._detect_warehouse_platform(project_id=project_id)

        if self._dashboard_driven_mode():
            logger.info(
                "Dashboard-driven mode active for %s: cubes and reports will be scoped "
                "to objects referenced by matched dashboards.",
                project_name,
            )
        # Reset per-project referenced IDs so multi-project runs stay isolated
        self._dashboard_referenced_ids = set()

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

    def _yield_folder_workunits(
        self,
        project: Dict[str, Any],
        project_id: str,
        project_name: str,
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        try:
            folders = self.client.get_folders(project_id)
            for folder in folders:
                if self.config.folder_pattern.allowed(folder.get("name", "")):
                    yield from self._emit_folder_container(folder, project)
        except Exception as e:
            logger.warning("Failed to get folders for %s: %s", project_name, e)
            self.report.report_warning(
                "folder-fetch-failed", context=project_name, exc=e
            )

    def _yield_dashboard_workunits(
        self,
        project: Dict[str, Any],
        project_id: str,
        project_name: str,
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
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
            self.report.report_warning(
                "dashboard-fetch-failed", context=project_name, exc=e
            )

    def _yield_report_workunits(
        self,
        project: Dict[str, Any],
        project_id: str,
        project_name: str,
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        try:
            reports = list(self.client.search_objects(project_id, object_type=3))
            logger.info("Found %s reports in %s", len(reports), project_name)
            driven = self._dashboard_driven_mode()
            matched = 0
            for report in reports:
                if driven:
                    # Dashboard-driven: only process reports referenced by matched dashboards.
                    # Skip IDs that belong to embedded legacy document datasets — those already
                    # have named stubs and consolidated lineage; full report processing would
                    # incorrectly populate chartInfo.inputs with warehouse table URNs.
                    if report["id"] not in self._dashboard_referenced_ids:
                        continue
                    if report["id"] in self._document_embedded_ids:
                        continue
                elif not self.config.report_pattern.allowed(report.get("name", "")):
                    continue
                matched += 1
                yield from self._process_report(report, project)
            if driven:
                logger.info(
                    "Dashboard-driven: ingested %s of %s reports (referenced by matched dashboards)",
                    matched,
                    len(reports),
                )
        except Exception as e:
            logger.warning("Failed to get reports for %s: %s", project_name, e)
            self.report.report_warning(
                "report-fetch-failed", context=project_name, exc=e
            )

    def _yield_cube_workunits(
        self,
        project: Dict[str, Any],
        project_id: str,
        project_name: str,
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        try:
            cubes = self._cubes_by_project.get(project_id) or list(
                self.client.search_objects(
                    project_id, object_type=self.config.cube_search_object_type
                )
            )
            self._cubes_by_project[project_id] = cubes
            logger.info("Found %s cubes in %s", len(cubes), project_name)
            driven = self._dashboard_driven_mode()
            matched = 0
            for cube in cubes:
                if driven:
                    # Dashboard-driven: only process cubes referenced by matched dashboards
                    if cube["id"] not in self._dashboard_referenced_ids:
                        continue
                elif not self.config.cube_pattern.allowed(cube.get("name", "")):
                    continue
                matched += 1
                yield from self._process_cube(cube, project)
            if driven:
                logger.info(
                    "Dashboard-driven: ingested %s of %s cubes (referenced by matched dashboards)",
                    matched,
                    len(cubes),
                )
        except Exception as e:
            logger.warning("Failed to get cubes for %s: %s", project_name, e)
            self.report.report_warning("cube-fetch-failed", context=project_name, exc=e)

    def _yield_library_dataset_workunits(
        self,
        project: Dict[str, Any],
        project_id: str,
        project_name: str,
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        try:
            for ds in self._datasets_by_project.get(project_id, []):
                yield from self._process_dataset(ds, project)
        except Exception as e:
            logger.warning("Failed to emit datasets for %s: %s", project_name, e)
            self.report.report_warning(
                "dataset-fetch-failed", context=project_name, exc=e
            )

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
            sub_types=[BIContainerSubTypes.MICROSTRATEGY_PROJECT],
            description=project.get("description"),
        )

    def _emit_folder_container(
        self, folder: Dict[str, Any], project: Dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
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
            sub_types=[BIContainerSubTypes.MICROSTRATEGY_FOLDER],
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
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
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

        chart_urns: List[str] = []
        # embedded_datasets: list of {"id": ..., "name": ...} from legacy document definition
        # used to emit named chart stubs so dashboard→chart links resolve in DataHub
        embedded_datasets: List[Dict[str, Any]] = []

        if self.config.include_lineage:
            try:
                if is_legacy:
                    defn = self.client.get_document_definition(
                        dashboard["id"], project_id
                    )
                    doc_extraction = self._extract_viz_ids_from_document(defn)
                    chart_urns = doc_extraction.chart_urns
                    embedded_datasets = doc_extraction.datasets
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
                self.report.report_warning(
                    "dashboard-definition-failed",
                    context=f"{dashboard.get('name')} (subtype={subtype})",
                    exc=e,
                )

        dash_props = self._build_common_custom_props(dashboard, project)
        dash_props["dashboard_id"] = dashboard["id"]
        dash_props["subtype"] = str(subtype)
        dash_props["object_type"] = (
            "legacy_document"
            if is_legacy
            else ("modern_dossier" if is_modern else "dossier")
        )

        cert = dashboard.get("certifiedInfo", {})
        extra_aspects: List[Aspect] = [StatusClass(removed=False)]
        if cert.get("certified"):
            extra_aspects.append(self._make_certified_tag())

        owners = None
        if self.config.include_ownership and dashboard.get("owner"):
            certifier = cert.get("certifier") if cert.get("certified") else None
            owners = self._build_owners(dashboard["owner"], certifier) or None

        yield SdkDashboard(
            name=dashboard["id"],
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            display_name=dashboard.get("name", dashboard["id"]),
            description=dashboard.get("description") or "",
            charts=chart_urns,
            dashboard_url=self._build_dashboard_url(dashboard["id"], project_id),
            custom_properties=dash_props,
            parent_container=self._project_key(project).as_urn(),
            owners=owners,
            created_at=self._parse_datetime(dashboard.get("dateCreated")),
            last_modified=self._parse_datetime(dashboard.get("dateModified")),
            extra_aspects=extra_aspects,
        )

        # For legacy documents and modern dossiers, fetch warehouse lineage per embedded
        # dataset and store as a name→tables map so stubs can set inputs[] directly.
        # This replaces the synthetic __datasource entity — each chart stub now carries its
        # own specific warehouse tables, matching the standard DataHub BI connector pattern.
        per_dataset_tables: Dict[str, List[str]] = {}
        if (
            (is_legacy or is_modern)
            and self.config.include_lineage
            and self.config.include_warehouse_lineage
        ):
            per_dataset_tables = self._fetch_per_dataset_tables(
                dashboard["id"],
                project_id,
                dashboard.get("name", dashboard["id"]),
                is_legacy=is_legacy,
            )

        # Register referenced dataset IDs for dashboard-driven scoping.
        if self._dashboard_driven_mode():
            for ds in embedded_datasets:
                self._dashboard_referenced_ids.add(ds["id"])
                if is_legacy:
                    # Track embedded document dataset IDs separately so _yield_report_workunits
                    # can skip them — they get stubs + direct warehouse inputs via per_dataset_tables,
                    # not full report processing.
                    self._document_embedded_ids.add(ds["id"])
            for ds in embedded_datasets:
                if (
                    ds["id"] not in self.cube_registry
                    and ds["id"] not in self.dataset_registry
                ):
                    yield from self._emit_embedded_chart_stub(
                        ds, project, per_dataset_tables.get(ds["name"], [])
                    )
        else:
            for ds in embedded_datasets:
                if is_legacy:
                    self._document_embedded_ids.add(ds["id"])
                yield from self._emit_embedded_chart_stub(
                    ds, project, per_dataset_tables.get(ds["name"], [])
                )

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
            self.report.report_warning(
                "Failed to extract visualization IDs from dossier definition.",
                context=str(defn.get("id", "")),
                title="dashboard-viz-extraction-failed",
                exc=e,
            )
        return [
            make_chart_urn(
                platform=self.platform,
                name=cid,
                platform_instance=self.config.platform_instance,
            )
            for cid in viz_ids
        ]

    def _extract_viz_ids_from_document(
        self, defn: Dict[str, Any]
    ) -> DocumentVizExtraction:
        """
        Extract chart URNs and named dataset records from legacy document definition.
        Structure: datasets[] → each entry has id (chart URN key) and name.

        Returns a DocumentVizExtraction so callers can emit named stubs for each
        embedded dataset, resolving the ghost-node problem in the DataHub lineage graph.
        """
        datasets: List[Dict[str, Any]] = []
        try:
            for dataset in defn.get("datasets", []):
                ds_id = dataset.get("id")
                if ds_id:
                    datasets.append(
                        {
                            "id": ds_id,
                            "name": dataset.get("name", ds_id),
                        }
                    )
        except Exception as e:
            logger.warning("Failed to extract document chart IDs: %s", e)
            self.report.report_warning(
                "Failed to extract dataset chart IDs from document definition.",
                context=str(defn.get("id", "")),
                title="dashboard-viz-extraction-failed",
                exc=e,
            )

        chart_urns = [
            make_chart_urn(
                platform=self.platform,
                name=ds["id"],
                platform_instance=self.config.platform_instance,
            )
            for ds in datasets
        ]
        return DocumentVizExtraction(chart_urns, datasets)

    def _emit_embedded_chart_stub(
        self,
        dataset: Dict[str, Any],
        project: Dict[str, Any],
        warehouse_urns: Optional[List[str]] = None,
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        """
        Emit a chart stub for an embedded document dataset.

        Legacy documents (subtype 14081) reference their datasets by ID in
        dashboardInfo.charts. Without a corresponding chart entity, DataHub
        shows these as unresolved ghost nodes — UUID strings with no name.

        This stub emits:
          - chartInfo with the dataset name, inputs pointing directly at the
            specific warehouse tables this dataset queries (per-dataset SQL parsed
            from datasets/sqlView — no synthetic __datasource intermediary)
          - status, dataPlatformInstance, container (required for browse)
          - inputFields (attributes + metrics) if include_report_definitions is enabled
        """
        extra_aspects: List[Aspect] = [StatusClass(removed=False)]

        # InputFields — fetch the dataset definition to get attributes and metrics.
        if self.config.include_report_definitions:
            parent_urn = (
                warehouse_urns[0]
                if warehouse_urns
                else self._project_key(project).as_urn()
            )
            try:
                defn = self.client.get_report(dataset["id"], project["id"])
                avail = (
                    defn.get("definition", {}).get("availableObjects", {})
                    if defn
                    else {}
                )
                if avail:
                    input_fields = self._build_input_fields(
                        avail, parent_urn, project["id"]
                    )
                    if input_fields:
                        extra_aspects.append(InputFieldsClass(fields=input_fields))
            except Exception as e:
                logger.debug(
                    "Could not fetch definition for embedded dataset %s: %s",
                    dataset.get("name", dataset["id"]),
                    e,
                )

        yield SdkChart(
            name=dataset["id"],
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            display_name=dataset.get("name", dataset["id"]),
            description="",
            input_datasets=warehouse_urns or [],
            custom_properties={
                "project_id": project["id"],
                "project_name": project.get("name", ""),
                "dataset_id": dataset["id"],
                "source": "embedded_document_dataset",
            },
            subtype="Dataset",
            parent_container=self._project_key(project).as_urn(),
            extra_aspects=extra_aspects,
        )

    # ── Report processing ─────────────────────────────────────────────────────

    def _process_report(
        self, report: Dict[str, Any], project: Dict[str, Any]
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
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
                logger.debug(
                    "Could not fetch definition for report %s: %s",
                    report.get("name"),
                    e,
                )

        # Merge: full definition fields override shallow search-result fields where available
        effective = {**report}
        if report_defn:
            effective["description"] = (
                report_defn.get("description") or report.get("description") or ""
            )

        inputs: List[str] = []
        sql_for_column_lineage: Optional[str] = None

        if self.config.include_lineage:
            # When we have the full definition, dataSource.id is reliably populated
            source_for_registry = report_defn if report_defn else report
            registry_inputs = self._get_report_registry_inputs(
                source_for_registry, project
            )
            inputs.extend(registry_inputs)

            # Dashboard-driven: register the referenced cube/dataset so _yield_cube_workunits
            # (which runs after report processing) includes it even if not directly linked
            # from a dashboard.
            if self._dashboard_driven_mode() and registry_inputs:
                source_id = source_for_registry.get("dataSource", {}).get(
                    "id"
                ) or source_for_registry.get("sourceId")
                if source_id:
                    self._dashboard_referenced_ids.add(source_id)

            # Fall back to warehouse lineage via sqlView for live reports (no cube backing)
            if self.config.include_warehouse_lineage and not registry_inputs:
                rlu = self._get_report_warehouse_upstreams(
                    report["id"], project["id"], report.get("name", "")
                )
                inputs.extend(rlu.upstream_urns)
                sql_for_column_lineage = rlu.sql

        # Build enriched customProperties
        subtype = report.get("subtype", 0)
        custom_props = self._build_common_custom_props(report, project)
        custom_props["report_id"] = report["id"]
        custom_props["report_type"] = str(subtype)
        custom_props["report_type_name"] = self._report_subtype_name(subtype)
        if report_defn:
            prompts = report_defn.get("prompts", [])
            if prompts:
                custom_props["prompt_count"] = str(len(prompts))
                custom_props["has_prompts"] = "true"
            if report_defn.get("definition", {}).get("filter"):
                custom_props["has_filter"] = "true"
            # Surface field counts from availableObjects — schemaMetadata is not
            # valid for chart entities, so we capture this in customProperties instead
            avail = report_defn.get("definition", {}).get("availableObjects", {})
            if avail:
                attrs = avail.get("attributes", [])
                metrics = avail.get("metrics", [])
                if attrs:
                    custom_props["attribute_count"] = str(len(attrs))
                if metrics:
                    custom_props["metric_count"] = str(len(metrics))

        cert = report.get("certifiedInfo", {})
        extra_aspects: List[Aspect] = [StatusClass(removed=False)]
        if cert.get("certified"):
            extra_aspects.append(self._make_certified_tag())

        # InputFields — attributes and metrics visible on the chart in DataHub
        if report_defn:
            avail = report_defn.get("definition", {}).get("availableObjects", {})
            if avail:
                parent_urn = (
                    inputs[0] if inputs else self._project_key(project).as_urn()
                )
                input_fields = self._build_input_fields(
                    avail, parent_urn, project["id"]
                )
                if input_fields:
                    extra_aspects.append(InputFieldsClass(fields=input_fields))

        owners = None
        if self.config.include_ownership and effective.get("owner"):
            certifier = cert.get("certifier") if cert.get("certified") else None
            owners = self._build_owners(effective["owner"], certifier) or None

        yield SdkChart(
            name=report["id"],
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            display_name=effective.get("name", report["id"]),
            description=effective.get("description") or "",
            input_datasets=inputs,
            chart_url=self._build_report_url(report["id"], project["id"]),
            custom_properties=custom_props,
            subtype=self._report_subtype_name(subtype),
            parent_container=self._project_key(project).as_urn(),
            owners=owners,
            created_at=self._parse_datetime(effective.get("dateCreated")),
            last_modified=self._parse_datetime(effective.get("dateModified")),
            extra_aspects=extra_aspects,
        )

        # Column-level lineage from SQL — still emitted as MWUs
        if sql_for_column_lineage and self.config.include_column_lineage:
            plat = self._warehouse_platform
            yield from self._emit_column_lineage_from_sql(
                sql=sql_for_column_lineage,
                downstream_urn=chart_urn,
                platform=plat or self.platform,
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
            info = self.cube_registry.get(data_source_id) or self.dataset_registry.get(
                data_source_id
            )
            if info:
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
    ) -> ReportLineageResult:
        """
        Get warehouse table URNs for a live report via sqlView.

        Uses executionStage=resolve_prompts so the report's SQL plan is
        resolved without executing the query against the warehouse.
        """
        try:
            instance_id = self.client.create_report_instance(report_id, project_id)
        except Exception as e:
            logger.debug("Could not create report instance for %s: %s", name, e)
            return ReportLineageResult([], None)

        if not instance_id:
            return ReportLineageResult([], None)

        sql: Optional[str] = None
        try:
            sql = self.client.get_report_sql_view(report_id, instance_id, project_id)
        except Exception as e:
            logger.debug("Failed to get sqlView for report %s: %s", name, e)
        finally:
            try:
                self.client.delete_report_instance(report_id, instance_id, project_id)
            except Exception as e:
                logger.debug(
                    "Failed to delete report instance %s for %s: %s",
                    instance_id,
                    report_id,
                    e,
                )

        if not sql:
            return ReportLineageResult([], None)

        tables = _extract_tables_from_sql(sql)
        return ReportLineageResult(self._tables_to_urns(list(set(tables))), sql)

    # ── Cube processing ───────────────────────────────────────────────────────

    def _process_cube(
        self, cube: Dict[str, Any], project: Dict[str, Any]
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=f"{project['id']}.{cube['id']}",
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        cube_props = self._build_common_custom_props(cube, project)
        cube_props["cube_id"] = cube["id"]
        cube_props["cube_type"] = "intelligent_cube"

        fetch_cube_sql = self.config.include_cube_view_sql or (
            self.config.include_lineage and self.config.include_warehouse_lineage
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

        cert = cube.get("certifiedInfo", {})
        extra_aspects: List[Aspect] = [StatusClass(removed=False)]
        if cert.get("certified"):
            extra_aspects.append(self._make_certified_tag())
        if prefetched_sql and self.config.include_cube_view_sql:
            extra_aspects.append(
                ViewPropertiesClass(
                    materialized=False,
                    viewLanguage="SQL",
                    viewLogic=prefetched_sql,
                )
            )

        schema_metadata: Optional[SchemaMetadataClass] = None
        if self.config.include_cube_schema:
            schema_metadata = self._build_cube_schema_metadata(cube, project["id"])

        upstreams: Optional[UpstreamLineageClass] = None
        sql_for_col_lineage: Optional[str] = None
        if self.config.include_lineage and self.config.include_warehouse_lineage:
            clu = self._build_cube_warehouse_upstream(
                cube, project, sql=prefetched_sql
            )
            upstreams = clu.upstream_lineage
            sql_for_col_lineage = clu.sql

        owners = None
        if self.config.include_ownership and cube.get("owner"):
            certifier = cert.get("certifier") if cert.get("certified") else None
            owners = self._build_owners(cube["owner"], certifier) or None

        yield SdkDataset(
            platform=self.platform,
            name=f"{project['id']}.{cube['id']}",
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=cube.get("name", cube["id"]),
            description=cube.get("description"),
            custom_properties=cube_props,
            subtype="Intelligent Cube",
            parent_container=self._project_key(project).as_urn(),
            schema=schema_metadata,
            upstreams=upstreams,
            owners=owners,
            extra_aspects=extra_aspects,
        )

        # Column-level lineage still emitted as MWUs
        if sql_for_col_lineage and self.config.include_column_lineage:
            plat = self._warehouse_platform or self.platform
            yield from self._emit_column_lineage_from_sql(
                sql=sql_for_col_lineage,
                downstream_urn=dataset_urn,
                platform=plat,
            )

    # MSTR report subtype → DataHub display label (matches MSTR UI terminology)
    _REPORT_SUBTYPE_NAMES: Dict[int, str] = {
        768: "Grid Report",
        769: "Graph Report",
        770: "Grid and Graph Report",
        774: "Non-Interactive Report",
        776: "Intelligent Cube",  # shouldn't reach here — cubes use _process_cube
        777: "Multi-Layer Grid Report",
    }

    @classmethod
    def _report_subtype_name(cls, subtype: int) -> str:
        return cls._REPORT_SUBTYPE_NAMES.get(subtype, BIAssetSubTypes.REPORT)

    def _make_certified_tag(self) -> GlobalTagsClass:
        """Emit a 'Certified' tag for objects with certifiedInfo.certified=true."""
        tag_urn = "urn:li:tag:Certified"
        return GlobalTagsClass(tags=[TagAssociationClass(tag=tag_urn)])

    def _build_common_custom_props(
        self, obj: Dict[str, Any], project: Dict[str, Any]
    ) -> Dict[str, str]:
        """
        Build enriched customProperties shared across cubes, reports, and dashboards.
        Adds certification status, certifier, version, and object dates on top of
        the base project_id/project_name/object_id fields.
        """
        props: Dict[str, str] = {
            "project_id": project["id"],
            "project_name": project.get("name", ""),
        }
        cert = obj.get("certifiedInfo", {})
        if cert:
            props["certified"] = str(cert.get("certified", False)).lower()
            certifier = cert.get("certifier") or {}
            certifier_name = (
                certifier.get("fullName") or certifier.get("username") or ""
            )
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

    def _build_input_fields(
        self,
        avail: Dict[str, Any],
        parent_dataset_urn: str,
        project_id: Optional[str] = None,
    ) -> List[InputFieldClass]:
        """
        Build InputField objects from a MicroStrategy availableObjects dict.

        Each attribute form and each metric becomes an InputField with:
          - schemaFieldUrn  pointing to parent_dataset_urn (the upstream entity)
          - schemaField     carrying the type, nativeDataType, role tags, and description

        When include_field_formulas is true and project_id is provided, fetches the
        expression for each attribute (physical column mapping) and metric (aggregation
        formula) via the model API and surfaces it as the field description:
          Attribute: "ORG_DS_NUM | DISTRICT_NUMBER"  (column expressions across tables)
          Metric:    "Sum(NET_SLS_RTL_AMT)"
          Calculated metric: "({Net Sales Retail Amt} - {Net Sales Retail Amt LY}) / Abs(...)"
        """
        fetch_formulas = self.config.include_field_formulas and bool(project_id)
        fields: List[InputFieldClass] = []
        attrs = avail.get("attributes", [])
        metrics = avail.get("metrics", [])

        for attr in attrs:
            attr_name = attr.get("name") or attr.get("id", "")
            attr_id = attr.get("id", "")
            forms = attr.get("forms", [])
            multi = len(forms) > 1

            description: Optional[str] = None
            if fetch_formulas and attr_id:
                assert (
                    project_id is not None
                )  # guaranteed by fetch_formulas check above
                expr = self.client.get_attribute_expression(attr_id, project_id)
                if expr:
                    description = expr

            if not forms:
                field_path = attr_name
                fields.append(
                    InputFieldClass(
                        schemaFieldUrn=make_schema_field_urn(
                            parent_dataset_urn, field_path
                        ),
                        schemaField=SchemaFieldClass(
                            fieldPath=field_path,
                            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                            nativeDataType="attribute",
                            description=description,
                            globalTags=GlobalTagsClass(
                                tags=[TagAssociationClass(tag="urn:li:tag:ATTRIBUTE")]
                            ),
                        ),
                    )
                )
                continue

            for form in forms:
                form_name = form.get("name", "")
                data_type = form.get("dataType", "varChar")
                form_cat = form.get("baseFormCategory", "")
                field_path = f"{attr_name}.{form_name}" if multi else attr_name
                native = f"attribute:{form_cat}" if form_cat else "attribute"
                tags = [TagAssociationClass(tag="urn:li:tag:ATTRIBUTE")]
                if form_cat:
                    tags.append(
                        TagAssociationClass(tag=f"urn:li:tag:{form_cat.upper()}")
                    )
                fields.append(
                    InputFieldClass(
                        schemaFieldUrn=make_schema_field_urn(
                            parent_dataset_urn, field_path
                        ),
                        schemaField=SchemaFieldClass(
                            fieldPath=field_path,
                            type=self._mstr_type_to_datahub(data_type),
                            nativeDataType=native,
                            description=description,
                            globalTags=GlobalTagsClass(tags=tags),
                        ),
                    )
                )

        for metric in metrics:
            field_path = metric.get("name") or metric.get("id", "")
            metric_id = metric.get("id", "")

            description = None
            if fetch_formulas and metric_id:
                assert (
                    project_id is not None
                )  # guaranteed by fetch_formulas check above
                expr = self.client.get_metric_expression(metric_id, project_id)
                if expr:
                    description = expr

            fields.append(
                InputFieldClass(
                    schemaFieldUrn=make_schema_field_urn(
                        parent_dataset_urn, field_path
                    ),
                    schemaField=SchemaFieldClass(
                        fieldPath=field_path,
                        type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                        nativeDataType="metric",
                        description=description,
                        globalTags=GlobalTagsClass(
                            tags=[TagAssociationClass(tag="urn:li:tag:METRIC")]
                        ),
                    ),
                )
            )

        return fields

    # ── Per-dataset warehouse table helper ───────────────────────────────────

    def _fetch_per_dataset_tables(
        self,
        document_id: str,
        project_id: str,
        doc_name: str,
        *,
        is_legacy: bool,
    ) -> Dict[str, List[str]]:
        """
        Create a document/dossier instance, fetch datasets/sqlView, and return a
        mapping of dataset name → list of warehouse dataset URNs.

        Each entry in the sqlView response has a ``name`` field that matches the
        embedded dataset name from the document definition.  That name is used as
        the join key so each chart stub gets only the warehouse tables it queries.

        The instance is always cleaned up in the finally block.
        """
        instance_id: Optional[str] = None
        try:
            if is_legacy:
                instance_id = self.client.create_document_instance(
                    document_id, project_id
                )
            else:
                instance_id = self.client.create_dossier_instance(
                    document_id, project_id
                )
        except Exception as e:
            logger.debug("Could not create instance for document '%s': %s", doc_name, e)
            return {}

        if not instance_id:
            return {}

        result: Dict[str, List[str]] = {}
        try:
            datasets_sql = self.client.get_dossier_datasets_sql(
                document_id, instance_id, project_id
            )
            for ds in datasets_sql:
                if not isinstance(ds, dict):
                    continue
                name = ds.get("name", "")
                sql = ds.get("sqlStatement", "")
                if not name or not sql:
                    continue
                tables = _extract_tables_from_sql(sql)
                urns = self._tables_to_urns(sorted(tables))
                if urns:
                    result[name] = urns
            logger.info(
                "Document '%s': per-dataset tables resolved for %s/%s datasets",
                doc_name,
                len(result),
                len(datasets_sql),
            )
        except Exception as e:
            logger.warning("Failed to fetch per-dataset SQL for '%s': %s", doc_name, e)
            self.report.report_warning(
                "Failed to fetch per-dataset SQL for document.",
                context=doc_name,
                title="document-sql-fetch-failed",
                exc=e,
            )
        finally:
            if instance_id:
                try:
                    self.client.delete_dossier_instance(
                        document_id, instance_id, project_id
                    )
                except Exception as e:
                    logger.debug(
                        "Failed to delete dossier instance %s for %s: %s",
                        instance_id,
                        document_id,
                        e,
                    )

        return result

    # ── Dataset processing ────────────────────────────────────────────────────

    def _process_dataset(
        self, dataset: Dict[str, Any], project: Dict[str, Any]
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        owners = None
        if self.config.include_ownership and dataset.get("owner"):
            owners = self._build_owners(dataset["owner"]) or None

        yield SdkDataset(
            platform=self.platform,
            name=f"{project['id']}.{dataset['id']}",
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=dataset.get("name", dataset["id"]),
            description=dataset.get("description"),
            custom_properties={
                "project_id": project["id"],
                "project_name": project.get("name", ""),
                "dataset_id": dataset["id"],
            },
            subtype="Dataset",
            parent_container=self._project_key(project).as_urn(),
            owners=owners,
            extra_aspects=[StatusClass(removed=False)],
        )

    # ── Lineage helpers ───────────────────────────────────────────────────────

    def _tables_to_urns(self, tables: List[str]) -> List[str]:
        """Convert parsed table names to DataHub dataset URNs."""
        plat = self._warehouse_platform
        if not plat:
            if tables:
                self.report.report_warning(
                    "No warehouse platform detected; skipping upstream table(s). "
                    "Platform is auto-detected from /api/datasources and /api/v2/tables — "
                    "check that the service account has access to those endpoints.",
                    context=f"skipped {len(tables)} table(s)",
                    title="warehouse-lineage-skipped",
                )
            return []
        urns = []
        for table in sorted(tables):
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
        db = (self.config.warehouse_lineage_database or "").strip()
        schema = (self.config.warehouse_lineage_schema or "").strip()
        if db and schema:
            return f"{db}.{schema}.{table}"
        if schema:
            return f"{schema}.{table}"
        return table

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
                        default_db=self.config.warehouse_lineage_database,
                        default_schema=self.config.warehouse_lineage_schema,
                    )
                except Exception as _col_err:
                    logger.debug(
                        "Column lineage parse failed for a SQL statement in %s: %s",
                        downstream_urn,
                        _col_err,
                    )

            for wu in aggregator.gen_metadata():
                yield wu

        except Exception as e:
            logger.debug(
                "Column lineage generation failed for %s: %s", downstream_urn, e
            )

    # ── SDK V2 helpers ────────────────────────────────────────────────────────

    def _parse_datetime(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse an ISO date string into a datetime object for SDK V2 entities."""
        if not date_str:
            return None
        try:
            return date_parser.parse(date_str)
        except Exception as e:
            logger.debug("Failed to parse date string '%s': %s", date_str, e)
            return None

    def _build_owners(
        self,
        owner_info: Union[str, Dict[str, Any]],
        certifier_info: Optional[Dict[str, Any]] = None,
    ) -> List[OwnerClass]:
        """Build OwnerClass list for SDK V2 entity constructors."""

        def _resolve_urn(info: Union[str, Dict[str, Any]]) -> Optional[str]:
            if isinstance(info, str):
                return make_user_urn(info)
            if isinstance(info, dict):
                identifier = (
                    info.get("username")
                    or info.get("email")
                    or info.get("name")
                    or info.get("fullName")
                )
                return make_user_urn(identifier) if identifier else None
            return None

        owners: List[OwnerClass] = []
        owner_urn = _resolve_urn(owner_info)
        if owner_urn:
            owners.append(
                OwnerClass(owner=owner_urn, type=OwnershipTypeClass.DATAOWNER)
            )
        if certifier_info:
            cert_urn = _resolve_urn(certifier_info)
            if cert_urn and cert_urn != owner_urn:
                owners.append(
                    OwnerClass(owner=cert_urn, type=OwnershipTypeClass.TECHNICAL_OWNER)
                )
        return owners

    def _build_cube_schema_metadata(
        self, cube: Dict[str, Any], project_id: str
    ) -> Optional[SchemaMetadataClass]:
        """Build SchemaMetadataClass for a cube. Returns None on error or empty schema."""
        try:
            raw = self.client.get_cube(cube["id"], project_id)
            if not raw or not isinstance(raw, dict):
                return None
            if _is_iserver_error(raw, ISERVER_CUBE_NOT_PUBLISHED):
                self.report.report_warning(
                    "cube-not-published", context=str(cube.get("name"))
                )
                logger.debug(
                    "Cube %s not published (iServerCode=%s); skipping schema",
                    cube.get("name"),
                    ISERVER_CUBE_NOT_PUBLISHED,
                )
                return None
            if _is_iserver_error(raw, ISERVER_DYNAMIC_SOURCING_CUBE):
                self.report.report_warning(
                    "cube-dynamic-sourcing", context=str(cube.get("name"))
                )
                logger.debug(
                    "Cube %s uses dynamic sourcing (iServerCode=%s); skipping schema",
                    cube.get("name"),
                    ISERVER_DYNAMIC_SOURCING_CUBE,
                )
                return None

            avail = raw.get("definition", {}).get("availableObjects", {})
            attrs = avail.get("attributes", [])
            metrics = avail.get("metrics", [])

            if not attrs and not metrics:
                logger.debug(
                    "Cube %s has 0 attributes and 0 metrics — may be a system/caching cube",
                    cube.get("name"),
                )
                return None

            fields: List[SchemaFieldClass] = []

            for attr in attrs:
                attr_name = attr.get("name") or attr.get("id", "")
                forms = attr.get("forms", [])
                multi = len(forms) > 1

                if not forms:
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
                return None

            return SchemaMetadataClass(
                schemaName=cube.get("name", cube["id"]),
                platform=make_data_platform_urn(self.platform),
                version=0,
                hash="",
                platformSchema=OtherSchemaClass(rawSchema=""),
                fields=fields,
            )
        except Exception as e:
            logger.warning("Failed to get schema for cube %s: %s", cube.get("name"), e)
            self.report.report_warning(
                "cube-schema-failed", context=str(cube.get("name")), exc=e
            )
            return None

    def _build_cube_warehouse_upstream(
        self,
        cube: Dict[str, Any],
        project: Dict[str, Any],
        sql: Optional[str] = None,
    ) -> CubeLineageResult:
        """Build UpstreamLineageClass for warehouse→cube lineage."""
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
                logger.debug("Skipping warehouse lineage for cube %s: %s", cube_name, e)
                return CubeLineageResult(None, None)

        if not resolved_sql:
            return CubeLineageResult(None, None)

        tables = _extract_tables_from_sql(resolved_sql)
        if not tables:
            logger.debug("No source tables parsed from sqlView for cube %s", cube_name)
            return CubeLineageResult(None, None)

        upstream_urns = self._tables_to_urns(list(set(tables)))
        if not upstream_urns:
            return CubeLineageResult(None, None)

        upstreams = [
            UpstreamClass(dataset=u, type=DatasetLineageTypeClass.TRANSFORMED)
            for u in upstream_urns
        ]
        return CubeLineageResult(UpstreamLineageClass(upstreams=upstreams), resolved_sql)

    # ── Utility helpers ───────────────────────────────────────────────────────

    def _project_key(self, project: Dict[str, Any]) -> ProjectKey:
        return ProjectKey(
            project=project["id"],
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

    def _build_object_url(self, object_id: str, project_id: str) -> Optional[str]:
        if not self.config.connection.base_url:
            return None
        base = self.config.connection.base_url.rstrip("/")
        return f"{base}/app/{project_id}/{object_id}"

    def _build_dashboard_url(self, dashboard_id: str, project_id: str) -> Optional[str]:
        return self._build_object_url(dashboard_id, project_id)

    def _build_report_url(self, report_id: str, project_id: str) -> Optional[str]:
        return self._build_object_url(report_id, project_id)

    def get_report(self) -> SourceReport:
        return self.report

    # ── Test connection ───────────────────────────────────────────────────────

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        test_report.capability_report = {}
        try:
            config = MicroStrategyConfig.model_validate(config_dict)
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
