# Snowflake Enhancements: Stages, Tasks, and Snowpipe — Planning Document

**Created**: 2026-03-27
**Linear Issue**: [ING-1533](https://linear.app/acryl-data/issue/ING-1533/snowflake-add-support-for-snowpipe-tasks-and-stages)
**Status**: IN_PROGRESS
**Assignee**: alok.ranjan@datahub.com

---

## Overview

Add support for three currently unsupported Snowflake objects to the existing DataHub Snowflake connector:

1. **Snowflake Stages** — Named storage locations (internal/external cloud storage)
2. **Snowflake Tasks** — Scheduled/orchestrated SQL workflows with DAG dependencies
3. **Snowflake Snowpipe** — Continuous data loading pipelines from cloud storage

**Customer Impact**: 3 accounts (2 prospects, 1 customer). Key use case is the full lineage chain: `S3 → Stage → Snowpipe → Table`.

This is an **enhancement to the existing connector** (`metadata-ingestion/src/datahub/ingestion/source/snowflake/`), not a new connector.

---

## Research Summary

### Source Classification

- **Type**: Data Warehouse (existing connector)
- **Source Category**: `data_warehouses`
- **Interface**: Snowflake Python Connector (already in use)
- **Standards File**: `standards/source_types/data_warehouses.md`
- **Documentation**: [Snowflake SQL Reference](https://docs.snowflake.com/en/sql-reference)

### Existing Connector Architecture

The Snowflake connector is a mature, modular codebase:

| Module | Purpose |
|--------|---------|
| `snowflake_v2.py` | Main source class (`SnowflakeV2Source`), extends `StatefulIngestionSourceBase` |
| `snowflake_config.py` | Config (`SnowflakeV2Config`) |
| `snowflake_schema.py` | Data models (`SnowflakeTable`, `SnowflakeView`, `SnowflakeStream`, etc.) + `SnowflakeDataDictionary` for querying metadata |
| `snowflake_schema_gen.py` | `SnowflakeSchemaGenerator` — emits MCPs for datasets/containers |
| `snowflake_query.py` | `SnowflakeQuery` — SQL query builder (static methods) |
| `snowflake_lineage_v2.py` | Lineage extraction via `SqlParsingAggregator` |
| `snowflake_utils.py` | Shared mixins: `SnowflakeIdentifierBuilder`, `SnowflakeFilter`, `SnowsightUrlBuilder` |
| `constants.py` | `SnowflakeObjectDomain` enum, cloud providers |
| `subtypes.py` (common) | `DatasetSubTypes`, `DatasetContainerSubTypes`, `DataJobSubTypes`, `FlowContainerSubTypes` |

**Patterns to follow**:
- Data models as `@dataclass` in `snowflake_schema.py`
- SQL queries as static methods in `SnowflakeQuery`
- Metadata fetching via `SnowflakeDataDictionary`
- MCP emission in `SnowflakeSchemaGenerator` (or new dedicated generators)
- Feature toggles in `SnowflakeV2Config`

### Snowflake Metadata Sources

| Object | Metadata Source | Key Columns |
|--------|----------------|-------------|
| **Stages** | `SHOW STAGES IN SCHEMA <schema>` | `name`, `database_name`, `schema_name`, `owner`, `comment`, `type` (INTERNAL/EXTERNAL), `url`, `cloud`, `region`, `storage_integration`, `created_on` |
| **Tasks** | `SHOW TASKS IN SCHEMA <schema>` | `name`, `database_name`, `schema_name`, `owner`, `comment`, `warehouse`, `schedule`, `predecessors`, `state`, `definition`, `condition`, `allow_overlapping_execution`, `created_on` |
| **Pipes** | `SHOW PIPES IN SCHEMA <schema>` | `name`, `database_name`, `schema_name`, `owner`, `comment`, `definition`, `auto_ingest`, `notification_channel`, `created_on` |

**Permissions required**: `USAGE` on database/schema + object-level permissions (typically `MONITOR` or `OWNERSHIP` for pipes/tasks, `USAGE` for stages).

---

## Entity Mapping

| Source Concept | DataHub Entity | Entity Subtype | URN Format | Notes |
|----------------|---------------|----------------|------------|-------|
| Stage | Container | `SNOWFLAKE_STAGE` | `urn:li:container:<guid>` | New subtype. Nested under Schema container. |
| Internal Stage Data | Dataset | `SNOWFLAKE_STAGE_DATA` | `urn:li:dataset:(urn:li:dataPlatform:snowflake,<db>.<schema>.<stage_name>,<env>)` | Placeholder dataset inside internal stage container. Has `datasetProperties` but no `schemaMetadata`. Used as upstream in pipe lineage. |
| Task | DataJob | `SNOWFLAKE_TASK` | `urn:li:dataJob:(urn:li:dataFlow:(snowflake,<db>.<schema>.tasks,<env>),<task_name>)` | Grouped into per-schema DataFlows |
| Pipe (Snowpipe) | DataJob | `SNOWFLAKE_PIPE` | `urn:li:dataJob:(urn:li:dataFlow:(snowflake,<db>.<schema>.pipes,<env>),<pipe_name>)` | Grouped into per-schema DataFlows |
| Task DAG Flow | DataFlow | `SNOWFLAKE_TASK_GROUP` | `urn:li:dataFlow:(snowflake,<db>.<schema>.tasks,<env>)` | Per-schema grouping of tasks |
| Pipe Flow | DataFlow | `SNOWFLAKE_PIPE_GROUP` | `urn:li:dataFlow:(snowflake,<db>.<schema>.pipes,<env>)` | Per-schema grouping of pipes |

### Container Hierarchy (Stages)

```
Database Container (existing)
  └── Schema Container (existing)
        └── Stage Container (NEW — SNOWFLAKE_STAGE)
              └── Dataset (internal stages only — SNOWFLAKE_STAGE_DATA)
```

**Internal stages** get a child Dataset entity with:
- `datasetProperties`: description = "Internal stage data managed by Snowflake", customProperties = { stage_type, stage_name }
- `subTypes`: `SNOWFLAKE_STAGE_DATA`
- `container`: parent stage container
- No `schemaMetadata` (opaque storage — no columns to report)

**External stages** do NOT get a child dataset — the resolved S3/GCS/Azure dataset URN is used directly in pipe lineage instead.

### Flow Hierarchy (Tasks & Pipes)

```
DataFlow: <db>.<schema>.tasks
  └── DataJob: task_1 (SNOWFLAKE_TASK)
  └── DataJob: task_2 (SNOWFLAKE_TASK)  -- predecessors: [task_1]

DataFlow: <db>.<schema>.pipes
  └── DataJob: pipe_1 (SNOWFLAKE_PIPE)
```

### Lineage

**Stage lineage differs by type:**

- **External Stages**: The stage is a named pointer to an S3/GCS/Azure path — it does NOT copy data. For lineage, we resolve the stage URL to the underlying cloud storage dataset URN and use that as the Pipe's input directly. The stage Container exists for discovery/browsing only, not in the lineage chain.
- **Internal Stages**: Snowflake-managed opaque storage. A **placeholder Dataset** (`SNOWFLAKE_STAGE_DATA`) is emitted inside the stage container with `datasetProperties` (description, stage metadata in custom properties) but no `schemaMetadata`. This dataset is used as the pipe's upstream input, providing a **uniform lineage chain** for all pipe types.

| Lineage Type | Source → Target | Method | Applies To |
|-------------|----------------|--------|------------|
| Task → Dataset | Parse SQL in task `definition` | `SqlParsingAggregator` (existing) | All tasks |
| Task → Task | `predecessors` field | `DataJobInputOutput.inputDatajobs` | Tasks with predecessors |
| Pipe → Table | Parse `COPY INTO` target | Extract target table from definition | All pipes |
| Pipe ← S3/GCS/Azure | Resolve external stage URL → cloud dataset URN | `make_s3_urn_for_lineage` etc. (existing utils) | External stage pipes (always — consistent with existing COPY_HISTORY lineage) |
| Pipe ← Internal Stage Dataset | Internal stage placeholder dataset | Emitted by `SnowflakeStagesExtractor` | Internal stage pipes (always) |

**Pipe lineage is now uniform — every pipe has an upstream:**

```
External stage pipe:
  S3 Dataset (urn:li:dataset:s3://bucket/path) ──→ Pipe DataJob ──→ Snowflake Table
  (stage URL always resolved — consistent with existing COPY_HISTORY lineage pattern)

Internal stage pipe:
  Internal Stage Dataset ──→ Pipe DataJob ──→ Snowflake Table
  (dataset has datasetProperties with stage metadata, no schema)
```

**Note:** The existing Snowflake connector already resolves S3 URLs to dataset URNs unconditionally
in `SnowflakeLineageExtractor.get_external_upstreams()` via `make_s3_urn_for_lineage()`. We follow
the same pattern — no config flag needed. The URN is emitted regardless of whether the S3/GCS/Azure
source is also ingested; lineage resolves in the UI if/when it is.

---

## Architecture Decisions

### 1. New Subtypes to Add

**In `subtypes.py`:**
```python
# DatasetSubTypes
SNOWFLAKE_STAGE_DATA = "Snowflake Stage Data"

# DatasetContainerSubTypes
SNOWFLAKE_STAGE = "Snowflake Stage"

# FlowContainerSubTypes (or new enum)
SNOWFLAKE_TASK_GROUP = "Snowflake Task Group"
SNOWFLAKE_PIPE_GROUP = "Snowflake Pipe Group"

# DataJobSubTypes
SNOWFLAKE_TASK = "Snowflake Task"
SNOWFLAKE_PIPE = "Snowflake Pipe"
```

**In `constants.py` (`SnowflakeObjectDomain`):**
```python
STAGE = "stage"
TASK = "task"
PIPE = "pipe"
```

### 2. New Data Models (in `snowflake_schema.py`)

```python
@dataclass
class SnowflakeStage:
    name: str
    created: datetime
    owner: str
    database_name: str
    schema_name: str
    comment: Optional[str]
    stage_type: str  # INTERNAL or EXTERNAL
    url: Optional[str]  # For external stages
    cloud: Optional[str]  # aws/gcp/azure
    region: Optional[str]
    storage_integration: Optional[str]
    # File format info in custom properties

@dataclass
class SnowflakeTask:
    name: str
    created: datetime
    owner: str
    database_name: str
    schema_name: str
    comment: Optional[str]
    warehouse: Optional[str]
    schedule: Optional[str]
    predecessors: List[str]
    state: str  # STARTED, SUSPENDED
    definition: str  # SQL body
    condition: Optional[str]  # WHEN clause
    allow_overlapping_execution: bool
    owner_role_type: str

@dataclass
class SnowflakePipe:
    name: str
    created: datetime
    owner: str
    database_name: str
    schema_name: str
    comment: Optional[str]
    definition: str  # COPY INTO statement
    auto_ingest: bool
    notification_channel: Optional[str]
```

### 3. New SQL Queries (in `snowflake_query.py`)

```python
@staticmethod
def show_stages_for_schema(schema_name: str, db_name: str) -> str: ...

@staticmethod
def show_tasks_for_schema(schema_name: str, db_name: str) -> str: ...

@staticmethod
def show_pipes_for_schema(schema_name: str, db_name: str) -> str: ...
```

### 4. Config Additions (in `snowflake_config.py`)

```python
class StagesConfig(ConfigModel):
    enabled: bool = Field(default=False, description="Enable ingestion of Snowflake Stages as containers.")

class TasksConfig(ConfigModel):
    enabled: bool = Field(default=False, description="Enable ingestion of Snowflake Tasks as DataJobs.")

class PipesConfig(ConfigModel):
    enabled: bool = Field(default=False, description="Enable ingestion of Snowflake Snowpipe as DataJobs.")

# Added to SnowflakeV2Config:
stages: StagesConfig = Field(default_factory=StagesConfig, description="...")
tasks: TasksConfig = Field(default_factory=TasksConfig, description="...")
pipes: PipesConfig = Field(default_factory=PipesConfig, description="...")
```

All three features **disabled by default** (consistent with `streamlit_config` pattern).

### 5. New Source Modules

| File | Purpose |
|------|---------|
| `snowflake_stages.py` | `SnowflakeStagesExtractor` — fetch stages, emit Container MCPs |
| `snowflake_tasks.py` | `SnowflakeTasksExtractor` — fetch tasks, emit DataFlow/DataJob MCPs, parse SQL for lineage |
| `snowflake_pipes.py` | `SnowflakePipesExtractor` — fetch pipes, emit DataFlow/DataJob MCPs, parse COPY INTO for lineage |

Each extractor follows the same pattern as existing extractors (e.g., `SnowflakeAssertionsHandler`, `SnowflakeSharesHandler`):
- Receives `config`, `report`, `connection` in constructor
- Has a `get_workunits()` → `Iterable[MetadataWorkUnit]` method
- Called from `SnowflakeV2Source.get_workunits_internal()`

### 6. URN Construction

**Stage Container Key** (new, in `snowflake_utils.py` or inline):
```python
# Uses gen_containers() pattern with a new StageKey
# guid = hash(platform, instance, database, schema, stage_name)
```

**DataFlow URNs** (tasks/pipes):
```python
# Tasks: make_data_flow_urn("snowflake", f"{db}.{schema}.tasks", env)
# Pipes: make_data_flow_urn("snowflake", f"{db}.{schema}.pipes", env)
```

**DataJob URNs**:
```python
# Task: make_data_job_urn_with_flow(flow_urn, task_name)
# Pipe: make_data_job_urn_with_flow(flow_urn, pipe_name)
```

---

## Capabilities

| Capability | Priority | Notes |
|-----------|----------|-------|
| CONTAINERS (Stages) | Required | Stage containers under schema (both internal and external) |
| DATA_FLOW (Tasks, Pipes) | Required | Per-schema flow groupings |
| DATA_JOB (Tasks, Pipes) | Required | Individual tasks and pipes as jobs |
| LINEAGE_COARSE (Tasks) | Required | SQL parsing of task definitions |
| LINEAGE_COARSE (Pipes) | Required | Parse COPY INTO for pipe→table (downstream) |
| LINEAGE_COARSE (Pipe upstream) | Required | External stage pipes: always resolve stage URL → S3/GCS/Azure dataset URN (consistent with existing COPY_HISTORY pattern). Internal stage pipes: use placeholder dataset. |

---

## Configuration Example

```yaml
source:
  type: snowflake
  config:
    account_id: myaccount
    username: datahub
    password: ${SNOWFLAKE_PASSWORD}

    # Existing config...
    schema_pattern:
      allow: ["PUBLIC"]

    # NEW: Stages (disabled by default)
    stages:
      enabled: true

    # NEW: Tasks (disabled by default)
    tasks:
      enabled: true

    # NEW: Snowpipe (disabled by default)
    pipes:
      enabled: true
```

---

## Testing Strategy

| Test Type | Scope | Location |
|-----------|-------|----------|
| Unit: Stage data model | Config validation, Stage→Container MCP generation | `tests/unit/snowflake/test_snowflake_stages.py` |
| Unit: Task data model | Config validation, Task→DataJob MCP generation, SQL parsing | `tests/unit/snowflake/test_snowflake_tasks.py` |
| Unit: Pipe data model | Config validation, Pipe→DataJob MCP generation, COPY INTO parsing | `tests/unit/snowflake/test_snowflake_pipes.py` |
| Unit: Lineage | Task SQL → upstream/downstream datasets | `tests/unit/snowflake/test_snowflake_tasks.py` |
| Unit: Task DAG | Predecessor → `inputDatajobs` mapping | `tests/unit/snowflake/test_snowflake_tasks.py` |
| Integration | Full extraction with mocked Snowflake responses + golden file | `tests/integration/snowflake/test_snowflake_stages_tasks_pipes.py` |

**Golden file requirements**:
- At least 1 internal stage + 1 external stage (both as Containers)
- At least 2 tasks with DAG dependency (predecessor relationship)
- At least 1 pipe referencing an external stage + 1 pipe referencing an internal stage
- Container hierarchy: Database → Schema → Stage (for both stage types)
- DataFlow → DataJob hierarchy for tasks and pipes
- Lineage edges:
  - Task → Dataset (from SQL parsing)
  - Task → Task (predecessor DAG)
  - External stage pipe: S3 Dataset → Pipe → Snowflake Table (always resolved)
  - Internal stage pipe: Internal Stage Dataset → Pipe → Snowflake Table
  - Internal stage dataset has `datasetProperties` but no `schemaMetadata`

---

## Known Limitations

| Limitation | Impact | Workaround |
|-----------|--------|------------|
| File-level tracking not supported | Cannot track individual files loaded by Snowpipe | Start with stage-level lineage; evaluate based on customer feedback |
| Internal stage dataset has no schema | Users clicking the internal stage dataset see properties but no columns | By design — internal stages are opaque Snowflake-managed storage; `datasetProperties` provides context (description, stage metadata) |
| External stage lineage requires external datasets in DataHub | If S3/GCS bucket isn't ingested, lineage target won't resolve in UI | URN is always emitted (consistent with existing COPY_HISTORY pattern) — resolves if/when the cloud storage source is also ingested |
| Task WHEN clauses not rendered in UI | Conditional execution logic not visualized | Stored in custom properties |
| Pipe notification channel details | SQS/SNS details are opaque | Captured as custom properties |
| Stage Container not in lineage chain | Stages appear in browse/search but not in lineage graph | By design — stages are discovery/metadata entities. For external stages, the resolved S3/GCS/Azure dataset URN is used in lineage instead |

---

## Implementation Plan

### Phase 1: Stages (Foundation)

- [ ] Add `SnowflakeStage` dataclass to `snowflake_schema.py`
- [ ] Add `STAGE` to `SnowflakeObjectDomain` in `constants.py`
- [ ] Add `SNOWFLAKE_STAGE` to `DatasetContainerSubTypes` in `subtypes.py`
- [ ] Add `show_stages_for_schema()` to `SnowflakeQuery`
- [ ] Add stage fetching to `SnowflakeDataDictionary`
- [ ] Add `StagesConfig` to `snowflake_config.py`
- [ ] Create `snowflake_stages.py` with `SnowflakeStagesExtractor`
  - Emit Container MCPs for **both** internal and external stages
  - Aspects: `containerProperties`, `subTypes` (`SNOWFLAKE_STAGE`), parent `container` (schema)
  - Custom properties: stage_type (INTERNAL/EXTERNAL), url (external only), cloud, region, storage_integration, file_format
  - **For internal stages**: emit a child Dataset (`SNOWFLAKE_STAGE_DATA`) with:
    - `datasetProperties`: description = "Internal stage data managed by Snowflake", customProperties = { stage_type: "INTERNAL", stage_name: "<name>" }
    - `subTypes`: `SNOWFLAKE_STAGE_DATA`
    - `container`: parent stage container URN
    - No `schemaMetadata` (opaque storage)
  - Build a lookup map of stage name → stage metadata + dataset URN (used by PipesExtractor in Phase 3)
- [ ] Wire into `SnowflakeV2Source.get_workunits_internal()`
- [ ] Add stage names to `SnowflakeSchema` dataclass for tracking
- [ ] Unit tests: `tests/unit/snowflake/test_snowflake_stages.py`

### Phase 2: Tasks (High Customer Demand)

- [ ] Add `SnowflakeTask` dataclass to `snowflake_schema.py`
- [ ] Add `TASK` to `SnowflakeObjectDomain`
- [ ] Add `SNOWFLAKE_TASK` to `DataJobSubTypes`, `SNOWFLAKE_TASK_GROUP` to `FlowContainerSubTypes`
- [ ] Add `show_tasks_for_schema()` to `SnowflakeQuery`
- [ ] Add task fetching to `SnowflakeDataDictionary`
- [ ] Add `TasksConfig` to `snowflake_config.py`
- [ ] Create `snowflake_tasks.py` with `SnowflakeTasksExtractor`
  - Emit DataFlow MCP per schema (grouping)
  - Emit DataJob MCP per task with:
    - `DataJobInfo` (name, description, custom properties: schedule, warehouse, state, condition)
    - `DataJobInputOutput` (parsed SQL → upstream/downstream datasets + predecessor tasks via `inputDatajobs`)
    - `SubTypes` aspect with `SNOWFLAKE_TASK`
    - `Ownership` from task owner
  - Use `SqlParsingAggregator` (existing) for SQL parsing of task definitions
- [ ] Wire into `SnowflakeV2Source.get_workunits_internal()`
- [ ] Unit tests: `tests/unit/snowflake/test_snowflake_tasks.py`
  - Task DAG with predecessors
  - SQL definition parsing → lineage

### Phase 3: Snowpipe (Depends on Stages)

- [ ] Add `SnowflakePipe` dataclass to `snowflake_schema.py`
- [ ] Add `PIPE` to `SnowflakeObjectDomain`
- [ ] Add `SNOWFLAKE_PIPE` to `DataJobSubTypes`, `SNOWFLAKE_PIPE_GROUP` to `FlowContainerSubTypes`
- [ ] Add `show_pipes_for_schema()` to `SnowflakeQuery`
- [ ] Add pipe fetching to `SnowflakeDataDictionary`
- [ ] Add `PipesConfig` to `snowflake_config.py`
- [ ] Create `snowflake_pipes.py` with `SnowflakePipesExtractor`
  - Emit DataFlow MCP per schema (grouping)
  - Emit DataJob MCP per pipe with:
    - `DataJobInfo` (name, description, custom properties: auto_ingest, notification_channel, stage_name, stage_type)
    - `DataJobInputOutput`:
      - **Downstream** (always): Target table dataset URN (from COPY INTO's `INTO table_name`)
      - **Upstream** (external stage): Always resolve stage URL → S3/GCS/Azure dataset URN using `make_s3_urn_for_lineage` / equivalent GCS/Azure utils. Consistent with existing `get_external_upstreams()` pattern — no config flag.
      - **Upstream** (internal stage): Internal Stage Dataset URN (placeholder dataset emitted by StagesExtractor)
    - `SubTypes` aspect with `SNOWFLAKE_PIPE`
    - `Ownership` from pipe owner
  - Parse COPY INTO statement to extract stage reference and target table
  - Consume stage lookup map from `SnowflakeStagesExtractor` to determine stage type and URL
- [ ] Wire into `SnowflakeV2Source.get_workunits_internal()`
- [ ] Unit tests: `tests/unit/snowflake/test_snowflake_pipes.py`

### Phase 4: Integration Tests & Documentation

- [ ] Integration test with golden file covering all three features
- [ ] Update connector documentation (`metadata-ingestion/docs/sources/snowflake/`)
- [ ] Update `snowflake_recipe.yml` with new config options
- [ ] Verify stale entity removal works for stages/tasks/pipes

---

## Open Technical Decisions

| Decision | Options | Recommendation | Status |
|----------|---------|----------------|--------|
| Stage entity model | Container vs Dataset | **Container** — stages are storage locations without schemas. They exist for discovery, not lineage. | Decided |
| Stage role in lineage | Stage in lineage chain vs resolve to underlying storage | **Resolve to underlying storage** — external stages resolve their URL to S3/GCS/Azure dataset URNs for pipe upstream lineage. Internal stages emit a placeholder Dataset (`SNOWFLAKE_STAGE_DATA`) used as pipe upstream. Stage Container is for browsing only. | Decided |
| Internal vs external stage handling | Same treatment vs differentiated | **Differentiated** — both emitted as Containers. Internal stages additionally emit a child Dataset (with `datasetProperties`, no schema) used as pipe upstream. External stages resolve to S3/GCS/Azure URNs. | Decided |
| Internal stage dataset | No upstream vs placeholder dataset | **Placeholder dataset** — `SNOWFLAKE_STAGE_DATA` subtype, `datasetProperties` with description + custom properties (stage_type, stage_name), no `schemaMetadata`. Provides uniform lineage: every pipe has an upstream. | Decided |
| File-level tracking | Stage-level only vs virtual datasets for file patterns | **Stage-level only** — evaluate based on customer feedback | Per ticket |
| External stage lineage | Auto-create vs configurable | **Always resolve** — no config flag. Consistent with existing `get_external_upstreams()` in `snowflake_lineage_v2.py` which always resolves S3 URLs via `make_s3_urn_for_lineage()`. URN emitted regardless; resolves in UI when cloud storage source is also ingested. | Decided |
| Task WHEN clause representation | Custom properties vs dedicated UI | **Custom properties** — evaluate UI later | Per ticket |

---

## Approval

- [ ] User approved this plan on: _pending_
- [ ] Approval message: "_pending_"
