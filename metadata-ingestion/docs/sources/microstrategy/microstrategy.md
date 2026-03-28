---
sidebar_position: 42
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MicroStrategy
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)

## Overview

This connector extracts metadata from MicroStrategy's REST API, enabling you to catalog and understand your MicroStrategy BI assets within DataHub.

### Important Capabilities

| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. Supported for types - Project, Folder. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled via stateful ingestion. |
| [Domains](../../../domains.md) | ✅ | Supported via the `domain` config field. |
| Ownership | ✅ | Enabled by default via `include_ownership`. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled by default via `include_lineage`. |

### Extracted Metadata

This plugin extracts the following metadata from MicroStrategy:

- **Projects** as containers with hierarchical organization
- **Folders** as nested containers within projects
- **Dashboards (Dossiers)** with descriptions, URLs, and custom properties
- **Reports** as charts with metadata and lineage
- **Intelligent Cubes** as datasets with schema information
- **Datasets** with attributes and metrics
- **Lineage** showing data flow from cubes/datasets to dashboards and reports
- **Ownership** information (creators and owners)

#### Concept Mapping

This ingestion source maps the following MicroStrategy concepts to DataHub entities:

| Source Concept | DataHub Concept | Notes |
| -------------- | --------------- | ----- |
| `"MicroStrategy"` | [Data Platform](../../metamodel/entities/dataPlatform.md) | |
| Project | [Container](../../metamodel/entities/container.md) | SubType `"Project"` |
| Folder | [Container](../../metamodel/entities/container.md) | SubType `"Folder"` |
| Dashboard (Dossier) | [Dashboard](../../metamodel/entities/dashboard.md) | |
| Report | [Chart](../../metamodel/entities/chart.md) | Reports are represented as Chart entities |
| Intelligent Cube | [Dataset](../../metamodel/entities/dataset.md) | SubType `"Cube"` |
| Dataset | [Dataset](../../metamodel/entities/dataset.md) | |
| User (Owner) | [User (a.k.a CorpUser)](../../metamodel/entities/corpuser.md) | Optionally Extracted via `include_ownership` |

#### Lineage

When `include_lineage` is enabled, the connector extracts:

- **Dashboard → Report**: Dashboards link to their constituent reports via `DashboardInfo.charts` array
- **Report → Dataset/Cube**: Reports link to their data sources via `ChartInfo.inputs` array

Optional **warehouse (physical table) lineage** for Intelligent Cubes: set `include_warehouse_lineage: true` and configure `warehouse_lineage_platform` (and usually `warehouse_lineage_database` / `warehouse_lineage_schema`) so dataset URNs match your warehouse ingestion. The connector calls `GET /api/model/cubes/{id}` and emits `upstreamLineage` on the cube dataset from `physicalTables`.

**Project availability**: By default only projects with `status: 0` (loaded on IServer) are ingested. Set `include_unloaded_projects: true` to include idle/unloaded projects (may hit IServer ERR001-style errors).

Cubes are discovered via search with `cube_search_object_type` (default `776`). The REST client also recognizes MicroStrategy JSON `iServerCode` for unloaded projects and treats legacy **ClassCast**-style HTTP 500 responses as empty definitions for dossiers/cubes when appropriate.

## Prerequisites

### MicroStrategy Environment

- MicroStrategy Library web application with REST API enabled
- MicroStrategy version 10.11 or later (REST API v2 required)
- Network access to MicroStrategy from DataHub environment

### Authentication

Choose one of the following authentication methods:

**Standard Authentication** (Recommended for production):
- Service account with metadata read permissions
- Required permissions:
  - Access to projects you want to ingest
  - Read access to dashboards, reports, and cubes
  - User permissions to query metadata via REST API

**Anonymous Guest Access** (Demo instances only):
- Only works on MicroStrategy instances configured for guest access
- Limited to public demo environments like demo.microstrategy.com
- Not recommended for production use

### Required Permissions

The service account needs:
- **Browse** permission on target projects
- **Read** permission on dashboards (dossiers), reports, and cubes
- **Use** permission for REST API access

## Installation

```bash
pip install 'acryl-datahub[microstrategy]'
```

## Quickstart

### Basic Configuration

<Tabs>
<TabItem value="standard" label="Standard Authentication" default>

```yaml
source:
  type: microstrategy
  config:
    # Connection settings
    connection:
      base_url: https://your-instance.microstrategy.com/MicroStrategyLibrary
      username: ${MSTR_USERNAME}
      password: ${MSTR_PASSWORD}
      timeout_seconds: 30
      max_retries: 3

sink:
  type: datahub-rest
  config:
    server: http://localhost:8080
```

</TabItem>
<TabItem value="anonymous" label="Anonymous Access (Demo)">

```yaml
source:
  type: microstrategy
  config:
    # Connection settings for demo instances
    connection:
      base_url: https://demo.microstrategy.com/MicroStrategyLibrary
      use_anonymous: true
      timeout_seconds: 30

sink:
  type: datahub-rest
  config:
    server: http://localhost:8080
```

</TabItem>
</Tabs>

### Advanced Configuration

```yaml
source:
  type: microstrategy
  config:
    # Connection settings
    connection:
      base_url: https://your-instance.microstrategy.com/MicroStrategyLibrary
      username: ${MSTR_USERNAME}
      password: ${MSTR_PASSWORD}
      timeout_seconds: 60
      max_retries: 5

    # Filtering patterns
    project_pattern:
      allow:
        - "^Production.*"
        - "^Analytics.*"
      deny:
        - "^Test.*"
        - "^Dev.*"

    dashboard_pattern:
      deny:
        - "^Personal.*"
        - "^Draft.*"

    # Feature flags
    include_lineage: true
    include_ownership: true
    include_cube_schema: true

    # Platform instance
    platform_instance: prod-mstr

    # Stateful ingestion for deleted entity detection
    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true

sink:
  type: datahub-rest
  config:
    server: http://localhost:8080
```

## Configuration Options

<Tabs>
<TabItem value="options" label="Options" default>

Note that a `.` is used to denote nested fields in the YAML recipe.

<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">connection</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">MicroStrategyConnectionConfig</span></div> | Connection settings for MicroStrategy REST API |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">base_url</span>&nbsp;<abbr title="Required">✅</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Base URL for MicroStrategy REST API (e.g., 'https://demo.microstrategy.com/MicroStrategyLibrary'). Should be the URL to the MicroStrategy Library web application. Do not include '/api' suffix - it will be appended automatically. |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">username</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | MicroStrategy username for authentication. For demo instances, you can use anonymous access by setting use_anonymous=True. For production, use a service account with metadata read permissions. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">password</span></div> <div className="type-name-line"><span className="type-name">string(password)</span></div> | Password for MicroStrategy authentication. Required if username is provided. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">use_anonymous</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Use anonymous guest access (for demo instances). When enabled, username/password are not required. Only works on MicroStrategy instances configured for guest access. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">timeout_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | HTTP request timeout in seconds. Increase for slow networks or large metadata responses. <div className="default-line default-line-with-docs">Default: <span className="default-value">30</span></div> |
| <div className="path-line"><span className="path-prefix">connection.</span><span className="path-main">max_retries</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Maximum number of retry attempts for failed API requests. Uses exponential backoff. <div className="default-line default-line-with-docs">Default: <span className="default-value">3</span></div> |
| <div className="path-line"><span className="path-main">project_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | Regex patterns to filter projects. Example: {'allow': ['^Production.*'], 'deny': ['^Test.*']}. Only matching projects will be ingested. <div className="default-line default-line-with-docs">Default: <span className="default-value">Allow all</span></div> |
| <div className="path-line"><span className="path-main">folder_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | Regex patterns to filter folders within projects. <div className="default-line default-line-with-docs">Default: <span className="default-value">Allow all</span></div> |
| <div className="path-line"><span className="path-main">dashboard_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | Regex patterns to filter dashboards (dossiers). Applies to dashboard names. Use to exclude personal or test dashboards. <div className="default-line default-line-with-docs">Default: <span className="default-value">Allow all</span></div> |
| <div className="path-line"><span className="path-main">report_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | Regex patterns to filter reports. <div className="default-line default-line-with-docs">Default: <span className="default-value">Allow all</span></div> |
| <div className="path-line"><span className="path-main">include_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract lineage between dashboards/reports and cubes/datasets. Shows data flow from cubes to visualizations. Critical for impact analysis. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_usage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract usage statistics (view counts, user access). **Note: Not yet implemented - this flag currently has no effect.** <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-main">include_ownership</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract ownership information (creators, owners). Automatically links dashboards/reports to their creators. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_cube_schema</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Extract schema (attributes, metrics) from Intelligent Cubes. Required for column-level lineage. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">StatefulStaleMetadataRemovalConfig</span></div> | Automatically remove deleted dashboards/reports from DataHub. Maintains catalog accuracy when objects are deleted in MicroStrategy. Recommended for production environments. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>

</TabItem>
</Tabs>

## Known Limitations

### Usage Statistics (Not Yet Implemented)

The `include_usage` configuration flag exists but usage statistics extraction is not yet implemented. Setting this flag to `true` will have no effect. This feature is planned for a future release.

- **Affects**: Usage statistics (view counts, user access patterns)
- **Workaround**: None currently available
- **Planned**: Future implementation will extract usage data from MicroStrategy audit logs API

### Lineage for Complex Dashboards

For production environments with large or complex MicroStrategy dashboards, lineage extraction may take significant time or fail if the MicroStrategy API returns errors for dashboard definitions.

- **Affects**: Dashboard → Cube/Dataset lineage
- **Workaround**: Configure API timeouts appropriately (`timeout_seconds` and `max_retries`), use stateful ingestion for retries
- **Note**: The connector properly handles API errors and continues processing other entities

### Column-Level Lineage

Column-level lineage from cubes to dashboard visualizations is not currently supported. Only table-level (cube-level) lineage is extracted.

- **Affects**: Fine-grained column lineage
- **Workaround**: Use table-level lineage for impact analysis

## Troubleshooting

### Connection Issues

**Problem**: Unable to connect to MicroStrategy REST API

**Solutions**:
- Verify the `base_url` is correct and accessible from your DataHub environment
- Test connectivity: `curl https://your-instance.com/MicroStrategyLibrary/api/status`
- Check firewall rules and network access
- Ensure MicroStrategy REST API is enabled in your environment

### Authentication Failures

**Problem**: Authentication fails with "Invalid credentials" or "Unauthorized"

**Solutions**:
- Verify username and password are correct
- Check that the service account has appropriate permissions
- For anonymous access, verify the instance supports guest access
- Test authentication manually:
  ```bash
  curl -X POST "https://your-instance.com/MicroStrategyLibrary/api/auth/login" \
    -H "Content-Type: application/json" \
    -d '{"username": "your-user", "password": "your-password"}'
  ```

### Missing Dashboards or Reports

**Problem**: Some dashboards or reports are not appearing in DataHub

**Solutions**:
- Check the `dashboard_pattern` and `report_pattern` filters in your configuration
- Verify the service account has read permissions on the missing objects
- Check DataHub ingestion logs for filtering messages or API errors
- Ensure the objects exist in allowed projects (check `project_pattern`)

### Slow Ingestion Performance

**Problem**: Ingestion is taking a long time to complete

**Solutions**:
- Increase `timeout_seconds` for slow API responses
- Reduce the scope using pattern filters (`project_pattern`, `dashboard_pattern`)
- Check MicroStrategy API performance and rate limiting
- Monitor network latency between DataHub and MicroStrategy

**Note**: Large MicroStrategy environments (>2,000 entities) may take 45-60+ minutes for initial ingestion. This is normal due to API pagination and rate limiting. Consider using incremental ingestion with `stateful_ingestion` for subsequent runs.

### HTTP 500 Errors from MicroStrategy API

**Problem**: Seeing many HTTP 500 errors in ingestion logs

**Solutions**:
- This often indicates MicroStrategy API issues or resource constraints
- The connector will continue processing other entities (fail gracefully)
- Check MicroStrategy server logs for API errors
- Contact MicroStrategy support if errors persist
- Consider increasing `max_retries` to handle transient errors

### Empty Lineage

**Problem**: No lineage edges are showing up in DataHub

**Solutions**:
- Verify `include_lineage: true` is set in the configuration
- Check that dashboards/reports actually reference cubes or datasets
- Review ingestion logs for lineage extraction errors
- Ensure the MicroStrategy API returns dashboard definitions (not HTTP 500)

## Compatibility

- **MicroStrategy Version**: 10.11 or later (REST API v2 required)
- **DataHub Version**: 0.8.0 or later
- **Python**: 3.8 or later

## Code Coordinates

- Class Name: `datahub.ingestion.source.microstrategy.MicroStrategySource`
- Browse on [GitHub](../../../../metadata-ingestion/src/datahub/ingestion/source/microstrategy/source.py)

## Questions

If you've got any questions on configuring ingestion for MicroStrategy, feel free to ping us on [our Slack](https://datahub.com/slack).
