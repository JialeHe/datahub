"""Unit tests for MicroStrategy source connector."""

import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.microstrategy.client import (
    MicroStrategyClient,
    MicroStrategyProjectUnavailableError,
)
from datahub.ingestion.source.microstrategy.config import (
    MicroStrategyConfig,
    MicroStrategyConnectionConfig,
)
from datahub.ingestion.source.microstrategy.constants import ISERVER_PROJECT_UNAVAILABLE
from datahub.ingestion.source.microstrategy.source import (
    FolderKey,
    MicroStrategySource,
    ProjectKey,
)


def create_search_objects_mock(
    dashboards: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    reports: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    cubes: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> Callable[[str, int], List[Dict[str, Any]]]:
    """
    Helper to create search_objects mock function.

    Args:
        dashboards: Dict mapping project_id to list of dashboards
        reports: Dict mapping project_id to list of reports
        cubes: Dict mapping project_id to list of cubes

    Returns:
        Mock function compatible with search_objects(project_id, object_type)
    """
    dashboards = dashboards or {}
    reports = reports or {}
    cubes = cubes or {}

    def search_objects_side_effect(
        project_id: str, object_type: int
    ) -> List[Dict[str, Any]]:
        # type=776 is cubes (search), type=3 is reports, type=55 is dashboards
        if object_type == 55:  # Dashboards
            return dashboards.get(project_id, [])
        elif object_type == 3:  # Reports
            return reports.get(project_id, [])
        elif object_type == 776:  # Cubes
            return cubes.get(project_id, [])
        return []

    return search_objects_side_effect


class TestMicroStrategyConfigValidation:
    """Test configuration validation logic."""

    def test_base_url_validation_rejects_non_http(self):
        """Test that base_url must start with http:// or https://."""
        with pytest.raises(ValueError, match="base_url must start with http"):
            MicroStrategyConnectionConfig(base_url="ftp://invalid.com")

    def test_base_url_validation_accepts_http(self):
        """Test that http:// URLs are accepted."""
        config = MicroStrategyConnectionConfig(
            base_url="http://localhost:8080", use_anonymous=True
        )
        assert config.base_url == "http://localhost:8080"

    def test_base_url_validation_accepts_https(self):
        """Test that https:// URLs are accepted."""
        config = MicroStrategyConnectionConfig(
            base_url="https://demo.microstrategy.com", use_anonymous=True
        )
        assert config.base_url == "https://demo.microstrategy.com"

    def test_base_url_trailing_slashes_removed(self):
        """Test that trailing slashes are removed from base_url."""
        config = MicroStrategyConnectionConfig(
            base_url="https://demo.microstrategy.com///", use_anonymous=True
        )
        assert config.base_url == "https://demo.microstrategy.com"

    def test_include_warehouse_lineage_requires_platform(self):
        with pytest.raises(ValueError, match="warehouse_lineage_platform"):
            MicroStrategyConfig.parse_obj(
                {
                    "connection": {
                        "base_url": "https://demo.microstrategy.com",
                        "use_anonymous": True,
                    },
                    "include_warehouse_lineage": True,
                }
            )

    def test_include_warehouse_lineage_accepts_platform(self):
        cfg = MicroStrategyConfig.parse_obj(
            {
                "connection": {
                    "base_url": "https://demo.microstrategy.com",
                    "use_anonymous": True,
                },
                "include_warehouse_lineage": True,
                "warehouse_lineage_platform": "snowflake",
            }
        )
        assert cfg.warehouse_lineage_platform == "snowflake"


class TestContainerKeyURNGeneration:
    """Test that custom ContainerKey subclasses generate unique URNs."""

    def test_project_keys_generate_unique_urns(self):
        """Test that different projects generate different URNs."""
        project_key1 = ProjectKey(
            project="project_a", platform="microstrategy", instance=None, env="PROD"
        )
        project_key2 = ProjectKey(
            project="project_b", platform="microstrategy", instance=None, env="PROD"
        )

        urn1 = project_key1.as_urn()
        urn2 = project_key2.as_urn()

        # URNs should be different for different projects
        assert urn1 != urn2
        # Both should be valid container URNs
        assert urn1.startswith("urn:li:container:")
        assert urn2.startswith("urn:li:container:")

    def test_folder_keys_with_same_id_different_projects_generate_unique_urns(self):
        """Test that folders with same ID in different projects get unique URNs."""
        folder_key1 = FolderKey(
            project="project_a",
            folder="folder_1",
            platform="microstrategy",
            instance=None,
            env="PROD",
        )
        folder_key2 = FolderKey(
            project="project_b",
            folder="folder_1",
            platform="microstrategy",
            instance=None,
            env="PROD",
        )

        urn1 = folder_key1.as_urn()
        urn2 = folder_key2.as_urn()

        # URNs should be different because they're in different projects
        assert urn1 != urn2


class TestProjectFiltering:
    """Test project filtering logic."""

    def test_project_pattern_filters_projects(self):
        """Test that project patterns correctly filter projects."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
            "project_pattern": {
                "allow": ["^Production.*"],
                "deny": [".*Test$"],
            },
        }
        config = MicroStrategyConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "1", "name": "Production Main", "status": 0},
                {"id": "2", "name": "Production Test", "status": 0},  # filtered by deny
                {"id": "3", "name": "Dev Project", "status": 0},  # filtered by allow
                {"id": "4", "name": "Production Analytics", "status": 0},
            ]
            mock_client.get_folders.return_value = []
            mock_client.search_objects.side_effect = create_search_objects_mock()

            source = MicroStrategySource(config, ctx)
            workunits = list(source.get_workunits())

            # Check that only "Production Main" and "Production Analytics" were processed
            # Each container generates multiple aspects (containerProperties, status, etc.)
            # So we need to count unique container URNs
            container_urns = set(
                wu.metadata.entityUrn
                for wu in workunits
                if hasattr(wu.metadata, "entityUrn")
                and "container" in str(wu.metadata.entityUrn)
            )

            # Should have 2 unique project containers (Production Main, Production Analytics)
            assert len(container_urns) == 2


class TestCubeRegistryResolution:
    """Test cross-project cube registry resolution."""

    def test_cube_registry_resolves_cross_project_references(self):
        """Test that cubes from different projects can be resolved for lineage."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
            "include_lineage": True,
        }
        config = MicroStrategyConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            # Project A has a report that references cube from Project B
            mock_client.get_projects.return_value = [
                {"id": "project_a", "name": "Project A", "status": 0},
                {"id": "project_b", "name": "Project B", "status": 0},
            ]

            mock_client.search_objects.side_effect = create_search_objects_mock(
                cubes={
                    "project_b": [
                        {"id": "cube_123", "name": "Sales Cube", "description": ""}
                    ]
                },
                reports={
                    "project_a": [
                        {
                            "id": "report_1",
                            "name": "Sales Report",
                            "description": "",
                            "type": 3,
                            "subtype": 3,
                            "dataSource": {
                                "id": "cube_123"
                            },  # References cube from Project B
                        }
                    ]
                },
            )
            mock_client.get_folders.return_value = []

            source = MicroStrategySource(config, ctx)
            workunits = list(source.get_workunits())

            # Verify cube was registered
            assert "cube_123" in source.cube_registry
            assert source.cube_registry["cube_123"]["project_id"] == "project_b"

            # Verify report has lineage (inputs should contain the cube URN)
            chart_info_workunits = [
                wu
                for wu in workunits
                if hasattr(wu.metadata, "aspect")
                and wu.metadata.aspect.__class__.__name__ == "ChartInfoClass"
            ]

            assert len(chart_info_workunits) > 0
            chart_info = chart_info_workunits[0].metadata.aspect  # type: ignore
            assert chart_info is not None
            assert chart_info.customProperties["report_type"] == "3"  # type: ignore

            # Check that lineage includes the cube
            assert hasattr(chart_info, "inputs")
            assert len(chart_info.inputs) > 0  # type: ignore
            assert any("cube_123" in input_urn for input_urn in chart_info.inputs)  # type: ignore


class TestOwnershipExtraction:
    """Test ownership extraction logic."""

    def test_ownership_handles_string_owner(self):
        """Test that ownership aspect is created when owner is a string."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
            "include_ownership": True,
        }
        config = MicroStrategyConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "project_1", "name": "Test Project", "status": 0}
            ]
            mock_client.search_objects.side_effect = create_search_objects_mock(
                dashboards={
                    "project_1": [
                        {
                            "id": "dashboard_1",
                            "name": "Test Dashboard",
                            "description": "",
                            "owner": "john.doe",  # String owner
                        }
                    ]
                }
            )
            mock_client.get_folders.return_value = []

            source = MicroStrategySource(config, ctx)
            workunits = list(source.get_workunits())

            # Find ownership workunit
            ownership_workunits = [
                wu
                for wu in workunits
                if hasattr(wu.metadata, "aspect")
                and wu.metadata.aspect.__class__.__name__ == "OwnershipClass"
            ]

            assert len(ownership_workunits) > 0
            ownership = ownership_workunits[0].metadata.aspect  # type: ignore
            assert ownership is not None
            assert len(ownership.owners) == 1  # type: ignore
            assert "john.doe" in ownership.owners[0].owner  # type: ignore

    def test_ownership_handles_dict_owner(self):
        """Test that ownership aspect is created when owner is a dict with username."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
            "include_ownership": True,
        }
        config = MicroStrategyConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "project_1", "name": "Test Project", "status": 0}
            ]
            mock_client.search_objects.side_effect = create_search_objects_mock(
                dashboards={
                    "project_1": [
                        {
                            "id": "dashboard_1",
                            "name": "Test Dashboard",
                            "description": "",
                            "owner": {
                                "username": "jane.smith",
                                "email": "jane@example.com",
                            },
                        }
                    ]
                }
            )
            mock_client.get_folders.return_value = []

            source = MicroStrategySource(config, ctx)
            workunits = list(source.get_workunits())

            # Find ownership workunit
            ownership_workunits = [
                wu
                for wu in workunits
                if hasattr(wu.metadata, "aspect")
                and wu.metadata.aspect.__class__.__name__ == "OwnershipClass"
            ]

            assert len(ownership_workunits) > 0
            ownership = ownership_workunits[0].metadata.aspect  # type: ignore
            assert ownership is not None
            assert len(ownership.owners) == 1  # type: ignore
            assert "jane.smith" in ownership.owners[0].owner  # type: ignore


class TestDashboardVisualizationExtraction:
    """Test visualization extraction from dashboard definitions."""

    def test_visualization_ids_extracted_from_dashboard_chapters(self):
        """Test that visualization IDs are correctly extracted from dashboard definition."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
            "include_lineage": True,
        }
        config = MicroStrategyConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "project_1", "name": "Test Project", "status": 0}
            ]
            mock_client.search_objects.side_effect = create_search_objects_mock(
                dashboards={
                    "project_1": [
                        {
                            "id": "dashboard_1",
                            "name": "Test Dashboard",
                            "description": "",
                            "subtype": 14336,
                        }
                    ]
                }
            )

            # Modern dossier: chapters → pages → visualizations
            mock_client.get_dossier_definition.return_value = {
                "chapters": [
                    {
                        "name": "Chapter 1",
                        "pages": [
                            {
                                "visualizations": [
                                    {"key": "viz_1", "type": "chart"},
                                    {"key": "viz_2", "type": "grid"},
                                ],
                            }
                        ],
                    },
                    {
                        "name": "Chapter 2",
                        "pages": [
                            {
                                "visualizations": [
                                    {"key": "viz_3", "type": "map"},
                                ],
                            }
                        ],
                    },
                ]
            }

            mock_client.get_folders.return_value = []

            source = MicroStrategySource(config, ctx)
            workunits = list(source.get_workunits())

            # Find dashboard info workunit
            dashboard_info_workunits = [
                wu
                for wu in workunits
                if hasattr(wu.metadata, "aspect")
                and wu.metadata.aspect.__class__.__name__ == "DashboardInfoClass"
            ]

            assert len(dashboard_info_workunits) > 0
            dashboard_info = dashboard_info_workunits[0].metadata.aspect  # type: ignore
            assert dashboard_info is not None

            # Verify 3 chart URNs were created
            assert len(dashboard_info.charts) == 3  # type: ignore
            assert all("viz_" in chart_urn for chart_urn in dashboard_info.charts)  # type: ignore

            mock_client.get_dossier_definition.assert_called_once_with(
                "dashboard_1", "project_1"
            )

    def test_empty_dashboard_definition_handled_gracefully(self):
        """Test that dashboards with no visualizations don't crash the source."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
            "include_lineage": True,
        }
        config = MicroStrategyConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "project_1", "name": "Test Project", "status": 0}
            ]
            mock_client.search_objects.side_effect = create_search_objects_mock(
                dashboards={
                    "project_1": [
                        {
                            "id": "dashboard_1",
                            "name": "Empty Dashboard",
                            "description": "",
                            "subtype": 14336,
                        }
                    ]
                }
            )

            mock_client.get_dossier_definition.return_value = {"chapters": []}

            mock_client.get_folders.return_value = []

            source = MicroStrategySource(config, ctx)
            workunits = list(source.get_workunits())

            # Should still create dashboard workunit, just with empty charts list
            dashboard_info_workunits = [
                wu
                for wu in workunits
                if hasattr(wu.metadata, "aspect")
                and wu.metadata.aspect.__class__.__name__ == "DashboardInfoClass"
            ]

            assert len(dashboard_info_workunits) > 0
            dashboard_info = dashboard_info_workunits[0].metadata.aspect  # type: ignore
            assert dashboard_info is not None
            assert len(dashboard_info.charts) == 0  # type: ignore

            mock_client.get_dossier_definition.assert_called_once_with(
                "dashboard_1", "project_1"
            )


class TestCubeSchemaExtraction:
    """Test cube schema extraction logic."""

    def test_cube_schema_includes_attributes_and_metrics(self):
        """Test that cube schema correctly extracts attributes and metrics."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
            "include_cube_schema": True,
        }
        config = MicroStrategyConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "project_1", "name": "Test Project", "status": 0}
            ]
            mock_client.search_objects.side_effect = create_search_objects_mock(
                cubes={
                    "project_1": [
                        {
                            "id": "cube_1",
                            "name": "Sales Cube",
                            "description": "Sales data",
                        }
                    ]
                }
            )

            # Schema lives under definition.availableObjects (MSTR REST cube payload)
            mock_client.get_cube_schema.return_value = {
                "definition": {
                    "availableObjects": {
                        "attributes": [
                            {
                                "id": "attr_1",
                                "name": "Region",
                                "description": "Sales region",
                                "forms": [
                                    {
                                        "name": "ID",
                                        "dataType": "varChar",
                                        "baseFormCategory": "DESC",
                                    }
                                ],
                            },
                            {
                                "id": "attr_2",
                                "name": "Product",
                                "description": "Product name",
                                "forms": [
                                    {
                                        "name": "ID",
                                        "dataType": "varChar",
                                        "baseFormCategory": "DESC",
                                    }
                                ],
                            },
                        ],
                        "metrics": [
                            {
                                "id": "metric_1",
                                "name": "Revenue",
                                "description": "Total revenue",
                            },
                            {
                                "id": "metric_2",
                                "name": "Units",
                                "description": "Units sold",
                            },
                        ],
                    }
                }
            }
            mock_client.get_cube_sql_view.return_value = 'SELECT 1 FROM "S"."T"'

            mock_client.get_folders.return_value = []

            source = MicroStrategySource(config, ctx)
            workunits = list(source.get_workunits())

            # Find schema metadata workunit
            schema_workunits = [
                wu
                for wu in workunits
                if hasattr(wu.metadata, "aspect")
                and wu.metadata.aspect.__class__.__name__ == "SchemaMetadataClass"
            ]

            assert len(schema_workunits) > 0
            schema_metadata = schema_workunits[0].metadata.aspect  # type: ignore
            assert schema_metadata is not None

            # Verify fields include both attributes and metrics
            assert len(schema_metadata.fields) == 4  # type: ignore

            field_paths = [field.fieldPath for field in schema_metadata.fields]  # type: ignore
            assert "Region" in field_paths
            assert "Product" in field_paths
            assert "Revenue" in field_paths
            assert "Units" in field_paths

            view_workunits = [
                wu
                for wu in workunits
                if hasattr(wu.metadata, "aspect")
                and wu.metadata.aspect.__class__.__name__ == "ViewPropertiesClass"
            ]
            assert len(view_workunits) == 1
            assert "FROM" in view_workunits[0].metadata.aspect.viewLogic  # type: ignore

            # Verify native data types
            field_types = {
                field.fieldPath: field.nativeDataType
                for field in schema_metadata.fields  # type: ignore
            }
            assert field_types["Region"] == "attribute:DESC"
            assert field_types["Revenue"] == "metric"

    def test_cube_schema_extraction_failure_continues_ingestion(self):
        """Test that cube schema extraction failure doesn't stop ingestion."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
            "include_cube_schema": True,
        }
        config = MicroStrategyConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "project_1", "name": "Test Project", "status": 0}
            ]
            mock_client.search_objects.side_effect = create_search_objects_mock(
                cubes={
                    "project_1": [
                        {"id": "cube_1", "name": "Sales Cube", "description": ""}
                    ]
                }
            )

            # Mock schema extraction to fail
            mock_client.get_cube_schema.side_effect = Exception(
                "Permission denied to cube schema"
            )
            mock_client.get_cube_sql_view.return_value = ""

            mock_client.get_folders.return_value = []

            source = MicroStrategySource(config, ctx)
            workunits = list(source.get_workunits())

            # Should still emit dataset properties workunit for cube
            dataset_properties_workunits = [
                wu
                for wu in workunits
                if hasattr(wu.metadata, "aspect")
                and wu.metadata.aspect.__class__.__name__ == "DatasetPropertiesClass"
            ]

            assert len(dataset_properties_workunits) > 0

            # Should NOT emit schema metadata workunit
            schema_workunits = [
                wu
                for wu in workunits
                if hasattr(wu.metadata, "aspect")
                and wu.metadata.aspect.__class__.__name__ == "SchemaMetadataClass"
            ]

            assert len(schema_workunits) == 0


class TestURLConstruction:
    """Test external URL construction logic."""

    def test_dashboard_url_construction(self):
        """Test that dashboard URLs are correctly constructed."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com/MicroStrategyLibrary",
                "use_anonymous": True,
            },
        }
        config = MicroStrategyConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        source = MicroStrategySource(config, ctx)
        url = source._build_dashboard_url("dashboard_123", "project_456")

        assert (
            url
            == "https://demo.microstrategy.com/MicroStrategyLibrary/app/project_456/dashboard_123"
        )

    def test_report_url_construction(self):
        """Test that report URLs are correctly constructed."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com/MicroStrategyLibrary",
                "use_anonymous": True,
            },
        }
        config = MicroStrategyConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        source = MicroStrategySource(config, ctx)
        url = source._build_report_url("report_789", "project_456")

        assert (
            url
            == "https://demo.microstrategy.com/MicroStrategyLibrary/app/project_456/report_789"
        )

    def test_url_construction_handles_trailing_slashes(self):
        """Test that trailing slashes in base_url don't create malformed URLs."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com/MicroStrategyLibrary///",
                "use_anonymous": True,
            },
        }
        config = MicroStrategyConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        source = MicroStrategySource(config, ctx)
        url = source._build_dashboard_url("dashboard_123", "project_456")
        assert url is not None

        # Should not have double slashes
        assert "//" not in url.replace("https://", "")  # type: ignore
        assert url.endswith("/project_456/dashboard_123")  # type: ignore


class TestAuditStampParsing:
    """Test timestamp parsing and audit stamp creation."""

    def test_audit_stamps_parse_iso_timestamps(self):
        """Test that ISO timestamps are correctly parsed to audit stamps."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
        }
        config = MicroStrategyConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        source = MicroStrategySource(config, ctx)
        audit_stamps = source._build_audit_stamps(
            created_time="2024-01-15T10:30:00Z",
            modified_time="2024-03-12T14:45:00Z",
            owner_info="john.doe",
        )

        # Verify timestamps were parsed (milliseconds since epoch)
        assert audit_stamps.created is not None
        assert audit_stamps.created.time > 0
        assert audit_stamps.lastModified is not None
        assert audit_stamps.lastModified.time > 0

        # Verify actor URN
        assert "john.doe" in audit_stamps.created.actor

    def test_audit_stamps_handle_missing_timestamps(self):
        """Test that missing timestamps don't crash audit stamp creation."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
        }
        config = MicroStrategyConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        source = MicroStrategySource(config, ctx)
        audit_stamps = source._build_audit_stamps(
            created_time=None, modified_time=None, owner_info=None
        )

        # Should return empty audit stamps object without crashing
        assert audit_stamps is not None

    def test_audit_stamps_handle_invalid_timestamps(self, caplog):
        """Test that invalid timestamps are handled gracefully with logging."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
        }
        config = MicroStrategyConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        source = MicroStrategySource(config, ctx)

        with caplog.at_level(logging.DEBUG):
            audit_stamps = source._build_audit_stamps(
                created_time="not-a-timestamp",
                modified_time="also-invalid",
                owner_info="john.doe",
            )

            # Should return audit stamps without crashing
            assert audit_stamps is not None

            # Should log debug message about parsing failure
            assert any(
                "Failed to parse timestamps" in record.message
                for record in caplog.records
            )


class TestErrorHandling:
    """Test error handling and recovery."""

    def test_project_extraction_failure_continues_to_next_project(self):
        """Test that failure to extract one project doesn't stop ingestion."""
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
        }
        config = MicroStrategyConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "project_1", "name": "Project 1", "status": 0},
                {"id": "project_2", "name": "Project 2", "status": 0},
            ]

            def search_objects_with_error(
                project_id: str, object_type: int
            ) -> List[Dict[str, Any]]:
                if project_id == "project_1" and object_type == 55:  # Dashboards
                    raise Exception("Permission denied")
                if project_id == "project_2" and object_type == 55:
                    return [
                        {"id": "dashboard_1", "name": "Dashboard 1", "description": ""}
                    ]
                return []

            mock_client.search_objects.side_effect = search_objects_with_error
            mock_client.get_folders.return_value = []

            source = MicroStrategySource(config, ctx)
            workunits = list(source.get_workunits())

            # Should still emit containers for both projects
            # Count unique container URNs (each container generates multiple aspects)
            container_urns = set(
                wu.metadata.entityUrn
                for wu in workunits
                if hasattr(wu.metadata, "entityUrn")
                and "container" in str(wu.metadata.entityUrn)
            )
            assert len(container_urns) == 2

            # Should emit dashboard from project_2
            dashboard_info_workunits = [
                wu
                for wu in workunits
                if hasattr(wu.metadata, "aspect")
                and wu.metadata.aspect.__class__.__name__ == "DashboardInfoClass"
            ]
            assert len(dashboard_info_workunits) == 1


class TestLoadedProjectFiltering:
    """Projects with status != 0 are skipped unless include_unloaded_projects is set."""

    def test_skips_unloaded_projects_by_default(self):
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
        }
        config = MicroStrategyConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "loaded", "name": "Loaded", "status": 0},
                {"id": "idle", "name": "Idle", "status": 2},
            ]
            mock_client.get_folders.return_value = []
            mock_client.search_objects.side_effect = create_search_objects_mock()

            source = MicroStrategySource(config, ctx)
            list(source.get_workunits())

            mock_client.get_folders.assert_called_once_with("loaded")

    def test_include_unloaded_projects_processes_all_matching(self):
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
            "include_unloaded_projects": True,
        }
        config = MicroStrategyConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "loaded", "name": "Loaded", "status": 0},
                {"id": "idle", "name": "Idle", "status": 2},
            ]
            mock_client.get_folders.return_value = []
            mock_client.search_objects.side_effect = create_search_objects_mock()

            source = MicroStrategySource(config, ctx)
            list(source.get_workunits())

            assert mock_client.get_folders.call_count == 2


class TestMicroStrategyClientErrors:
    def test_request_raises_project_unavailable_on_iserver_body(self):
        cfg = MicroStrategyConnectionConfig(
            base_url="https://mstr.example.com",
            use_anonymous=True,
        )
        client = MicroStrategyClient(cfg)
        client.auth_token = "tok"
        client.token_created_at = datetime.now()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b"{}"
        mock_resp.json.return_value = {
            "iServerCode": ISERVER_PROJECT_UNAVAILABLE,
            "message": "Project not loaded",
            "ticketId": "abc",
        }

        with (
            patch.object(client.session, "request", return_value=mock_resp),
            pytest.raises(MicroStrategyProjectUnavailableError) as excinfo,
        ):
            client._request("GET", "/api/projects")

        assert excinfo.value.i_server_code == ISERVER_PROJECT_UNAVAILABLE
        assert excinfo.value.ticket_id == "abc"

    def test_get_dashboard_definition_empty_on_classcast_500(self):
        cfg = MicroStrategyConnectionConfig(
            base_url="https://mstr.example.com",
            use_anonymous=True,
        )
        client = MicroStrategyClient(cfg)
        client.auth_token = "tok"
        client.token_created_at = datetime.now()

        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.content = b"{}"
        mock_resp.json.return_value = {"message": "Cannot be cast to DossierBean"}

        with patch.object(client.session, "request", return_value=mock_resp):
            assert client.get_dashboard_definition("d1", "p1") == {}


class TestWarehouseLineageEmission:
    def test_model_cube_physical_tables_emit_upstream_lineage(self):
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
            "include_lineage": True,
            "include_warehouse_lineage": True,
            "warehouse_lineage_platform": "snowflake",
            "warehouse_lineage_database": "db",
            "warehouse_lineage_schema": "public",
        }
        config = MicroStrategyConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "project_1", "name": "P1", "status": 0}
            ]
            mock_client.search_objects.side_effect = create_search_objects_mock(
                cubes={
                    "project_1": [
                        {"id": "cube_1", "name": "Cube", "description": ""},
                    ]
                }
            )
            mock_client.get_folders.return_value = []
            mock_client.get_cube_sql_view.return_value = (
                'SELECT a FROM "analytics"."fact_sales" x'
            )

            source = MicroStrategySource(config, ctx)
            workunits = list(source.get_workunits())

            upstream_wus = [
                wu
                for wu in workunits
                if hasattr(wu.metadata, "aspect")
                and wu.metadata.aspect.__class__.__name__ == "UpstreamLineageClass"
            ]
            assert len(upstream_wus) == 1
            lineage = upstream_wus[0].metadata.aspect  # type: ignore[union-attr]
            assert len(lineage.upstreams) == 1  # type: ignore[union-attr]
            assert "snowflake" in lineage.upstreams[0].dataset  # type: ignore[union-attr]


class TestApiCallReduction:
    def test_cube_search_once_per_project(self) -> None:
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
        }
        config = MicroStrategyConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "project_1", "name": "P1", "status": 0},
            ]
            mock_client.get_folders.return_value = []
            mock_client.search_objects.side_effect = create_search_objects_mock(
                cubes={
                    "project_1": [
                        {"id": "cube_1", "name": "Cube", "description": ""},
                    ],
                },
            )

            source = MicroStrategySource(config, ctx)
            list(source.get_workunits())

            cube_calls = [
                c
                for c in mock_client.search_objects.call_args_list
                if (len(c.args) > 1 and c.args[1] == 776)
                or c.kwargs.get("object_type") == 776
            ]
            assert len(cube_calls) == 1

    def test_include_dashboards_false_skips_type_55_search(self) -> None:
        config_dict = {
            "connection": {
                "base_url": "https://demo.microstrategy.com",
                "use_anonymous": True,
            },
            "include_dashboards": False,
        }
        config = MicroStrategyConfig.parse_obj(config_dict)
        ctx = PipelineContext(run_id="test-run")

        with patch(
            "datahub.ingestion.source.microstrategy.source.MicroStrategyClient"
        ) as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_datasets.return_value = []
            mock_client_class.return_value = mock_client
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)

            mock_client.get_projects.return_value = [
                {"id": "project_1", "name": "P1", "status": 0},
            ]
            mock_client.get_folders.return_value = []
            mock_client.search_objects.side_effect = create_search_objects_mock(
                cubes={
                    "project_1": [
                        {"id": "cube_1", "name": "Cube", "description": ""},
                    ],
                },
            )

            source = MicroStrategySource(config, ctx)
            list(source.get_workunits())

            dashboard_calls = [
                c
                for c in mock_client.search_objects.call_args_list
                if (len(c.args) > 1 and c.args[1] == 55)
                or c.kwargs.get("object_type") == 55
            ]
            assert dashboard_calls == []
