"""Integration tests for the looker-v2 source."""

from pathlib import Path
from unittest import mock

import pytest
from freezegun import freeze_time
from looker_sdk.sdk.api40.models import (
    Dashboard,
    DashboardBase,
    DashboardElement,
    FolderBase,
    LookmlModel,
    LookmlModelExplore,
    LookmlModelExploreField,
    LookmlModelExploreFieldset,
    Query,
)

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

FROZEN_TIME = "2020-04-14 07:00:00"

GOLDEN_DIR = Path(__file__).parent / "golden" / "looker_v2"


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_core_ingest(pytestconfig: pytest.Config, tmp_path: Path) -> None:
    mocked_client = mock.MagicMock()

    with mock.patch("looker_sdk.init40") as mock_sdk:
        mock_sdk.return_value = mocked_client

        # Mock folders
        mocked_client.all_folders.return_value = [
            FolderBase(
                id="1",
                name="Shared",
                parent_id=None,
                is_personal=False,
                is_personal_descendant=False,
            ),
        ]

        # Mock models with one explore
        mocked_client.all_lookml_models.return_value = [
            LookmlModel(
                name="lkml_samples",
                project_name="lkml_samples",
                explores=[
                    LookmlModelExplore(
                        name="order_items",
                    )
                ],
            )
        ]

        # Mock explore detail
        mocked_client.lookml_model_explore.return_value = LookmlModelExplore(
            id="lkml_samples::order_items",
            name="order_items",
            label="Order Items",
            description="Order items explore",
            view_name="order_items",
            project_name="lkml_samples",
            model_name="lkml_samples",
            fields=LookmlModelExploreFieldset(
                dimensions=[
                    LookmlModelExploreField(
                        name="order_items.id",
                        type="number",
                        description="Order item ID",
                        label_short="ID",
                        view="order_items",
                    )
                ],
                measures=[
                    LookmlModelExploreField(
                        name="order_items.count",
                        type="count",
                        description="Count of order items",
                        label_short="Count",
                        view="order_items",
                    )
                ],
            ),
            source_file="order_items.explore.lkml",
        )

        # Mock dashboards list (minimal)
        mocked_client.all_dashboards.return_value = [
            DashboardBase(
                id="1",
                title="Sales Dashboard",
                folder=FolderBase(id="1", name="Shared"),
            )
        ]

        # Mock full dashboard with elements
        mocked_client.dashboard.return_value = Dashboard(
            id="1",
            title="Sales Dashboard",
            description="Sales metrics dashboard",
            folder=FolderBase(id="1", name="Shared"),
            dashboard_elements=[
                DashboardElement(
                    id="101",
                    title="Order Count",
                    type="vis",
                    query=Query(
                        model="lkml_samples",
                        view="order_items",
                        fields=["order_items.count"],
                        dynamic_fields=None,
                    ),
                    look_id=None,
                    look=None,
                    merge_result_id=None,
                )
            ],
        )

        # Mock empty looks, users
        mocked_client.all_looks.return_value = []
        mocked_client.all_users.return_value = []

        # Mock PDT graph (empty)
        pdt_graph_mock = mock.MagicMock()
        pdt_graph_mock.graph_text = None
        mocked_client.graph_derived_tables_for_model.return_value = pdt_graph_mock

        # Mock connections (empty — no lineage resolution needed)
        mocked_client.all_connections.return_value = []

        output_file = tmp_path / "looker_v2_mces.json"

        pipeline = Pipeline.create(
            {
                "run_id": "looker-v2-test",
                "source": {
                    "type": "looker-v2",
                    "config": {
                        "base_url": "https://looker.company.com",
                        "client_id": "foo",
                        "client_secret": "bar",
                        "extract_looks": False,
                        "extract_usage_history": False,
                        "project_name": "lkml_samples",
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {
                        "filename": str(output_file),
                    },
                },
            }
        )
        pipeline.run()
        pipeline.raise_from_status()

        mce_helpers.check_golden_file(
            pytestconfig,
            output_path=output_file,
            golden_path=GOLDEN_DIR / "core_ingest.json",
        )
