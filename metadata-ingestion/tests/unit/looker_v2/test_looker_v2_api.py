"""Unit tests for Looker V2 API features.

Covers:
- Bulk folder pre-fetch and ancestor walk
- View discovery categorization
- ManifestParser constant extraction
- Config validation (extract_looks + stateful ingestion)
"""

from typing import Any, Optional
from unittest.mock import MagicMock

import pytest
from looker_sdk.sdk.api40.models import FolderBase

from datahub.ingestion.source.looker_v2.looker_v2_context import LookerV2Context
from datahub.ingestion.source.looker_v2.looker_v2_folder_processor import (
    LookerFolderProcessor,
)
from datahub.ingestion.source.looker_v2.lookml_view_discovery import (
    ViewDiscovery,
    extract_explore_views_from_api,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_folder(
    folder_id: str,
    name: str,
    parent_id: Optional[str] = None,
    is_personal: bool = False,
) -> FolderBase:
    f = FolderBase(name=name)
    f.id = folder_id
    f.parent_id = parent_id
    f.is_personal = is_personal
    f.is_personal_descendant = False
    return f


def make_explore(name: str, view_name: Optional[str] = None, joins: Any = None) -> Any:
    e = MagicMock()
    e.name = name
    e.view_name = view_name
    e.joins = joins or []
    return e


# ---------------------------------------------------------------------------
# Folder registry and ancestor walk
# ---------------------------------------------------------------------------


class TestFolderProcessorAncestorWalk:
    """Tests for LookerFolderProcessor.get_folder_ancestors."""

    def _make_processor(
        self, folders: list, skip_personal: bool = False
    ) -> LookerFolderProcessor:
        config = MagicMock()
        config.skip_personal_folders = skip_personal
        ctx = LookerV2Context(
            config=config,
            looker_api=MagicMock(),
            reporter=MagicMock(),
            pipeline_ctx=MagicMock(),
            platform="looker",
            folder_registry={f.id: f for f in folders if f.id},
        )
        return LookerFolderProcessor(ctx)

    def test_no_ancestors_for_root(self):
        root = make_folder("1", "Root")
        proc = self._make_processor([root])
        assert proc.get_folder_ancestors("1") == []

    def test_deep_ancestor_chain(self):
        folders = [
            make_folder("1", "Root"),
            make_folder("2", "Level1", parent_id="1"),
            make_folder("3", "Level2", parent_id="2"),
            make_folder("4", "Level3", parent_id="3"),
        ]
        proc = self._make_processor(folders)
        ancestors = proc.get_folder_ancestors("4")
        assert [a.id for a in ancestors] == ["1", "2", "3"]

    def test_cycle_protection(self):
        a = make_folder("1", "A", parent_id="2")
        b = make_folder("2", "B", parent_id="1")
        proc = self._make_processor([a, b])
        result = proc.get_folder_ancestors("1")
        assert isinstance(result, list)

    def test_personal_folder_skipped(self):
        personal = make_folder("p1", "My Folder", is_personal=True)
        proc = self._make_processor([personal], skip_personal=True)
        assert proc.should_skip_personal_folder(personal)

    def test_personal_folder_not_skipped_when_flag_off(self):
        personal = make_folder("p1", "My Folder", is_personal=True)
        proc = self._make_processor([personal], skip_personal=False)
        assert not proc.should_skip_personal_folder(personal)


# ---------------------------------------------------------------------------
# extract_explore_views_from_api
# ---------------------------------------------------------------------------


class TestExtractExploreViews:
    def test_basic_view_name(self):
        explore = make_explore("orders_explore", view_name="orders")
        result = extract_explore_views_from_api([("proj", "model", explore)])
        assert "orders" in result

    def test_explore_name_fallback(self):
        explore = make_explore("orders_explore", view_name=None)
        result = extract_explore_views_from_api([("proj", "model", explore)])
        assert "orders_explore" in result

    def test_joined_views_included(self):
        join1 = MagicMock()
        join1.from_ = "customers"
        join1.name = "customers"

        join2 = MagicMock()
        join2.from_ = None
        join2.name = "products"

        explore = make_explore("orders", view_name="orders", joins=[join1, join2])
        result = extract_explore_views_from_api([("proj", "model", explore)])
        assert "orders" in result
        assert "customers" in result
        assert "products" in result

    def test_empty_explores(self):
        result = extract_explore_views_from_api([])
        assert len(result) == 0

    def test_multiple_explores(self):
        e1 = make_explore("orders", view_name="orders")
        e2 = make_explore("users", view_name="users")
        result = extract_explore_views_from_api([("p", "m", e1), ("p", "m", e2)])
        assert "orders" in result
        assert "users" in result


# ---------------------------------------------------------------------------
# ViewDiscovery categorization (uses temp files)
# ---------------------------------------------------------------------------


class TestViewDiscoveryCategorization:
    def test_reachable_view_categorized(self, tmp_path):
        # Create a minimal project structure
        (tmp_path / "orders.view.lkml").write_text(
            "view: orders { sql_table_name: orders ;; }"
        )
        (tmp_path / "model.model.lkml").write_text(
            'include: "orders.view.lkml"\nexplore: orders {}'
        )

        discovery = ViewDiscovery(
            base_folder=str(tmp_path),
            project_name="test_project",
        )
        result = discovery.discover(explore_view_names=frozenset(["orders"]))

        assert "orders" in result.reachable_views
        assert "orders" not in result.unreachable_views

    def test_unreachable_view_categorized(self, tmp_path):
        # "extra" is included by the model but not referenced by any explore → unreachable
        (tmp_path / "orders.view.lkml").write_text(
            "view: orders { sql_table_name: orders ;; }"
        )
        (tmp_path / "extra.view.lkml").write_text(
            "view: extra { sql_table_name: extra ;; }"
        )
        (tmp_path / "model.model.lkml").write_text(
            'include: "orders.view.lkml"\ninclude: "extra.view.lkml"\nexplore: orders {}'
        )

        discovery = ViewDiscovery(
            base_folder=str(tmp_path),
            project_name="test_project",
        )
        result = discovery.discover(explore_view_names=frozenset(["orders"]))

        assert "extra" in result.unreachable_views
        assert "extra" not in result.reachable_views

    def test_orphaned_file_detected(self, tmp_path):
        (tmp_path / "orders.view.lkml").write_text(
            "view: orders { sql_table_name: orders ;; }"
        )
        (tmp_path / "orphan.view.lkml").write_text(
            "view: orphan { sql_table_name: orphan ;; }"
        )
        (tmp_path / "model.model.lkml").write_text(
            'include: "orders.view.lkml"\nexplore: orders {}'
        )

        discovery = ViewDiscovery(
            base_folder=str(tmp_path),
            project_name="test_project",
        )
        result = discovery.discover(explore_view_names=frozenset(["orders"]))

        orphan_file = str(tmp_path / "orphan.view.lkml")
        assert orphan_file in result.orphaned_files


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestExtractLooksRequiresStatefulIngestion:
    """extract_looks=True must be paired with stateful_ingestion.enabled=True."""

    BASE_CONFIG = {
        "base_url": "https://looker.example.com",
        "client_id": "id",
        "client_secret": "secret",
        "base_folder": "/tmp",
    }

    def test_extract_looks_without_stateful_raises(self):
        from pydantic import ValidationError

        from datahub.ingestion.source.looker_v2.looker_v2_config import LookerV2Config

        with pytest.raises(ValidationError, match="stateful_ingestion.enabled"):
            LookerV2Config(**{**self.BASE_CONFIG, "extract_looks": True})

    def test_extract_looks_with_stateful_disabled_raises(self):
        from pydantic import ValidationError

        from datahub.ingestion.source.looker_v2.looker_v2_config import LookerV2Config

        with pytest.raises(ValidationError, match="stateful_ingestion.enabled"):
            LookerV2Config(
                **{
                    **self.BASE_CONFIG,
                    "extract_looks": True,
                    "stateful_ingestion": {"enabled": False},
                }
            )

    def test_extract_looks_with_stateful_enabled_passes(self):
        from datahub.ingestion.source.looker_v2.looker_v2_config import LookerV2Config

        config = LookerV2Config(
            **{
                **self.BASE_CONFIG,
                "extract_looks": True,
                "stateful_ingestion": {"enabled": True},
            }
        )
        assert config.extract_looks is True

    def test_extract_looks_false_no_stateful_passes(self):
        from datahub.ingestion.source.looker_v2.looker_v2_config import LookerV2Config

        config = LookerV2Config(**{**self.BASE_CONFIG, "extract_looks": False})
        assert config.extract_looks is False
