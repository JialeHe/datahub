"""Unit tests for Looker V2 API features.

Covers:
- Bulk folder pre-fetch and ancestor walk
- PDT graph API default
- View discovery categorization
- ManifestParser constant extraction
"""

from typing import Any, Dict, Optional
from unittest.mock import MagicMock

from looker_sdk.sdk.api40.models import FolderBase

from datahub.ingestion.source.looker_v2.looker_v2_config import LookerV2Config
from datahub.ingestion.source.looker_v2.view_discovery import (
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


class TestFolderAncestorWalk:
    """Tests for _get_folder_ancestors using the pre-fetched registry."""

    def _build_registry(self, folders: list) -> Dict[str, FolderBase]:
        return {f.id: f for f in folders if f.id}

    def test_no_ancestors_for_root(self):
        root = make_folder("1", "Root")
        registry = self._build_registry([root])

        # Mock a minimal LookerV2Source with registry populated
        from datahub.ingestion.source.looker_v2.looker_v2_source import LookerV2Source

        src = object.__new__(LookerV2Source)
        src._folder_registry = registry  # type: ignore[attr-defined]

        ancestors = src._get_folder_ancestors("1")  # type: ignore[attr-defined]
        assert ancestors == []

    def test_single_level_ancestor(self):
        root = make_folder("1", "Root")
        child = make_folder("2", "Child", parent_id="1")
        registry = self._build_registry([root, child])

        from datahub.ingestion.source.looker_v2.looker_v2_source import LookerV2Source

        src = object.__new__(LookerV2Source)
        src._folder_registry = registry  # type: ignore[attr-defined]

        ancestors = src._get_folder_ancestors("2")  # type: ignore[attr-defined]
        assert len(ancestors) == 1
        assert ancestors[0].id == "1"

    def test_deep_ancestor_chain(self):
        folders = [
            make_folder("1", "Root"),
            make_folder("2", "Level1", parent_id="1"),
            make_folder("3", "Level2", parent_id="2"),
            make_folder("4", "Level3", parent_id="3"),
        ]
        registry = self._build_registry(folders)

        from datahub.ingestion.source.looker_v2.looker_v2_source import LookerV2Source

        src = object.__new__(LookerV2Source)
        src._folder_registry = registry  # type: ignore[attr-defined]

        ancestors = src._get_folder_ancestors("4")  # type: ignore[attr-defined]
        # Should return [Root, Level1, Level2] in order (top-down)
        assert [a.id for a in ancestors] == ["1", "2", "3"]

    def test_unknown_folder_returns_empty(self):
        # When registry is populated but folder not found, return empty immediately
        # (avoids falling back to API call in unit tests)
        known = make_folder("1", "Root")
        registry = self._build_registry([known])

        from datahub.ingestion.source.looker_v2.looker_v2_source import LookerV2Source

        src = object.__new__(LookerV2Source)
        src._folder_registry = registry  # type: ignore[attr-defined]

        # Folder "999" is not in the registry
        ancestors = src._get_folder_ancestors("999")  # type: ignore[attr-defined]
        assert ancestors == []

    def test_cycle_protection(self):
        # Two folders pointing at each other — should not infinite loop
        a = make_folder("1", "A", parent_id="2")
        b = make_folder("2", "B", parent_id="1")
        registry = self._build_registry([a, b])

        from datahub.ingestion.source.looker_v2.looker_v2_source import LookerV2Source

        src = object.__new__(LookerV2Source)
        src._folder_registry = registry  # type: ignore[attr-defined]

        ancestors = src._get_folder_ancestors("1")  # type: ignore[attr-defined]
        # Should terminate, not hang
        assert isinstance(ancestors, list)


# ---------------------------------------------------------------------------
# Personal folder detection
# ---------------------------------------------------------------------------


class TestPersonalFolderSkip:
    def _make_source_with_registry(self, folders: list, skip_personal: bool = True):
        from datahub.ingestion.source.looker_v2.looker_v2_source import LookerV2Source

        src = object.__new__(LookerV2Source)
        src._folder_registry = {f.id: f for f in folders if f.id}  # type: ignore[attr-defined]
        src.config = MagicMock()  # type: ignore[attr-defined]
        src.config.skip_personal_folders = skip_personal
        return src

    def test_personal_folder_skipped(self):
        personal = make_folder("p1", "My Folder", is_personal=True)
        src = self._make_source_with_registry([personal])
        assert src._should_skip_personal_folder(personal)  # type: ignore[attr-defined]

    def test_non_personal_folder_not_skipped(self):
        shared = make_folder("s1", "Shared")
        src = self._make_source_with_registry([shared])
        assert not src._should_skip_personal_folder(shared)  # type: ignore[attr-defined]

    def test_skip_personal_false_always_returns_false(self):
        personal = make_folder("p1", "My Folder", is_personal=True)
        src = self._make_source_with_registry([personal], skip_personal=False)
        assert not src._should_skip_personal_folder(personal)  # type: ignore[attr-defined]

    def test_child_of_personal_folder_skipped(self):
        personal = make_folder("p1", "Personal Root", is_personal=True)
        child = make_folder("c1", "Personal Child", parent_id="p1")
        child.is_personal_descendant = True
        src = self._make_source_with_registry([personal, child])
        assert src._should_skip_personal_folder(child)  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# PDT graph API default
# ---------------------------------------------------------------------------


class TestPDTGraphDefault:
    def test_use_pdt_graph_api_defaults_true(self):
        cfg = LookerV2Config.model_validate(
            {
                "client_id": "test_id",
                "client_secret": "test_secret",
                "base_url": "https://example.looker.com",
            }
        )
        assert cfg.use_pdt_graph_api is True


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
