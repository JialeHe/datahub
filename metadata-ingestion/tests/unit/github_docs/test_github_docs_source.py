"""Tests for GitHubDocsSource."""

from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

from datahub.ingestion.source.github_docs.github_docs_source import GitHubDocsSource

# -- Helper to build a source without hitting the network --


def _make_source(**config_overrides: Any) -> GitHubDocsSource:
    config_dict: Dict[str, Any] = {
        "repo": "my-org/my-repo",
        "token": "ghp_test123",
    }
    config_dict.update(config_overrides)

    ctx = MagicMock()
    ctx.pipeline_name = "test-pipeline"
    ctx.run_id = "test-run"
    ctx.graph = None

    with patch.object(GitHubDocsSource, "__init__", lambda self, *a, **kw: None):
        source = GitHubDocsSource.__new__(GitHubDocsSource)

    from datahub.ingestion.source.github_docs.github_docs_config import (
        GitHubDocsSourceConfig,
    )
    from datahub.ingestion.source.github_docs.github_docs_report import (
        GitHubDocsSourceReport,
    )

    source.config = GitHubDocsSourceConfig.parse_obj(config_dict)
    source.report = GitHubDocsSourceReport()
    source.session = MagicMock()
    return source


# -- Title generation --


class TestTitleFromPath:
    def test_simple_file(self) -> None:
        assert (
            GitHubDocsSource._title_from_path("docs/getting-started.md")
            == "Getting Started"
        )

    def test_underscores(self) -> None:
        assert GitHubDocsSource._title_from_path("my_guide.txt") == "My Guide"

    def test_folder(self) -> None:
        assert (
            GitHubDocsSource._title_from_path("docs/api-reference") == "Api Reference"
        )

    def test_nested_path(self) -> None:
        assert GitHubDocsSource._title_from_path("a/b/c/deep-file.md") == "Deep File"

    def test_no_extension(self) -> None:
        assert GitHubDocsSource._title_from_path("README") == "Readme"


# -- ID generation --


class TestSanitizeId:
    def test_basic(self) -> None:
        assert GitHubDocsSource._sanitize_id("hello-world") == "hello-world"

    def test_unsafe_chars(self) -> None:
        assert GitHubDocsSource._sanitize_id("a/b/c") == "a-b-c"

    def test_collapses_dashes(self) -> None:
        assert GitHubDocsSource._sanitize_id("a---b") == "a-b"

    def test_strips_leading_trailing_dashes(self) -> None:
        assert GitHubDocsSource._sanitize_id("--hello--") == "hello"

    def test_lowercases(self) -> None:
        assert GitHubDocsSource._sanitize_id("Hello.World") == "hello.world"

    def test_truncates_at_200(self) -> None:
        long_id = "a" * 300
        assert len(GitHubDocsSource._sanitize_id(long_id)) == 200


class TestDocIdGeneration:
    def test_file_doc_id(self) -> None:
        source = _make_source()
        doc_id = source._make_file_doc_id("docs/guide.md")
        assert doc_id == "github.my-org.my-repo.docs.guide"

    def test_folder_doc_id(self) -> None:
        source = _make_source()
        doc_id = source._make_folder_doc_id("docs/guides")
        assert doc_id == "github.my-org.my-repo.docs.guides._dir"

    def test_file_source_id(self) -> None:
        source = _make_source()
        assert source._make_file_source_id("docs/guide.md") == "file:docs/guide.md"

    def test_folder_source_id(self) -> None:
        source = _make_source()
        assert source._make_folder_source_id("docs") == "dir:docs"


# -- File filtering --


class TestFilterFiles:
    def _tree_blob(self, path: str, size: int = 100) -> Dict[str, Any]:
        return {"type": "blob", "path": path, "size": size}

    def test_filters_by_extension(self) -> None:
        source = _make_source(file_extensions=[".md"])
        tree = [
            self._tree_blob("docs/guide.md"),
            self._tree_blob("docs/data.json"),
            self._tree_blob("docs/notes.txt"),
        ]
        result = source._filter_files(tree)
        assert [path for path, _ in result] == ["docs/guide.md"]

    def test_filters_by_path_prefix(self) -> None:
        source = _make_source(path_prefix="docs")
        tree = [
            self._tree_blob("docs/guide.md"),
            self._tree_blob("src/main.py"),
        ]
        result = source._filter_files(tree)
        assert [path for path, _ in result] == ["docs/guide.md"]

    def test_skips_non_blobs(self) -> None:
        source = _make_source()
        tree = [
            {"type": "tree", "path": "docs"},
            self._tree_blob("docs/guide.md"),
        ]
        result = source._filter_files(tree)
        assert len(result) == 1

    def test_path_allow_filter(self) -> None:
        source = _make_source(paths={"allow": ["docs/public"]})
        tree = [
            self._tree_blob("docs/public/guide.md"),
            self._tree_blob("docs/private/secret.md"),
        ]
        result = source._filter_files(tree)
        assert [path for path, _ in result] == ["docs/public/guide.md"]

    def test_path_deny_filter(self) -> None:
        source = _make_source(paths={"deny": ["docs/internal"]})
        tree = [
            self._tree_blob("docs/guide.md"),
            self._tree_blob("docs/internal/secret.md"),
        ]
        result = source._filter_files(tree)
        assert [path for path, _ in result] == ["docs/guide.md"]

    def test_reports_discovered_count(self) -> None:
        source = _make_source()
        tree = [self._tree_blob("a.md"), self._tree_blob("b.md")]
        source._filter_files(tree)
        assert source.report.files_discovered == 2


# -- Folder hierarchy --


class TestCollectFolders:
    def test_collects_intermediate_dirs(self) -> None:
        source = _make_source(path_prefix="")
        files: List[Tuple[str, Optional[int]]] = [
            ("docs/guides/setup.md", 100),
            ("docs/api/reference.md", 200),
        ]
        folders = source._collect_folders(files)
        assert "docs" in folders
        assert "docs/guides" in folders
        assert "docs/api" in folders

    def test_sorted_shallowest_first(self) -> None:
        source = _make_source(path_prefix="")
        files: List[Tuple[str, Optional[int]]] = [
            ("a/b/c/deep.md", 100),
        ]
        folders = source._collect_folders(files)
        assert folders == ["a", "a/b", "a/b/c"]

    def test_with_path_prefix(self) -> None:
        source = _make_source(path_prefix="docs")
        files: List[Tuple[str, Optional[int]]] = [
            ("docs/guides/setup.md", 100),
        ]
        folders = source._collect_folders(files)
        assert folders == ["docs/guides"]

    def test_no_folders_for_root_files(self) -> None:
        source = _make_source(path_prefix="")
        files: List[Tuple[str, Optional[int]]] = [
            ("README.md", 100),
        ]
        folders = source._collect_folders(files)
        assert folders == []


# -- Parent resolution --


class TestResolveParentDocId:
    def test_resolves_parent_folder(self) -> None:
        source = _make_source(path_prefix="")
        lookup = {"dir:docs": "github.my-org.my-repo.docs._dir"}
        result = source._resolve_parent_doc_id("docs/guide.md", lookup)
        assert result == "github.my-org.my-repo.docs._dir"

    def test_returns_none_for_root_file(self) -> None:
        source = _make_source(path_prefix="")
        result = source._resolve_parent_doc_id("guide.md", {})
        assert result is None

    def test_returns_none_when_hierarchy_disabled(self) -> None:
        source = _make_source(preserve_hierarchy=False)
        lookup = {"dir:docs": "some-id"}
        result = source._resolve_parent_doc_id("docs/guide.md", lookup)
        assert result is None

    def test_returns_none_when_parent_not_in_lookup(self) -> None:
        source = _make_source(path_prefix="")
        result = source._resolve_parent_doc_id("docs/guide.md", {})
        assert result is None


# -- GitHub URL generation --


class TestGitHubUrls:
    def test_file_url(self) -> None:
        source = _make_source(branch="main")
        url = source._github_url_for_file("docs/guide.md")
        assert url == "https://github.com/my-org/my-repo/blob/main/docs/guide.md"

    def test_dir_url(self) -> None:
        source = _make_source(branch="main")
        url = source._github_url_for_dir("docs/guides")
        assert url == "https://github.com/my-org/my-repo/tree/main/docs/guides"


# -- Document creation --


class TestCreateDocument:
    def test_native_mode(self) -> None:
        source = _make_source(import_as_native=True)
        doc = source._create_document(
            doc_id="test-doc",
            title="Test Doc",
            text="Hello world",
            file_path="test.md",
            parent_urn=None,
            custom_properties={"key": "val"},
        )
        assert doc.id == "test-doc"
        assert doc.title == "Test Doc"
        assert doc.text == "Hello world"
        assert doc.is_native

    def test_external_mode(self) -> None:
        source = _make_source(import_as_native=False, branch="main")
        doc = source._create_document(
            doc_id="test-doc",
            title="Test Doc",
            text="Hello world",
            file_path="docs/guide.md",
            parent_urn=None,
            custom_properties={"key": "val"},
        )
        assert doc.id == "test-doc"
        assert doc.is_external
        assert (
            doc.external_url
            == "https://github.com/my-org/my-repo/blob/main/docs/guide.md"
        )

    def test_external_folder_uses_tree_url(self) -> None:
        source = _make_source(import_as_native=False, branch="main")
        doc = source._create_document(
            doc_id="test-dir",
            title="Docs",
            text="",
            file_path="docs",
            parent_urn=None,
            custom_properties={},
            is_folder=True,
        )
        assert doc.external_url == "https://github.com/my-org/my-repo/tree/main/docs"


# -- Limit helpers --


class TestReachedLimit:
    def test_zero_means_unlimited(self) -> None:
        source = _make_source(max_documents=0)
        assert source._reached_limit(99999) is False

    def test_at_limit(self) -> None:
        source = _make_source(max_documents=10)
        assert source._reached_limit(10) is True

    def test_below_limit(self) -> None:
        source = _make_source(max_documents=10)
        assert source._reached_limit(5) is False


# -- Relativize --


class TestRelativize:
    def test_with_prefix(self) -> None:
        source = _make_source(path_prefix="docs")
        assert source._relativize("docs/guide.md") == "guide.md"

    def test_without_prefix(self) -> None:
        source = _make_source(path_prefix="")
        assert source._relativize("docs/guide.md") == "docs/guide.md"

    def test_prefix_no_slash(self) -> None:
        source = _make_source(path_prefix="docs")
        assert source._relativize("docs_extra/file.md") == "docs_extra/file.md"
