"""Tests for GitHubDocsSourceConfig."""

from typing import Any

import pytest

from datahub.ingestion.source.github_docs.github_docs_config import (
    GitHubDocsSourceConfig,
)


def _make_config(**overrides: Any) -> GitHubDocsSourceConfig:
    defaults: dict[str, Any] = {
        "repo": "my-org/my-repo",
        "token": "ghp_test123",
    }
    defaults.update(overrides)
    return GitHubDocsSourceConfig.parse_obj(defaults)


class TestRepoValidation:
    def test_owner_repo_shorthand(self) -> None:
        config = _make_config(repo="owner/repo")
        assert config.repo == "owner/repo"

    def test_github_url(self) -> None:
        config = _make_config(repo="https://github.com/owner/repo")
        assert config.repo == "owner/repo"

    def test_github_url_with_git_suffix(self) -> None:
        config = _make_config(repo="https://github.com/owner/repo.git")
        assert config.repo == "owner/repo"

    def test_github_url_with_trailing_slash(self) -> None:
        config = _make_config(repo="https://github.com/owner/repo/")
        assert config.repo == "owner/repo"

    def test_invalid_url_raises(self) -> None:
        with pytest.raises(Exception, match="Could not parse GitHub URL"):
            _make_config(repo="https://github.com/just-owner")

    def test_bare_name_raises(self) -> None:
        with pytest.raises(Exception, match="Invalid repository identifier"):
            _make_config(repo="just-a-name")

    def test_empty_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid repository identifier"):
            _make_config(repo="")


class TestPathPrefixNormalization:
    def test_strips_slashes(self) -> None:
        config = _make_config(path_prefix="/docs/")
        assert config.path_prefix == "docs"

    def test_root_slash_becomes_empty(self) -> None:
        config = _make_config(path_prefix="/")
        assert config.path_prefix == ""

    def test_empty_stays_empty(self) -> None:
        config = _make_config(path_prefix="")
        assert config.path_prefix == ""

    def test_no_change_needed(self) -> None:
        config = _make_config(path_prefix="docs/guides")
        assert config.path_prefix == "docs/guides"


class TestExtensionNormalization:
    def test_dot_prepended(self) -> None:
        config = _make_config(file_extensions=["md", "txt"])
        assert config.file_extensions == [".md", ".txt"]

    def test_already_dotted(self) -> None:
        config = _make_config(file_extensions=[".md", ".rst"])
        assert config.file_extensions == [".md", ".rst"]

    def test_defaults(self) -> None:
        config = _make_config()
        assert config.file_extensions == [".md", ".txt"]


class TestPathFilterNormalization:
    def test_allow_strips_slashes(self) -> None:
        config = _make_config(paths={"allow": ["/docs/", "guides/"]})
        assert config.paths.allow == ["docs", "guides"]

    def test_deny_strips_slashes(self) -> None:
        config = _make_config(paths={"deny": ["/internal/"]})
        assert config.paths.deny == ["internal"]

    def test_empty_strings_filtered(self) -> None:
        config = _make_config(paths={"allow": ["docs", "  ", ""]})
        assert config.paths.allow == ["docs"]


class TestDefaults:
    def test_branch_default(self) -> None:
        config = _make_config()
        assert config.branch == "main"

    def test_import_as_native_default(self) -> None:
        config = _make_config()
        assert config.import_as_native is True

    def test_show_in_global_context_default(self) -> None:
        config = _make_config()
        assert config.show_in_global_context is True

    def test_preserve_hierarchy_default(self) -> None:
        config = _make_config()
        assert config.preserve_hierarchy is True

    def test_max_documents_default(self) -> None:
        config = _make_config()
        assert config.max_documents == 10000

    def test_max_file_size_default(self) -> None:
        config = _make_config()
        assert config.max_file_size_bytes == 1_000_000
