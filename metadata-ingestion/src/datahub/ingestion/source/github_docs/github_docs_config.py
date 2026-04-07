"""Configuration for the GitHub Docs ingestion source."""

import re
from typing import List, Optional

from pydantic import Field, SecretStr, field_validator, model_validator

from datahub.configuration.common import ConfigModel
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class PathFilterConfig(ConfigModel):
    """Configuration for filtering files by path."""

    allow: Optional[List[str]] = Field(
        default=None,
        description=(
            "List of path prefixes to include. "
            "Only files under these paths will be imported. "
            "Example: ['docs/', 'guides/getting-started/']"
        ),
    )

    deny: Optional[List[str]] = Field(
        default=None,
        description=(
            "List of path prefixes to exclude. Applied after allow filtering. "
            "Example: ['docs/internal/', 'docs/deprecated/']"
        ),
    )


class GitHubDocsSourceConfig(
    StatefulIngestionConfigBase[StatefulStaleMetadataRemovalConfig], ConfigModel
):
    """Configuration for the GitHub Docs ingestion source.

    Imports documents from a GitHub repository as native DataHub documents.
    Supports hierarchical folder structure, file extension filtering,
    and stateful ingestion with stale entity removal.
    """

    # GitHub connection
    repo: str = Field(
        description=(
            "GitHub repository identifier. "
            "Accepts 'owner/repo' shorthand or full URL "
            "'https://github.com/owner/repo'."
        ),
    )

    token: SecretStr = Field(
        description=(
            "GitHub Personal Access Token (PAT) or fine-grained token "
            "with read access to repository contents."
        ),
    )

    branch: str = Field(
        default="main",
        description="Branch to import from.",
    )

    # Path filtering
    path_prefix: str = Field(
        default="",
        description=(
            "Root path within the repository to import from. "
            "Only files under this path are considered. "
            "Example: 'docs/' to import only the docs directory."
        ),
    )

    paths: PathFilterConfig = Field(
        default_factory=PathFilterConfig,
        description="Additional path-based filtering (allow/deny lists).",
    )

    # File extension filtering
    file_extensions: List[str] = Field(
        default_factory=lambda: [".md", ".txt"],
        description=(
            "File extensions to include. Example: ['.md', '.txt', '.rst', '.html']"
        ),
    )

    # Import behavior
    import_as_native: bool = Field(
        default=True,
        description=(
            "When True, documents are imported as native DataHub documents — "
            "the full content is copied into DataHub and lives independently of GitHub. "
            "When False, documents are imported as external references — "
            "they link back to GitHub as the source of truth, with content indexed for search."
        ),
    )

    show_in_global_context: bool = Field(
        default=True,
        description=(
            "Whether imported documents appear in global search and sidebar. "
            "Set to False for AI-only context documents."
        ),
    )

    preserve_hierarchy: bool = Field(
        default=True,
        description=(
            "Whether to create folder documents preserving the GitHub directory structure "
            "as parent-child relationships. When False, all documents are imported flat."
        ),
    )

    # Limits
    max_file_size_bytes: int = Field(
        default=1_000_000,
        description="Maximum file size in bytes. Files larger than this are skipped.",
    )

    max_documents: int = Field(
        default=10000,
        ge=0,
        description=(
            "Maximum number of documents to import. Set to 0 to disable the limit."
        ),
    )

    @field_validator("repo")
    @classmethod
    def parse_repo(cls, v: str) -> str:
        """Normalize repo to 'owner/repo' format."""
        v = v.strip().rstrip("/")
        if v.startswith(("http://", "https://")):
            match = re.match(r"https?://github\.com/([^/]+/[^/]+?)(?:\.git)?$", v)
            if not match:
                raise ValueError(
                    f"Could not parse GitHub URL: {v}. "
                    "Expected format: https://github.com/owner/repo"
                )
            return match.group(1)
        if "/" in v and not v.startswith("/"):
            parts = v.split("/")
            if len(parts) == 2 and parts[0] and parts[1]:
                return v
        raise ValueError(
            f"Invalid repository identifier: {v}. "
            "Use 'owner/repo' or 'https://github.com/owner/repo'."
        )

    @field_validator("path_prefix")
    @classmethod
    def normalize_path_prefix(cls, v: str) -> str:
        """Strip leading/trailing slashes from path prefix."""
        if not v or v.strip() == "/":
            return ""
        return v.strip().strip("/")

    @field_validator("file_extensions")
    @classmethod
    def normalize_extensions(cls, v: List[str]) -> List[str]:
        """Ensure all extensions start with a dot."""
        return [ext if ext.startswith(".") else f".{ext}" for ext in v]

    @model_validator(mode="after")
    def validate_paths(self) -> "GitHubDocsSourceConfig":
        """Normalize allow/deny path lists."""
        if self.paths.allow is not None:
            self.paths.allow = [
                p.strip().strip("/") for p in self.paths.allow if p.strip()
            ]
        if self.paths.deny is not None:
            self.paths.deny = [
                p.strip().strip("/") for p in self.paths.deny if p.strip()
            ]
        return self
