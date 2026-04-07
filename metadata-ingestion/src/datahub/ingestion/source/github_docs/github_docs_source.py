"""
GitHub Docs source for DataHub ingestion.

Imports documents from a GitHub repository as native DataHub Document entities,
preserving the folder hierarchy as parent-child relationships. Uses the GitHub
REST API directly (no git clone, no disk I/O).
"""

import logging
import re
from base64 import b64decode
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests

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
    MetadataWorkUnitProcessor,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.github_docs.github_docs_config import (
    GitHubDocsSourceConfig,
)
from datahub.ingestion.source.github_docs.github_docs_report import (
    GitHubDocsSourceReport,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import DocumentStateClass
from datahub.sdk.document import Document

logger = logging.getLogger(__name__)

GITHUB_API_BASE = "https://api.github.com"


@platform_name("GitHub Docs")
@config_class(GitHubDocsSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
@capability(SourceCapability.DELETION_DETECTION, "Enabled by default")
class GitHubDocsSource(StatefulIngestionSourceBase, TestableSource):
    """Imports documents from a GitHub repository as native DataHub documents.

    Preserves folder hierarchy as parent-child document relationships.
    Supports stateful ingestion for automatic stale document removal.
    """

    platform = "github"

    config: GitHubDocsSourceConfig
    report: GitHubDocsSourceReport

    def __init__(self, config: GitHubDocsSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)  # type: ignore[arg-type]
        self.config = config
        self.report = GitHubDocsSourceReport()
        self.session = self._create_session()

        self.stale_entity_removal_handler = StaleEntityRemovalHandler.create(
            self, self.config, self.ctx
        )

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f"Bearer {self.config.token.get_secret_value()}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            }
        )
        return session

    @classmethod
    def create(
        cls, config_dict: Dict[str, Any], ctx: PipelineContext
    ) -> "GitHubDocsSource":
        config = GitHubDocsSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_report(self) -> GitHubDocsSourceReport:
        return self.report

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            self.stale_entity_removal_handler.workunit_processor,
        ]

    # -- Test connection --

    @staticmethod
    def test_connection(config_dict: Dict[str, Any]) -> TestConnectionReport:
        config = GitHubDocsSourceConfig.parse_obj(config_dict)
        try:
            session = requests.Session()
            session.headers.update(
                {
                    "Authorization": f"Bearer {config.token.get_secret_value()}",
                    "Accept": "application/vnd.github+json",
                }
            )
            resp = session.get(
                f"{GITHUB_API_BASE}/repos/{config.repo}",
                timeout=10,
            )
            if resp.status_code == 200:
                return TestConnectionReport(
                    basic_connectivity=CapabilityReport(capable=True)
                )
            elif resp.status_code == 404:
                return TestConnectionReport(
                    basic_connectivity=CapabilityReport(
                        capable=False,
                        failure_reason=f"Repository '{config.repo}' not found or not accessible",
                    )
                )
            elif resp.status_code == 401:
                return TestConnectionReport(
                    basic_connectivity=CapabilityReport(
                        capable=False,
                        failure_reason="Invalid GitHub token",
                    )
                )
            else:
                return TestConnectionReport(
                    basic_connectivity=CapabilityReport(
                        capable=False,
                        failure_reason=f"GitHub API returned HTTP {resp.status_code}",
                    )
                )
        except Exception as e:
            return TestConnectionReport(
                internal_failure=True,
                internal_failure_reason=f"Unexpected error: {e}",
            )

    # -- Core ingestion --

    def get_workunits_internal(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        # Fetch the file tree from GitHub
        tree = self._fetch_tree()
        if tree is None:
            self.report.report_failure(
                "Tree fetch failed",
                context=f"{self.config.repo}/{self.config.branch}",
            )
            return

        # Filter files matching our criteria
        matching_files = self._filter_files(tree)
        logger.info(
            "Found %d matching files in %s (branch: %s, path: %s)",
            len(matching_files),
            self.config.repo,
            self.config.branch,
            self.config.path_prefix or "/",
        )

        if not matching_files:
            return

        # Get the latest commit SHA for provenance
        commit_sha = self._get_commit_sha()

        # Collect intermediate directories for folder documents
        folder_paths: List[str] = []
        if self.config.preserve_hierarchy:
            folder_paths = self._collect_folders(matching_files)

        # Track folder sourceId -> document URN for parent resolution
        source_id_to_doc_id: Dict[str, str] = {}
        documents_emitted = 0

        # Emit folder documents first (shallowest first)
        for folder_path in folder_paths:
            if self._reached_limit(documents_emitted):
                break

            folder_doc_id = self._make_folder_doc_id(folder_path)
            folder_source_id = self._make_folder_source_id(folder_path)
            source_id_to_doc_id[folder_source_id] = folder_doc_id

            parent_doc_id = self._resolve_parent_doc_id(
                folder_path, source_id_to_doc_id
            )
            parent_urn = f"urn:li:document:{parent_doc_id}" if parent_doc_id else None

            folder_props = {
                "import_source": "github",
                "github_repo": self.config.repo,
                "github_branch": self.config.branch,
                "github_directory_path": folder_path,
                "is_folder_document": "true",
            }

            doc = self._create_document(
                doc_id=folder_doc_id,
                title=self._title_from_path(folder_path),
                text="",
                file_path=folder_path,
                parent_urn=parent_urn,
                custom_properties=folder_props,
                is_folder=True,
            )

            for wu in doc.as_workunits():
                yield wu

            self.report.report_folder_created()
            documents_emitted += 1

        # Emit file documents
        for file_path, file_size in matching_files:
            if self._reached_limit(documents_emitted):
                self.report.report_file_skipped(
                    file_path, "max_documents limit reached"
                )
                break

            try:
                content = self._fetch_file_content(file_path, file_size)
                if content is None:
                    continue

                file_doc_id = self._make_file_doc_id(file_path)
                file_source_id = self._make_file_source_id(file_path)
                source_id_to_doc_id[file_source_id] = file_doc_id

                parent_doc_id = self._resolve_parent_doc_id(
                    file_path, source_id_to_doc_id
                )
                parent_urn = (
                    f"urn:li:document:{parent_doc_id}" if parent_doc_id else None
                )

                file_props = {
                    "import_source": "github",
                    "github_repo": self.config.repo,
                    "github_branch": self.config.branch,
                    "github_file_path": file_path,
                    "github_commit_sha": commit_sha,
                }

                doc = self._create_document(
                    doc_id=file_doc_id,
                    title=self._title_from_path(file_path),
                    text=content,
                    file_path=file_path,
                    parent_urn=parent_urn,
                    custom_properties=file_props,
                )

                for wu in doc.as_workunits():
                    yield wu

                self.report.report_file_imported(file_path, len(content))
                documents_emitted += 1

            except Exception as e:
                self.report.report_file_failed(file_path, str(e))
                logger.warning("Failed to import %s: %s", file_path, e)

        logger.info(
            "Import complete: %d files imported, %d folders created, "
            "%d skipped, %d failed",
            self.report.files_imported,
            self.report.folders_created,
            self.report.files_skipped,
            self.report.files_failed,
        )

    # -- Document creation --

    def _create_document(
        self,
        *,
        doc_id: str,
        title: str,
        text: str,
        file_path: str,
        parent_urn: Optional[str],
        custom_properties: Dict[str, str],
        is_folder: bool = False,
    ) -> Document:
        """Create a Document entity, either native or external based on config."""
        if self.config.import_as_native:
            return Document.create_document(
                id=doc_id,
                title=title,
                text=text,
                status=DocumentStateClass.PUBLISHED,
                show_in_global_context=self.config.show_in_global_context,
                parent_document=parent_urn,
                custom_properties=custom_properties,
            )
        else:
            external_url = (
                self._github_url_for_dir(file_path)
                if is_folder
                else self._github_url_for_file(file_path)
            )
            return Document.create_external_document(
                id=doc_id,
                title=title,
                platform=self.platform,
                external_url=external_url,
                text=text or None,
                status=DocumentStateClass.PUBLISHED,
                show_in_global_context=self.config.show_in_global_context,
                parent_document=parent_urn,
                custom_properties=custom_properties,
            )

    def _github_url_for_file(self, file_path: str) -> str:
        """Build the GitHub web URL for a file."""
        return (
            f"https://github.com/{self.config.repo}"
            f"/blob/{self.config.branch}/{file_path}"
        )

    def _github_url_for_dir(self, dir_path: str) -> str:
        """Build the GitHub web URL for a directory."""
        return (
            f"https://github.com/{self.config.repo}"
            f"/tree/{self.config.branch}/{dir_path}"
        )

    # -- GitHub API helpers --

    def _fetch_tree(self) -> Optional[List[Dict[str, Any]]]:
        """Fetch the full recursive tree for the configured branch."""
        url = (
            f"{GITHUB_API_BASE}/repos/{self.config.repo}"
            f"/git/trees/{self.config.branch}?recursive=true"
        )
        resp = self.session.get(url, timeout=30)
        if resp.status_code != 200:
            logger.error(
                "Failed to fetch tree for %s/%s: HTTP %d",
                self.config.repo,
                self.config.branch,
                resp.status_code,
            )
            return None
        data = resp.json()
        return data.get("tree", [])

    def _fetch_file_content(
        self, file_path: str, file_size: Optional[int]
    ) -> Optional[str]:
        """Fetch and decode a single file's content from GitHub."""
        if file_size and file_size > self.config.max_file_size_bytes:
            self.report.report_file_skipped(
                file_path,
                f"Size {file_size} bytes exceeds limit {self.config.max_file_size_bytes}",
            )
            return None

        url = (
            f"{GITHUB_API_BASE}/repos/{self.config.repo}"
            f"/contents/{file_path}?ref={self.config.branch}"
        )
        resp = self.session.get(url, timeout=30)
        if resp.status_code != 200:
            self.report.report_file_failed(
                file_path, f"GitHub API returned HTTP {resp.status_code}"
            )
            return None

        data = resp.json()

        # Double-check size from the API response
        actual_size = data.get("size", 0)
        if actual_size > self.config.max_file_size_bytes:
            self.report.report_file_skipped(
                file_path,
                f"Size {actual_size} bytes exceeds limit {self.config.max_file_size_bytes}",
            )
            return None

        # Decode base64 content
        if data.get("encoding") == "base64" and data.get("content"):
            encoded = data["content"].replace("\n", "")
            return b64decode(encoded).decode("utf-8")

        # Fall back to download_url
        download_url = data.get("download_url")
        if download_url:
            dl_resp = self.session.get(download_url, timeout=30)
            if dl_resp.status_code == 200:
                return dl_resp.text

        self.report.report_file_failed(file_path, "Could not decode file content")
        return None

    def _get_commit_sha(self) -> str:
        """Get the latest commit SHA on the configured branch."""
        url = (
            f"{GITHUB_API_BASE}/repos/{self.config.repo}/branches/{self.config.branch}"
        )
        resp = self.session.get(url, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("commit", {}).get("sha", "unknown")
        return "unknown"

    # -- File filtering --

    def _filter_files(
        self, tree: List[Dict[str, Any]]
    ) -> List[Tuple[str, Optional[int]]]:
        """Filter tree entries to matching blob files."""
        ext_set = {ext.lower() for ext in self.config.file_extensions}
        prefix = self.config.path_prefix

        results: List[Tuple[str, Optional[int]]] = []
        for item in tree:
            if item.get("type") != "blob":
                continue

            path = item.get("path", "")

            # Path prefix filter
            if prefix and not path.startswith(prefix):
                continue

            # Extension filter
            dot = path.rfind(".")
            if dot < 0:
                continue
            ext = path[dot:].lower()
            if ext not in ext_set:
                continue

            # Allow/deny path filters
            if not self._path_allowed(path):
                continue

            self.report.report_file_discovered()
            size: Optional[int] = item.get("size")
            results.append((path, size))

        return results

    def _path_allowed(self, path: str) -> bool:
        """Check if a file path passes the allow/deny filters."""
        if self.config.paths.allow is not None:
            if not any(path.startswith(p) for p in self.config.paths.allow):
                return False
        if self.config.paths.deny is not None:
            if any(path.startswith(p) for p in self.config.paths.deny):
                return False
        return True

    # -- Folder hierarchy --

    def _collect_folders(self, files: List[Tuple[str, Optional[int]]]) -> List[str]:
        """Collect intermediate directory paths, sorted shallowest-first."""
        prefix = self.config.path_prefix
        dirs: Set[str] = set()

        for file_path, _ in files:
            relative = self._relativize(file_path)
            last_slash = relative.rfind("/")
            while last_slash > 0:
                rel_dir = relative[:last_slash]
                full_dir = f"{prefix}/{rel_dir}" if prefix else rel_dir
                dirs.add(full_dir)
                last_slash = rel_dir.rfind("/")

        return sorted(dirs, key=lambda d: d.count("/"))

    def _relativize(self, full_path: str) -> str:
        """Strip the path prefix to get a relative path."""
        prefix = self.config.path_prefix
        if not prefix:
            return full_path
        if full_path.startswith(prefix + "/"):
            return full_path[len(prefix) + 1 :]
        return full_path

    # -- ID generation --

    @staticmethod
    def _sanitize_id(raw: str) -> str:
        """Sanitize a string for use as a document ID."""
        safe = re.sub(r"[^a-zA-Z0-9_.\-]", "-", raw)
        safe = re.sub(r"-{2,}", "-", safe)
        safe = safe.strip("-").lower()
        return safe[:200] if len(safe) > 200 else safe

    def _make_file_doc_id(self, file_path: str) -> str:
        repo_part = self.config.repo.replace("/", ".")
        path_no_ext = file_path
        dot = path_no_ext.rfind(".")
        if dot > 0:
            path_no_ext = path_no_ext[:dot]
        path_part = path_no_ext.replace("/", ".")
        return self._sanitize_id(f"github.{repo_part}.{path_part}")

    def _make_folder_doc_id(self, folder_path: str) -> str:
        repo_part = self.config.repo.replace("/", ".")
        path_part = folder_path.replace("/", ".")
        return self._sanitize_id(f"github.{repo_part}.{path_part}._dir")

    def _make_file_source_id(self, file_path: str) -> str:
        return f"file:{file_path}"

    def _make_folder_source_id(self, folder_path: str) -> str:
        return f"dir:{folder_path}"

    def _resolve_parent_doc_id(
        self,
        path: str,
        source_id_to_doc_id: Dict[str, str],
    ) -> Optional[str]:
        """Find the parent folder's doc ID for a given file or folder path."""
        if not self.config.preserve_hierarchy:
            return None
        relative = self._relativize(path)
        last_slash = relative.rfind("/")
        if last_slash <= 0:
            return None
        parent_rel = relative[:last_slash]
        prefix = self.config.path_prefix
        parent_full = f"{prefix}/{parent_rel}" if prefix else parent_rel
        parent_source_id = f"dir:{parent_full}"
        return source_id_to_doc_id.get(parent_source_id)

    # -- Title generation --

    @staticmethod
    def _title_from_path(path: str) -> str:
        """Derive a human-readable title from a file/folder path."""
        base = path.rsplit("/", 1)[-1]
        dot = base.rfind(".")
        if dot > 0:
            base = base[:dot]
        words = base.replace("-", " ").replace("_", " ").split()
        return " ".join(w[0].upper() + w[1:].lower() if w else w for w in words)

    # -- Limit helpers --

    def _reached_limit(self, count: int) -> bool:
        if self.config.max_documents == 0:
            return False
        return count >= self.config.max_documents
