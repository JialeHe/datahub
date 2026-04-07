"""Report classes for the GitHub Docs ingestion source."""

from dataclasses import dataclass, field
from typing import List, Tuple

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)


@dataclass
class GitHubDocsSourceReport(StaleEntityRemovalSourceReport):
    """Ingestion report for GitHub Docs source."""

    # File-level metrics
    files_discovered: int = 0
    files_imported: int = 0
    files_skipped: int = 0
    files_failed: int = 0
    failed_files: List[Tuple[str, str]] = field(default_factory=list)

    # Folder metrics
    folders_created: int = 0

    # Content metrics
    total_text_bytes: int = 0

    def report_file_discovered(self) -> None:
        self.files_discovered += 1

    def report_file_imported(self, path: str, num_bytes: int) -> None:
        self.files_imported += 1
        self.total_text_bytes += num_bytes

    def report_file_skipped(self, path: str, reason: str) -> None:
        self.files_skipped += 1
        self.report_warning("File skipped", context=f"{path}: {reason}")

    def report_file_failed(self, path: str, error: str) -> None:
        self.files_failed += 1
        self.failed_files.append((path, error))
        self.report_failure("File import failed", context=f"{path}: {error}")

    def report_folder_created(self) -> None:
        self.folders_created += 1
