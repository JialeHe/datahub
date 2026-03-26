"""
Looker V2 Source Report.

Fine-grained statistics and reporting for the Looker V2 source.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyDict, LossyList, LossySet


@dataclass
class LookerV2SourceReport(StaleEntityRemovalSourceReport):
    """
    Comprehensive report for Looker V2 source ingestion.

    Tracks entity counts, lineage statistics, processing metrics,
    API telemetry, and stage timings.
    """

    # =========== Entity Counts ===========
    dashboards_discovered: int = 0
    dashboards_scanned: int = 0
    dashboards_filtered: LossyList[str] = field(default_factory=LossyList)

    charts_discovered: int = 0
    charts_scanned: int = 0

    looks_discovered: int = 0
    looks_scanned: int = 0

    explores_discovered: int = 0
    explores_scanned: int = 0

    models_discovered: int = 0

    views_discovered: int = 0
    views_reachable: int = 0  # Referenced by explores
    views_unreachable: int = 0  # Included but not referenced
    views_emitted: int = 0
    views_filtered: LossyList[str] = field(default_factory=LossyList)

    # =========== Orphaned Files ===========
    orphaned_view_files: LossyList[str] = field(default_factory=LossyList)
    orphaned_view_files_count: int = 0

    # =========== Lineage Stats ===========
    lineage_edges_extracted: int = 0
    lineage_via_api: int = 0  # View lineage from API SQL generation
    lineage_via_file_parse: int = 0  # View lineage from LookML parsing
    lineage_via_pdt_graph: int = 0  # View lineage from PDT graph API
    lineage_failures: LossyList[str] = field(default_factory=LossyList)

    # =========== PDT Graph Stats ===========
    pdt_graphs_fetched: int = 0
    pdt_edges_discovered: int = 0

    # =========== Processing Stats ===========
    refinements_discovered: int = 0
    refinements_applied: int = 0
    refinements_by_project: Dict[str, int] = field(default_factory=dict)
    refinement_chains: Dict[str, List[str]] = field(default_factory=dict)
    fields_added_by_refinement: int = 0
    fields_modified_by_refinement: int = 0

    liquid_templates_skipped: LossyList[str] = field(default_factory=LossyList)
    template_constants_missing: LossyList[str] = field(default_factory=LossyList)
    template_liquid_errors: LossyList[str] = field(default_factory=LossyList)
    projects_processed: int = 0  # Including dependencies

    # =========== Field Splitting Stats ===========
    field_splitting_used: int = 0  # Views that triggered splitting
    field_chunks_processed: int = 0  # Total chunks across all views
    field_chunks_succeeded: int = 0
    field_chunks_failed: int = 0
    individual_field_fallbacks: int = 0  # Times fallback was triggered
    problematic_fields: LossyList[str] = field(default_factory=LossyList)

    # =========== Usage Stats ===========
    dashboards_with_usage: int = 0
    charts_with_usage: int = 0
    dashboards_scanned_for_usage: int = 0
    charts_scanned_for_usage: int = 0
    dashboards_skipped_for_usage: LossySet[str] = field(default_factory=LossySet)
    charts_skipped_for_usage: LossySet[str] = field(default_factory=LossySet)
    dashboards_with_activity: LossySet[str] = field(default_factory=LossySet)
    charts_with_activity: LossySet[str] = field(default_factory=LossySet)
    query_latency: Dict[str, Any] = field(default_factory=dict)
    user_resolution_latency: Dict[str, Any] = field(default_factory=dict)

    # =========== API Telemetry ===========
    # Top N slowest API calls (N configurable via DATAHUB_LOOKER_API_TELEMETRY_TOP_K)
    api_call_latencies: LossyDict[str, float] = field(default_factory=LossyDict)
    api_call_counts: Dict[str, int] = field(default_factory=dict)
    api_total_time_seconds: float = 0.0

    # =========== API Performance ===========
    api_calls_by_endpoint: Dict[str, int] = field(default_factory=dict)
    api_calls_saved_by_cache: int = 0
    api_calls_parallel_batches: int = 0

    # =========== Cache Stats ===========
    user_registry_size: int = 0
    folder_registry_size: int = 0
    explore_cache_hits: int = 0
    explore_cache_misses: int = 0

    # =========== Stage Timings ===========
    stage_timings_seconds: Dict[str, float] = field(default_factory=dict)

    # =========== Upstream Latency Tracking ===========
    _upstream_latency_min: Optional[float] = field(default=None, repr=False)
    _upstream_latency_max: Optional[float] = field(default=None, repr=False)
    _upstream_latency_sum: float = field(default=0.0, repr=False)
    _upstream_latency_count: int = field(default=0, repr=False)

    def report_upstream_latency(self, start_time: float, end_time: float) -> None:
        """Record latency for an upstream API call."""
        latency = end_time - start_time
        if self._upstream_latency_min is None or latency < self._upstream_latency_min:
            self._upstream_latency_min = latency
        if self._upstream_latency_max is None or latency > self._upstream_latency_max:
            self._upstream_latency_max = latency
        self._upstream_latency_sum += latency
        self._upstream_latency_count += 1

    def report_orphaned_file(self, file_path: str) -> None:
        """Report an orphaned view file not included by any model."""
        self.orphaned_view_files.append(file_path)
        self.orphaned_view_files_count += 1
        self.report_warning(
            title="Orphaned LookML File",
            message=f"View file not included by any model: {file_path}",
            context=file_path,
        )

    def report_lineage_failure(self, view_name: str, error: str) -> None:
        """Report a lineage extraction failure."""
        self.lineage_failures.append(f"{view_name}: {error}")

    def report_field_splitting(
        self,
        view_name: str,
        chunks_processed: int,
        chunks_succeeded: int,
        chunks_failed: int,
    ) -> None:
        """Report field splitting statistics for a view."""
        self.field_splitting_used += 1
        self.field_chunks_processed += chunks_processed
        self.field_chunks_succeeded += chunks_succeeded
        self.field_chunks_failed += chunks_failed

    def report_dashboards_scanned_for_usage(self, num_dashboards: int) -> None:
        """Duck-type compatible with LookerDashboardSourceReport for usage generators."""
        self.dashboards_scanned_for_usage += num_dashboards

    def report_charts_scanned_for_usage(self, num_charts: int) -> None:
        """Duck-type compatible with LookerDashboardSourceReport for usage generators."""
        self.charts_scanned_for_usage += num_charts

    def report_query_latency(
        self, query_type: str, latency: datetime.timedelta
    ) -> None:
        """Duck-type compatible with LookerDashboardSourceReport for usage generators."""
        self.query_latency[query_type] = latency

    def report_user_resolution_latency(
        self, generator_type: str, latency: datetime.timedelta
    ) -> None:
        """Duck-type compatible with LookerDashboardSourceReport for usage generators."""
        self.user_resolution_latency[generator_type] = latency

    def report_refinement(
        self,
        view_name: str,
        project: str,
        fields_added: int = 0,
        fields_modified: int = 0,
    ) -> None:
        """Report a view refinement."""
        self.refinements_discovered += 1
        self.refinements_applied += 1
        self.refinements_by_project[project] = (
            self.refinements_by_project.get(project, 0) + 1
        )
        self.fields_added_by_refinement += fields_added
        self.fields_modified_by_refinement += fields_modified

    def update_api_telemetry(
        self,
        latencies: Dict[str, float],
        counts: Dict[str, int],
        total_time: float,
    ) -> None:
        """Update API telemetry from the telemetry tracker."""
        for key, value in latencies.items():
            self.api_call_latencies[key] = value
        self.api_call_counts.update(counts)
        self.api_total_time_seconds = total_time

    def compute_stats(self) -> None:
        """Compute derived statistics."""
        super().compute_stats()

        # Compute upstream latency stats
        if self._upstream_latency_count > 0:
            avg_latency = self._upstream_latency_sum / self._upstream_latency_count
            self.info(
                title="Upstream API Latency",
                message=(
                    f"min={self._upstream_latency_min:.3f}s, "
                    f"max={self._upstream_latency_max:.3f}s, "
                    f"avg={avg_latency:.3f}s, "
                    f"count={self._upstream_latency_count}"
                ),
            )
