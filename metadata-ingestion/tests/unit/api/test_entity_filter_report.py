import dataclasses

from datahub.ingestion.api.report import EntityFilterReport, Report, SupportsAsObj


@dataclasses.dataclass
class MyReport(Report):
    views: EntityFilterReport = EntityFilterReport.field(type="view")


def test_entity_filter_report():
    report = MyReport()
    assert report.views.type == "view"
    assert isinstance(report, SupportsAsObj)

    report2 = MyReport()

    report.views.processed(entity="foo")
    report.views.dropped(entity="bar")

    assert (
        report.as_string() == "{'views': {'filtered': ['bar'], 'processed': ['foo']}}"
    )

    # Verify that the reports don't accidentally share any state.
    assert report2.as_string() == "{'views': {'filtered': [], 'processed': []}}"


def test_progress_report_caps_failures_and_warnings():
    from datahub.ingestion.api.report import _cap_report_samples

    obj = {
        "events_produced": 500,
        "failures": ["err1", "err2", "err3", "err4", "err5"],
        "warnings": ["w1", "w2", "w3"],
        "infos": ["i1", "i2"],
        "aspects": {"dataset": {"schemaMetadata": 100}},
    }
    caps = {"failures": 2, "warnings": 1, "infos": 5}
    capped = _cap_report_samples(obj, caps)

    # failures capped to 2 + summary message
    assert len(capped["failures"]) == 3
    assert "interim report" in capped["failures"][-1]

    # warnings capped to 1 + summary message
    assert len(capped["warnings"]) == 2
    assert "interim report" in capped["warnings"][-1]

    # infos under cap — untouched
    assert capped["infos"] == ["i1", "i2"]

    # non-sample fields untouched
    assert capped["events_produced"] == 500
    assert capped["aspects"] == {"dataset": {"schemaMetadata": 100}}
