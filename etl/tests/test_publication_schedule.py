from __future__ import annotations

import pytest

from ficha_etl.publication_schedule import (
    PublicationGapError,
    next_publication_month,
    parse_available_months,
)


def test_current_before_baseline_starts_at_2026_06() -> None:
    assert (
        next_publication_month(
            "2026-04",
            ["2026-05", "2026-06", "2026-07", "2026-08"],
        )
        == "2026-06"
    )


def test_after_baseline_advances_exactly_one_month() -> None:
    available = ["2026-06", "2026-07", "2026-08"]
    assert next_publication_month("2026-06", available) == "2026-07"
    assert next_publication_month("2026-07", available) == "2026-08"


def test_noop_when_upstream_has_not_reached_expected_month() -> None:
    assert next_publication_month("2026-08", ["2026-06", "2026-07", "2026-08"]) is None


def test_missing_expected_month_never_allows_jump_to_later_snapshot() -> None:
    with pytest.raises(PublicationGapError, match="expected next publication 2026-07"):
        next_publication_month("2026-06", ["2026-06", "2026-08"])


def test_missing_baseline_with_later_months_is_a_gap() -> None:
    with pytest.raises(PublicationGapError, match="expected next publication 2026-06"):
        next_publication_month("2026-04", ["2026-05", "2026-07", "2026-08"])


def test_parser_tolerates_human_output_and_deduplicates() -> None:
    assert parse_available_months("oldest=2023-05 newest=2026-08\n2026-06\n2026-07 2026-08\n") == (
        "2023-05",
        "2026-06",
        "2026-07",
        "2026-08",
    )
