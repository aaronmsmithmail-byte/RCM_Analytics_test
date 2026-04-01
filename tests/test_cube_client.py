"""Unit tests for src/cube_client.py.

Since Cube isn't running in the test environment, these tests verify:
- Health check returns False when Cube is unavailable
- Query functions return None gracefully
- build_cube_filters produces correct filter structures
"""

from unittest.mock import MagicMock, patch

from src.cube_client import (
    _health_cache,
    build_cube_filters,
    get_cube_meta,
    get_semantic_mappings,
    is_cube_available,
    query_cube,
)


class TestCubeHealthCheck:
    def setup_method(self):
        _health_cache["available"] = None
        _health_cache["checked_at"] = 0

    def test_unavailable_when_no_server(self):
        _health_cache["available"] = None
        _health_cache["checked_at"] = 0
        assert is_cube_available() is False

    @patch("src.cube_client.requests.get")
    def test_available_when_server_responds(self, mock_get):
        _health_cache["available"] = None
        _health_cache["checked_at"] = 0
        mock_get.return_value = MagicMock(status_code=200)
        assert is_cube_available() is True


class TestCubeMetadata:
    def test_returns_none_when_unavailable(self):
        _health_cache["available"] = False
        _health_cache["checked_at"] = 9999999999
        assert get_cube_meta() is None

    def test_semantic_mappings_none_when_unavailable(self):
        _health_cache["available"] = False
        _health_cache["checked_at"] = 9999999999
        assert get_semantic_mappings() is None


class TestCubeQuery:
    def test_returns_none_when_unavailable(self):
        _health_cache["available"] = False
        _health_cache["checked_at"] = 9999999999
        result = query_cube(["claims.count"])
        assert result is None


class TestBuildCubeFilters:
    def test_date_only(self):
        filters, time_dims = build_cube_filters("2024-01-01", "2024-12-31")
        assert filters == []
        assert len(time_dims) == 1
        assert time_dims[0]["dimension"] == "claims.date_of_service"
        assert time_dims[0]["dateRange"] == ["2024-01-01", "2024-12-31"]

    def test_with_payer_filter(self):
        filters, _ = build_cube_filters("2024-01-01", "2024-12-31", payer_id="PYR001")
        assert len(filters) == 1
        assert filters[0]["member"] == "claims.payer_id"
        assert filters[0]["values"] == ["PYR001"]

    def test_with_all_filters(self):
        filters, time_dims = build_cube_filters(
            "2024-01-01",
            "2024-12-31",
            payer_id="PYR001",
            department="Cardiology",
            encounter_type="Outpatient",
        )
        assert len(filters) == 3
        assert len(time_dims) == 1
        members = {f["member"] for f in filters}
        assert members == {"claims.payer_id", "encounters.department", "encounters.encounter_type"}

    def test_no_optional_filters(self):
        filters, _ = build_cube_filters("2024-01-01", "2024-12-31")
        assert filters == []
