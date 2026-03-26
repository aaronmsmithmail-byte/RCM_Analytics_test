"""Unit tests for src/neo4j_client.py.

Since Neo4j isn't running in the test environment, these tests verify:
- Health check returns False when Neo4j is unavailable
- Query functions return None gracefully
- Seed function handles missing server gracefully
"""

from unittest.mock import MagicMock, patch

from src.neo4j_client import (
    _health_cache,
    get_kg_edges,
    get_kg_nodes,
    is_neo4j_available,
    seed_knowledge_graph,
)


class TestNeo4jHealthCheck:
    def setup_method(self):
        _health_cache["available"] = None
        _health_cache["checked_at"] = 0

    def test_unavailable_when_no_server(self):
        _health_cache["available"] = None
        _health_cache["checked_at"] = 0
        assert is_neo4j_available() is False

    def test_unavailable_when_neo4j_not_installed(self):
        _health_cache["available"] = None
        _health_cache["checked_at"] = 0
        with patch("src.neo4j_client._HAS_NEO4J", False):
            assert is_neo4j_available() is False


class TestNeo4jQueries:
    def test_nodes_none_when_unavailable(self):
        _health_cache["available"] = False
        _health_cache["checked_at"] = 9999999999
        assert get_kg_nodes() is None

    def test_edges_none_when_unavailable(self):
        _health_cache["available"] = False
        _health_cache["checked_at"] = 9999999999
        assert get_kg_edges() is None

    def test_seed_returns_false_when_unavailable(self):
        _health_cache["available"] = False
        _health_cache["checked_at"] = 9999999999
        assert seed_knowledge_graph() is False


class TestNeo4jWithMockedDriver:
    @patch("src.neo4j_client.GraphDatabase")
    def test_get_kg_nodes_returns_list(self, mock_gdb):
        _health_cache["available"] = True
        _health_cache["checked_at"] = 9999999999

        mock_record = {
            "entity_id": "claims",
            "entity_name": "claims",
            "entity_group": "Transactional",
            "silver_table": "silver_claims",
            "description": "claim_id PK, ...",
            "source_system": "Clearinghouse",
        }
        mock_session = MagicMock()
        mock_session.run.return_value = [mock_record]
        mock_driver = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)
        mock_gdb.driver.return_value = mock_driver

        nodes = get_kg_nodes()
        assert nodes is not None
        assert len(nodes) == 1
        assert nodes[0]["entity_id"] == "claims"

    @patch("src.neo4j_client.GraphDatabase")
    def test_get_kg_edges_returns_list(self, mock_gdb):
        _health_cache["available"] = True
        _health_cache["checked_at"] = 9999999999

        mock_record = {
            "parent_entity": "claims",
            "child_entity": "payments",
            "join_column": "claim_id",
            "cardinality": "1:N",
            "business_meaning": "A claim may receive payments",
        }
        mock_session = MagicMock()
        mock_session.run.return_value = [mock_record]
        mock_driver = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)
        mock_gdb.driver.return_value = mock_driver

        edges = get_kg_edges()
        assert edges is not None
        assert len(edges) == 1
        assert edges[0]["join_column"] == "claim_id"
