"""
Neo4j Knowledge Graph Client
=============================

Connects to a Neo4j instance to read the healthcare RCM entity-relationship
knowledge graph (10 entities, 11 FK relationships).

Gracefully returns None when Neo4j is unavailable so the app falls back
to DuckDB meta_kg_* tables.

Environment variables (set in .env — never hard-code values here):
    NEO4J_URI      — Bolt URI (default: bolt://localhost:7687)
    NEO4J_USER     — Username (default: neo4j)
    NEO4J_PASSWORD — Password (required; no default — set in .env)
"""

import os
import time

try:
    from neo4j import GraphDatabase

    _HAS_NEO4J = True
except ImportError:
    _HAS_NEO4J = False

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "")

# TTL-cached health check (avoids retrying on every request)
_health_cache = {"available": None, "checked_at": 0}
_HEALTH_TTL = 60  # seconds


def is_neo4j_available() -> bool:
    """Check if Neo4j is reachable. Result cached for 60 seconds."""
    if not _HAS_NEO4J:
        return False
    now = time.time()
    if now - _health_cache["checked_at"] < _HEALTH_TTL and _health_cache["available"] is not None:
        return _health_cache["available"]
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        driver.close()
        _health_cache["available"] = True
    except Exception:
        _health_cache["available"] = False
    _health_cache["checked_at"] = now
    return _health_cache["available"]


def _get_driver():
    """Create a Neo4j driver instance."""
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def get_kg_nodes():
    """
    Fetch all Entity nodes from Neo4j.

    Returns:
        list of dicts with keys: entity_id, entity_name, entity_group,
        silver_table, description, source_system.
        Returns None if Neo4j is unavailable.
    """
    if not is_neo4j_available():
        return None
    try:
        driver = _get_driver()
        with driver.session() as session:
            result = session.run(
                "MATCH (n:Entity) "
                "RETURN n.entity_id AS entity_id, n.entity_name AS entity_name, "
                "       n.entity_group AS entity_group, n.silver_table AS silver_table, "
                "       n.description AS description, n.source_system AS source_system "
                "ORDER BY n.entity_group, n.entity_id"
            )
            nodes = [dict(record) for record in result]
        driver.close()
        return nodes if nodes else None
    except Exception:
        return None


def get_kg_edges():
    """
    Fetch all HAS_FK relationships from Neo4j.

    Returns:
        list of dicts with keys: parent_entity, child_entity, join_column,
        cardinality, business_meaning.
        Returns None if Neo4j is unavailable.
    """
    if not is_neo4j_available():
        return None
    try:
        driver = _get_driver()
        with driver.session() as session:
            result = session.run(
                "MATCH (parent:Entity)-[r:HAS_FK]->(child:Entity) "
                "RETURN parent.entity_id AS parent_entity, "
                "       child.entity_id AS child_entity, "
                "       r.join_column AS join_column, "
                "       r.cardinality AS cardinality, "
                "       r.business_meaning AS business_meaning "
                "ORDER BY parent.entity_id, child.entity_id"
            )
            edges = [dict(record) for record in result]
        driver.close()
        return edges if edges else None
    except Exception:
        return None


def seed_knowledge_graph():
    """
    Seed Neo4j with the knowledge graph from the Cypher script.
    Only runs if Neo4j is available and empty.
    """
    if not is_neo4j_available():
        return False
    try:
        driver = _get_driver()
        with driver.session() as session:
            # Check if already seeded
            result = session.run("MATCH (n:Entity) RETURN count(n) AS cnt")
            count = result.single()["cnt"]
            if count > 0:
                driver.close()
                return True  # Already seeded

            # Read and execute seed script
            seed_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "neo4j", "seed.cypher")
            if not os.path.exists(seed_path):
                driver.close()
                return False

            with open(seed_path) as f:
                cypher = f.read()

            # Split by semicolons and execute each statement
            for statement in cypher.split(";"):
                stmt = statement.strip()
                if stmt and not stmt.startswith("//"):
                    session.run(stmt)

        driver.close()
        return True
    except Exception:
        return False
