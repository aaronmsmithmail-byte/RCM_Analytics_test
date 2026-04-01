"""Unit tests for run_agentic_turn() in src/ai_chat.py.

Uses mocked OpenAI client to test the tool-calling loop without hitting
a real API. Covers text responses, tool calls, max iterations, API key
validation, and error paths.
"""

import json
import os
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from src.ai_chat import _MAX_ITERATIONS, run_agentic_turn
from src.database import create_tables

# ===========================================================================
# Helpers to build mock API responses
# ===========================================================================


def _make_text_response(content="Here is the answer."):
    """Simulate an API response with a final text message (no tool calls)."""
    msg = MagicMock()
    msg.content = content
    msg.tool_calls = None

    choice = MagicMock()
    choice.message = msg
    choice.finish_reason = "stop"

    response = MagicMock()
    response.choices = [choice]
    return response


def _make_tool_call_response(query="SELECT 1", description="Test query", tool_call_id="tc_001"):
    """Simulate an API response requesting a tool call."""
    func = MagicMock()
    func.name = "run_sql"
    func.arguments = json.dumps({"query": query, "description": description})

    tc = MagicMock()
    tc.id = tool_call_id
    tc.function = func

    msg = MagicMock()
    msg.content = ""
    msg.tool_calls = [tc]

    choice = MagicMock()
    choice.message = msg
    choice.finish_reason = "tool_calls"

    response = MagicMock()
    response.choices = [choice]
    return response


# ===========================================================================
# Fixtures
# ===========================================================================


@pytest.fixture
def db(tmp_path):
    """Temporary database for tool execution."""
    db_path = str(tmp_path / "test.db")
    conn = duckdb.connect(db_path)
    create_tables(conn)
    conn.execute("""
        INSERT INTO silver_payers VALUES
            ('PYR001','Aetna','Commercial',0.85,'C001');
        INSERT INTO silver_patients VALUES
            ('PAT001','Alice','Smith','1980-01-01','F','PYR001','M001','10001');
        INSERT INTO silver_providers VALUES
            ('PRV001','Dr. A','1111111111','Cardiology','Internal Medicine');
        INSERT INTO silver_encounters VALUES
            ('ENC010','PAT001','PRV001','2024-01-15','2024-01-15','Outpatient','Cardiology');
        INSERT INTO silver_claims VALUES
            ('CLM001','ENC010','PAT001','PYR001','2024-01-15','2024-01-17',
             1000.0,'Paid',1,'Electronic',NULL);
    """)
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def base_messages():
    """Minimal message list with a system and user message."""
    return [
        {"role": "system", "content": "You are an RCM analyst."},
        {"role": "user", "content": "What is the total charge amount?"},
    ]


# ===========================================================================
# API key validation
# ===========================================================================


class TestRunAgenticTurnApiKey:
    def test_missing_api_key_yields_error(self, base_messages):
        with patch.dict(os.environ, {"OPENROUTER_API_KEY": ""}, clear=False):
            events = list(run_agentic_turn(base_messages))
        assert len(events) == 1
        assert events[0]["type"] == "error"
        assert "OPENROUTER_API_KEY" in events[0]["message"]

    def test_placeholder_api_key_yields_error(self, base_messages):
        with patch.dict(os.environ, {"OPENROUTER_API_KEY": "your_api_key_here"}, clear=False):
            events = list(run_agentic_turn(base_messages))
        assert len(events) == 1
        assert events[0]["type"] == "error"


# ===========================================================================
# Text response (no tool calls)
# ===========================================================================


class TestRunAgenticTurnTextResponse:
    @patch("openai.OpenAI")
    def test_text_response_yielded(self, mock_openai_cls, base_messages):
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client
        mock_client.chat.completions.create.return_value = _make_text_response("The total is $1,000.")

        with patch.dict(os.environ, {"OPENROUTER_API_KEY": "test_key_123"}, clear=False):
            events = list(run_agentic_turn(base_messages))

        text_events = [e for e in events if e["type"] == "text"]
        assert len(text_events) == 1
        assert text_events[0]["content"] == "The total is $1,000."

    @patch("openai.OpenAI")
    def test_messages_appended_with_assistant_response(self, mock_openai_cls, base_messages):
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client
        mock_client.chat.completions.create.return_value = _make_text_response("Answer here.")

        with patch.dict(os.environ, {"OPENROUTER_API_KEY": "test_key_123"}, clear=False):
            list(run_agentic_turn(base_messages))

        # messages should have been mutated with assistant reply
        assert base_messages[-1]["role"] == "assistant"
        assert base_messages[-1]["content"] == "Answer here."


# ===========================================================================
# Tool call flow
# ===========================================================================


class TestRunAgenticTurnToolCalls:
    @patch("openai.OpenAI")
    def test_tool_call_then_text(self, mock_openai_cls, base_messages, db):
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client

        # First call: model requests a tool call; second call: model returns text
        mock_client.chat.completions.create.side_effect = [
            _make_tool_call_response(
                query="SELECT COUNT(*) AS n FROM silver_claims",
                description="Count claims",
            ),
            _make_text_response("There is 1 claim."),
        ]

        with patch.dict(os.environ, {"OPENROUTER_API_KEY": "test_key_123"}, clear=False):
            with patch("src.ai_chat.execute_sql_tool") as mock_exec:
                mock_exec.return_value = {
                    "columns": ["n"],
                    "rows": [[1]],
                    "row_count": 1,
                    "total_rows": 1,
                    "truncated": False,
                }
                events = list(run_agentic_turn(base_messages))

        types = [e["type"] for e in events]
        assert "tool_call" in types
        assert "tool_result" in types
        assert "text" in types

    @patch("openai.OpenAI")
    def test_tool_result_contains_sql_and_description(self, mock_openai_cls, base_messages):
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client

        mock_client.chat.completions.create.side_effect = [
            _make_tool_call_response(query="SELECT 1 AS x", description="Test"),
            _make_text_response("Done."),
        ]

        with patch.dict(os.environ, {"OPENROUTER_API_KEY": "test_key_123"}, clear=False):
            with patch("src.ai_chat.execute_sql_tool") as mock_exec:
                mock_exec.return_value = {
                    "columns": ["x"],
                    "rows": [[1]],
                    "row_count": 1,
                    "total_rows": 1,
                    "truncated": False,
                }
                events = list(run_agentic_turn(base_messages))

        tool_result = [e for e in events if e["type"] == "tool_result"][0]
        assert tool_result["sql"] == "SELECT 1 AS x"
        assert tool_result["description"] == "Test"
        assert tool_result["columns"] == ["x"]

    @patch("openai.OpenAI")
    def test_tool_error_propagated_in_result(self, mock_openai_cls, base_messages):
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client

        mock_client.chat.completions.create.side_effect = [
            _make_tool_call_response(query="SELECT * FROM bad_table", description="Bad query"),
            _make_text_response("Sorry, that failed."),
        ]

        with patch.dict(os.environ, {"OPENROUTER_API_KEY": "test_key_123"}, clear=False):
            with patch("src.ai_chat.execute_sql_tool") as mock_exec:
                mock_exec.return_value = {"error": "no such table: bad_table"}
                events = list(run_agentic_turn(base_messages))

        tool_result = [e for e in events if e["type"] == "tool_result"][0]
        assert tool_result["error"] == "no such table: bad_table"

    @patch("openai.OpenAI")
    def test_messages_include_tool_result_for_history(self, mock_openai_cls, base_messages):
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client

        mock_client.chat.completions.create.side_effect = [
            _make_tool_call_response(query="SELECT 1", description="Test"),
            _make_text_response("Done."),
        ]

        with patch.dict(os.environ, {"OPENROUTER_API_KEY": "test_key_123"}, clear=False):
            with patch("src.ai_chat.execute_sql_tool") as mock_exec:
                mock_exec.return_value = {
                    "columns": ["1"],
                    "rows": [[1]],
                    "row_count": 1,
                    "total_rows": 1,
                    "truncated": False,
                }
                list(run_agentic_turn(base_messages))

        # Should have: system, user, assistant(tool_calls), tool(result), assistant(text)
        roles = [m["role"] for m in base_messages]
        assert "tool" in roles
        assert roles.count("assistant") == 2  # one with tool_calls, one with text


# ===========================================================================
# Max iterations
# ===========================================================================


class TestRunAgenticTurnMaxIterations:
    @patch("openai.OpenAI")
    def test_exceeds_max_iterations_yields_error(self, mock_openai_cls, base_messages):
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client

        # Always return tool calls, never text — should hit iteration limit
        mock_client.chat.completions.create.return_value = _make_tool_call_response(
            query="SELECT 1", description="Loop"
        )

        with patch.dict(os.environ, {"OPENROUTER_API_KEY": "test_key_123"}, clear=False):
            with patch("src.ai_chat.execute_sql_tool") as mock_exec:
                mock_exec.return_value = {
                    "columns": ["1"],
                    "rows": [[1]],
                    "row_count": 1,
                    "total_rows": 1,
                    "truncated": False,
                }
                events = list(run_agentic_turn(base_messages))

        error_events = [e for e in events if e["type"] == "error"]
        assert len(error_events) == 1
        assert "Exceeded" in error_events[0]["message"]

    @patch("openai.OpenAI")
    def test_api_called_max_iterations_times(self, mock_openai_cls, base_messages):
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client

        mock_client.chat.completions.create.return_value = _make_tool_call_response(
            query="SELECT 1", description="Loop"
        )

        with patch.dict(os.environ, {"OPENROUTER_API_KEY": "test_key_123"}, clear=False):
            with patch("src.ai_chat.execute_sql_tool") as mock_exec:
                mock_exec.return_value = {
                    "columns": ["1"],
                    "rows": [[1]],
                    "row_count": 1,
                    "total_rows": 1,
                    "truncated": False,
                }
                list(run_agentic_turn(base_messages))

        assert mock_client.chat.completions.create.call_count == _MAX_ITERATIONS


# ===========================================================================
# Model selection
# ===========================================================================


class TestRunAgenticTurnModelSelection:
    @patch("openai.OpenAI")
    def test_custom_model_passed_to_api(self, mock_openai_cls, base_messages):
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client
        mock_client.chat.completions.create.return_value = _make_text_response("OK")

        with patch.dict(os.environ, {"OPENROUTER_API_KEY": "test_key_123"}, clear=False):
            list(run_agentic_turn(base_messages, model="openai/gpt-5.4"))

        call_kwargs = mock_client.chat.completions.create.call_args
        assert call_kwargs.kwargs["model"] == "openai/gpt-5.4"
