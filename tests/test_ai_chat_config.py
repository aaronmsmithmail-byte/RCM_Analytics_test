"""
Tests for AI assistant environment variable configuration parsing.

Covers the AI_MAX_ROWS and AI_MAX_ITERATIONS env vars in src/ai_chat.py,
which are parsed at module import time with bounds clamping and ValueError
fallback for non-numeric values.
"""

import importlib
import os

import pytest
import src.ai_chat as ai_chat


def _reload(env_overrides: dict):
    """
    Temporarily set env vars, reload ai_chat, return (_MAX_ROWS, _MAX_ITERATIONS).
    Always restores original env and reloads back to defaults on exit.
    """
    originals = {k: os.environ.get(k) for k in env_overrides}
    try:
        for k, v in env_overrides.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        importlib.reload(ai_chat)
        return ai_chat._MAX_ROWS, ai_chat._MAX_ITERATIONS
    finally:
        for k, v in originals.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        importlib.reload(ai_chat)


# ── AI_MAX_ROWS ────────────────────────────────────────────────────────────────

def test_max_rows_default():
    rows, _ = _reload({"AI_MAX_ROWS": None})
    assert rows == 100


def test_max_rows_custom_value():
    rows, _ = _reload({"AI_MAX_ROWS": "50"})
    assert rows == 50


def test_max_rows_nonnumeric_falls_back_to_default():
    rows, _ = _reload({"AI_MAX_ROWS": "not_a_number"})
    assert rows == 100


def test_max_rows_below_minimum_is_clamped():
    rows, _ = _reload({"AI_MAX_ROWS": "3"})
    assert rows == 10


def test_max_rows_exactly_at_minimum():
    rows, _ = _reload({"AI_MAX_ROWS": "10"})
    assert rows == 10


# ── AI_MAX_ITERATIONS ──────────────────────────────────────────────────────────

def test_max_iterations_default():
    _, iters = _reload({"AI_MAX_ITERATIONS": None})
    assert iters == 8


def test_max_iterations_custom_value():
    _, iters = _reload({"AI_MAX_ITERATIONS": "12"})
    assert iters == 12


def test_max_iterations_nonnumeric_falls_back_to_default():
    _, iters = _reload({"AI_MAX_ITERATIONS": "bad_value"})
    assert iters == 8


def test_max_iterations_below_minimum_is_clamped():
    _, iters = _reload({"AI_MAX_ITERATIONS": "0"})
    assert iters == 1


def test_max_iterations_exactly_at_minimum():
    _, iters = _reload({"AI_MAX_ITERATIONS": "1"})
    assert iters == 1
