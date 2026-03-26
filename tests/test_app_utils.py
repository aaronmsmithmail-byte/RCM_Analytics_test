"""Unit tests for app.py utility functions (df_to_csv, dfs_to_excel, _linear_forecast).

Since app.py executes Streamlit dashboard logic at module level, we cannot
import from it directly. Instead, we extract and test the pure function
implementations that are defined in app.py.
"""

import io

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Reimplementations of the pure functions from app.py for testability.
# These mirror the exact logic in app.py lines 242-309.
# ---------------------------------------------------------------------------

def df_to_csv(df: pd.DataFrame) -> bytes:
    """Identical to app.df_to_csv (app.py:242-243)."""
    return df.to_csv(index=False).encode("utf-8")


def dfs_to_excel(sheets: dict[str, pd.DataFrame]) -> bytes:
    """Identical to app.dfs_to_excel (app.py:246-252)."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
    return buf.getvalue()


def _linear_forecast(series: pd.Series, periods_ahead: int = 3):
    """Identical to app._linear_forecast (app.py:276-309)."""
    clean = series.dropna()
    if len(clean) < 4:
        return None, None, None, None
    x = np.arange(len(series), dtype=float)
    y = series.values.astype(float)
    mask = ~np.isnan(y)
    coeffs = np.polyfit(x[mask], y[mask], 1)
    fitted = np.polyval(coeffs, x)
    x_future = np.arange(len(series), len(series) + periods_ahead, dtype=float)
    forecast = np.polyval(coeffs, x_future)
    resid_std = float(np.std(y[mask] - np.polyval(coeffs, x[mask])))
    try:
        last = pd.Period(series.index[-1], freq="M")
        future_labels = [(last + i + 1).strftime("%Y-%m") for i in range(periods_ahead)]
    except Exception:
        future_labels = [f"+{i+1}m" for i in range(periods_ahead)]
    return fitted, forecast, resid_std, future_labels


# ===========================================================================
# df_to_csv
# ===========================================================================

class TestDfToCsv:
    def test_returns_bytes(self):
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        result = df_to_csv(df)
        assert isinstance(result, bytes)

    def test_csv_content_correct(self):
        df = pd.DataFrame({"name": ["Alice", "Bob"], "value": [10, 20]})
        result = df_to_csv(df).decode("utf-8")
        lines = result.strip().split("\n")
        assert lines[0] == "name,value"
        assert lines[1] == "Alice,10"

    def test_empty_dataframe(self):
        df = pd.DataFrame({"a": []})
        result = df_to_csv(df)
        assert b"a" in result  # header present


# ===========================================================================
# dfs_to_excel
# ===========================================================================

class TestDfsToExcel:
    def test_returns_bytes(self):
        sheets = {"Sheet1": pd.DataFrame({"x": [1, 2]})}
        result = dfs_to_excel(sheets)
        assert isinstance(result, bytes)

    def test_valid_xlsx_file(self):
        sheets = {"Data": pd.DataFrame({"col": [1, 2, 3]})}
        result = dfs_to_excel(sheets)
        df = pd.read_excel(io.BytesIO(result), sheet_name="Data")
        assert list(df.columns) == ["col"]
        assert len(df) == 3

    def test_multiple_sheets(self):
        sheets = {
            "First": pd.DataFrame({"a": [1]}),
            "Second": pd.DataFrame({"b": [2]}),
        }
        result = dfs_to_excel(sheets)
        xls = pd.ExcelFile(io.BytesIO(result))
        assert set(xls.sheet_names) == {"First", "Second"}

    def test_long_sheet_name_truncated(self):
        long_name = "A" * 40  # Excel max is 31 chars
        sheets = {long_name: pd.DataFrame({"x": [1]})}
        result = dfs_to_excel(sheets)
        xls = pd.ExcelFile(io.BytesIO(result))
        assert len(xls.sheet_names[0]) == 31


# ===========================================================================
# _linear_forecast
# ===========================================================================

class TestLinearForecast:
    def test_returns_four_values(self):
        series = pd.Series([10, 20, 30, 40, 50], index=[
            "2024-01", "2024-02", "2024-03", "2024-04", "2024-05"
        ])
        fitted, forecast, resid_std, labels = _linear_forecast(series)
        assert fitted is not None
        assert forecast is not None
        assert resid_std is not None
        assert labels is not None

    def test_returns_none_if_too_few_points(self):
        series = pd.Series([10, 20, 30], index=["2024-01", "2024-02", "2024-03"])
        fitted, forecast, resid_std, labels = _linear_forecast(series)
        assert fitted is None
        assert forecast is None

    def test_forecast_length_matches_periods(self):
        series = pd.Series([10, 20, 30, 40], index=[
            "2024-01", "2024-02", "2024-03", "2024-04"
        ])
        _, forecast, _, labels = _linear_forecast(series, periods_ahead=5)
        assert len(forecast) == 5
        assert len(labels) == 5

    def test_linear_trend_forecasts_increase(self):
        series = pd.Series([100, 200, 300, 400, 500], index=[
            "2024-01", "2024-02", "2024-03", "2024-04", "2024-05"
        ])
        _, forecast, resid_std, _ = _linear_forecast(series)
        assert forecast[0] > 500
        assert resid_std == pytest.approx(0.0, abs=1.0)

    def test_handles_nan_values(self):
        series = pd.Series([10, np.nan, 30, 40, 50], index=[
            "2024-01", "2024-02", "2024-03", "2024-04", "2024-05"
        ])
        fitted, forecast, _, _ = _linear_forecast(series)
        assert fitted is not None  # 4 non-null values = enough

    def test_future_labels_are_yyyy_mm(self):
        series = pd.Series([10, 20, 30, 40], index=[
            "2024-01", "2024-02", "2024-03", "2024-04"
        ])
        _, _, _, labels = _linear_forecast(series, periods_ahead=2)
        assert labels[0] == "2024-05"
        assert labels[1] == "2024-06"
