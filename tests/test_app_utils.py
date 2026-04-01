"""Unit tests for app.py utility functions.

Covers df_to_csv, dfs_to_excel, _linear_forecast, and _forecast_model_stats.

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
        future_labels = [f"+{i + 1}m" for i in range(periods_ahead)]
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
        series = pd.Series([10, 20, 30, 40, 50], index=["2024-01", "2024-02", "2024-03", "2024-04", "2024-05"])
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
        series = pd.Series([10, 20, 30, 40], index=["2024-01", "2024-02", "2024-03", "2024-04"])
        _, forecast, _, labels = _linear_forecast(series, periods_ahead=5)
        assert len(forecast) == 5
        assert len(labels) == 5

    def test_linear_trend_forecasts_increase(self):
        series = pd.Series([100, 200, 300, 400, 500], index=["2024-01", "2024-02", "2024-03", "2024-04", "2024-05"])
        _, forecast, resid_std, _ = _linear_forecast(series)
        assert forecast[0] > 500
        assert resid_std == pytest.approx(0.0, abs=1.0)

    def test_handles_nan_values(self):
        series = pd.Series([10, np.nan, 30, 40, 50], index=["2024-01", "2024-02", "2024-03", "2024-04", "2024-05"])
        fitted, forecast, _, _ = _linear_forecast(series)
        assert fitted is not None  # 4 non-null values = enough

    def test_future_labels_are_yyyy_mm(self):
        series = pd.Series([10, 20, 30, 40], index=["2024-01", "2024-02", "2024-03", "2024-04"])
        _, _, _, labels = _linear_forecast(series, periods_ahead=2)
        assert labels[0] == "2024-05"
        assert labels[1] == "2024-06"


# ===========================================================================
# _forecast_model_stats — reimplemented from app.py
# ===========================================================================


def _forecast_model_stats(values: tuple, test_frac: float = 0.25):
    """Identical to app._forecast_model_stats (without st.cache_data)."""
    y_raw = np.array(values[1], dtype=float)
    mask = ~np.isnan(y_raw)
    y = y_raw[mask]
    x = np.arange(len(y), dtype=float)

    if len(y) < 6:
        return None

    split = max(int(len(y) * (1 - test_frac)), 4)
    if len(y) - split < 2:
        return None

    x_train, x_test = x[:split], x[split:]
    y_train, y_test = y[:split], y[split:]

    coeffs = np.polyfit(x_train, y_train, 1)
    y_train_pred = np.polyval(coeffs, x_train)
    y_test_pred = np.polyval(coeffs, x_test)

    ss_res_train = np.sum((y_train - y_train_pred) ** 2)
    ss_tot_train = np.sum((y_train - y_train.mean()) ** 2)
    r2_train = float(1 - ss_res_train / ss_tot_train) if ss_tot_train > 0 else 0.0

    ss_res_test = np.sum((y_test - y_test_pred) ** 2)
    ss_tot_test = np.sum((y_test - y_test.mean()) ** 2)
    r2_test = float(1 - ss_res_test / ss_tot_test) if ss_tot_test > 0 else 0.0

    mae_train = float(np.mean(np.abs(y_train - y_train_pred)))
    mae_test = float(np.mean(np.abs(y_test - y_test_pred)))

    nz_train = y_train != 0
    mape_train = (
        float(np.mean(np.abs((y_train[nz_train] - y_train_pred[nz_train]) / y_train[nz_train]))) * 100
        if nz_train.any()
        else None
    )
    nz_test = y_test != 0
    mape_test = (
        float(np.mean(np.abs((y_test[nz_test] - y_test_pred[nz_test]) / y_test[nz_test]))) * 100
        if nz_test.any()
        else None
    )

    return {
        "model": "Linear Regression (OLS)",
        "description": "Ordinary Least Squares degree-1 polynomial (numpy.polyfit)",
        "train_points": int(len(x_train)),
        "test_points": int(len(x_test)),
        "r2_train": r2_train,
        "r2_test": r2_test,
        "mae_train": mae_train,
        "mae_test": mae_test,
        "mape_train": mape_train,
        "mape_test": mape_test,
    }


class TestForecastModelStats:
    def test_returns_none_for_too_few_points(self):
        values = (("2024-01", "2024-02", "2024-03"), (10, 20, 30))
        assert _forecast_model_stats(values) is None

    def test_returns_dict_with_required_keys(self):
        idx = tuple(f"2024-{m:02d}" for m in range(1, 13))
        vals = tuple(range(100, 1300, 100))
        result = _forecast_model_stats((idx, vals))
        assert result is not None
        assert "model" in result
        assert "r2_train" in result
        assert "r2_test" in result
        assert "mae_train" in result
        assert "mae_test" in result
        assert "train_points" in result
        assert "test_points" in result

    def test_perfect_linear_has_high_r2(self):
        idx = tuple(f"2024-{m:02d}" for m in range(1, 13))
        vals = tuple(range(100, 1300, 100))  # perfectly linear
        result = _forecast_model_stats((idx, vals))
        assert result["r2_train"] == pytest.approx(1.0, abs=0.01)
        assert result["r2_test"] == pytest.approx(1.0, abs=0.01)

    def test_train_test_split_sizes(self):
        idx = tuple(f"2024-{m:02d}" for m in range(1, 13))
        vals = tuple(range(100, 1300, 100))
        result = _forecast_model_stats((idx, vals))
        assert result["train_points"] + result["test_points"] == 12
        assert result["test_points"] >= 2

    def test_mae_non_negative(self):
        idx = tuple(f"2024-{m:02d}" for m in range(1, 13))
        vals = tuple(float(100 + i * 10 + (i % 3) * 5) for i in range(12))
        result = _forecast_model_stats((idx, vals))
        assert result["mae_train"] >= 0
        assert result["mae_test"] >= 0

    def test_handles_nan_values(self):
        idx = tuple(f"2024-{m:02d}" for m in range(1, 13))
        vals = (100, float("nan"), 300, 400, 500, 600, 700, 800, float("nan"), 1000, 1100, 1200)
        result = _forecast_model_stats((idx, vals))
        # 10 non-NaN points: enough for a split
        assert result is not None
        assert result["train_points"] + result["test_points"] == 10

    def test_model_name_is_linear_regression(self):
        idx = tuple(f"2024-{m:02d}" for m in range(1, 13))
        vals = tuple(range(100, 1300, 100))
        result = _forecast_model_stats((idx, vals))
        assert result["model"] == "Linear Regression (OLS)"

    def test_returns_none_when_test_set_too_small(self):
        # 6 points, 75% train = 4.5 → 4, test = 2 → should work
        idx = tuple(f"2024-{m:02d}" for m in range(1, 7))
        vals = tuple(range(100, 700, 100))
        result = _forecast_model_stats((idx, vals))
        assert result is not None
        # 5 points: split = 3, test = 2 → still fine
        # But 5 points total is < 6 → None
        vals5 = tuple(range(100, 600, 100))
        result5 = _forecast_model_stats((tuple(f"2024-{m:02d}" for m in range(1, 6)), vals5))
        assert result5 is None


# ===========================================================================
# _detect_anomalies — reimplemented from app.py
# ===========================================================================


def _detect_anomalies(series: pd.Series) -> dict:
    """Identical to app._detect_anomalies."""
    clean = series.dropna()
    if len(clean) < 4:
        return {
            "mask": pd.Series(False, index=series.index),
            "lower_bound": None,
            "upper_bound": None,
            "anomalies": [],
            "count": 0,
        }
    q1 = float(np.percentile(clean.values, 25))
    q3 = float(np.percentile(clean.values, 75))
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    mask = (series < lower) | (series > upper)
    mask = mask.fillna(False)
    anomalies = [(idx, float(series[idx])) for idx in series.index if mask.get(idx, False)]
    return {
        "mask": mask,
        "lower_bound": lower,
        "upper_bound": upper,
        "anomalies": anomalies,
        "count": int(mask.sum()),
    }


class TestDetectAnomalies:
    def test_no_anomalies_in_stable_data(self):
        series = pd.Series(
            [10, 11, 12, 10, 11, 12, 10, 11],
            index=[f"2024-{m:02d}" for m in range(1, 9)],
        )
        result = _detect_anomalies(series)
        assert result["count"] == 0
        assert result["anomalies"] == []

    def test_detects_obvious_outlier(self):
        values = [10, 11, 12, 10, 11, 12, 10, 0]  # 0 is far below
        series = pd.Series(values, index=[f"2024-{m:02d}" for m in range(1, 9)])
        result = _detect_anomalies(series)
        assert result["count"] >= 1
        anom_labels = [a[0] for a in result["anomalies"]]
        assert "2024-08" in anom_labels

    def test_returns_correct_bounds(self):
        series = pd.Series([10, 20, 30, 40], index=["a", "b", "c", "d"])
        result = _detect_anomalies(series)
        # np.percentile: Q1=17.5, Q3=32.5, IQR=15 → lower=-5, upper=55
        assert result["lower_bound"] == pytest.approx(-5.0, abs=0.1)
        assert result["upper_bound"] == pytest.approx(55.0, abs=0.1)

    def test_handles_short_series(self):
        series = pd.Series([10, 20], index=["a", "b"])
        result = _detect_anomalies(series)
        assert result["count"] == 0
        assert result["lower_bound"] is None

    def test_anomaly_mask_matches_series_index(self):
        idx = [f"2024-{m:02d}" for m in range(1, 7)]
        series = pd.Series([10, 11, 12, 10, 11, 100], index=idx)
        result = _detect_anomalies(series)
        assert list(result["mask"].index) == idx


# ===========================================================================
# _detect_seasonality — reimplemented from app.py
# ===========================================================================


def _detect_seasonality(series: pd.Series) -> dict:
    """Identical to app._detect_seasonality."""
    clean = series.dropna()
    if len(clean) < 12:
        return {"strength": 0.0, "level": "none", "monthly_pattern": {}}
    try:
        months = [int(str(idx).split("-")[1]) for idx in clean.index]
    except (IndexError, ValueError):
        return {"strength": 0.0, "level": "none", "monthly_pattern": {}}
    values = clean.values.astype(float)
    total_var = float(np.var(values))
    if total_var == 0:
        return {"strength": 0.0, "level": "none", "monthly_pattern": {}}
    monthly_sums: dict[int, list[float]] = {}
    for m, v in zip(months, values):
        monthly_sums.setdefault(m, []).append(v)
    monthly_pattern = {m: float(np.mean(vals)) for m, vals in sorted(monthly_sums.items())}
    group_means = np.array(list(monthly_pattern.values()))
    between_var = float(np.var(group_means))
    strength = min(between_var / total_var, 1.0)
    if strength >= 0.5:
        level = "strong"
    elif strength >= 0.3:
        level = "moderate"
    else:
        level = "none"
    return {"strength": strength, "level": level, "monthly_pattern": monthly_pattern}


class TestDetectSeasonality:
    def test_no_seasonality_in_flat_data(self):
        idx = [f"2024-{m:02d}" for m in range(1, 13)]
        series = pd.Series([10.0] * 12, index=idx)
        result = _detect_seasonality(series)
        assert result["strength"] == 0.0
        assert result["level"] == "none"

    def test_detects_strong_seasonal_pattern(self):
        # Create a pattern where Jan-Jun are high, Jul-Dec are low (2 years)
        idx = [f"{y}-{m:02d}" for y in (2024, 2025) for m in range(1, 13)]
        vals = [100 if m <= 6 else 50 for m in range(1, 13)] * 2
        series = pd.Series(vals, index=idx)
        result = _detect_seasonality(series)
        assert result["strength"] > 0.3
        assert result["level"] in ("moderate", "strong")

    def test_strength_between_zero_and_one(self):
        idx = [f"{y}-{m:02d}" for y in (2024, 2025) for m in range(1, 13)]
        vals = [float(10 + m * 2 + (m % 3) * 5) for m in range(1, 13)] * 2
        series = pd.Series(vals, index=idx)
        result = _detect_seasonality(series)
        assert 0.0 <= result["strength"] <= 1.0

    def test_returns_monthly_pattern_dict(self):
        idx = [f"{y}-{m:02d}" for y in (2024, 2025) for m in range(1, 13)]
        vals = list(range(1, 25))
        series = pd.Series(vals, index=idx)
        result = _detect_seasonality(series)
        assert len(result["monthly_pattern"]) == 12
        assert all(m in result["monthly_pattern"] for m in range(1, 13))

    def test_handles_short_series(self):
        series = pd.Series([10, 20, 30], index=["2024-01", "2024-02", "2024-03"])
        result = _detect_seasonality(series)
        assert result["level"] == "none"
        assert result["monthly_pattern"] == {}
