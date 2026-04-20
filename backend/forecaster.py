"""Time series forecasting and LLM-based interpretation."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List

import pandas as pd


def _is_id_like_column_name(column_name: str) -> bool:
    normalized = (column_name or "").strip().lower()
    if not normalized:
        return False

    id_tokens = {
        "id",
        "uuid",
        "guid",
        "key",
        "code",
        "identifier",
        "record",
        "machine",
    }
    parts = [part for part in normalized.replace("-", "_").split("_") if part]

    if normalized.endswith("_id") or normalized == "id":
        return True
    if any(part in id_tokens for part in parts):
        return True
    return False


def _to_jsonable(value: Any) -> Any:
    """Convert pandas/numpy-like objects into JSON-safe primitives."""
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(v) for v in value]

    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    return value


def _infer_value_bounds(series: pd.Series, value_col: str = "") -> tuple[Any, Any]:
    """Infer sensible lower/upper bounds for bounded metrics."""
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None, None

    lower = None
    upper = None
    min_v = float(numeric.min())
    max_v = float(numeric.max())
    name = (value_col or "").strip().lower()

    # Ratings are typically bounded in [0, 5].
    if any(token in name for token in ("rating", "stars", "score")) and min_v >= 0 and max_v <= 5.5:
        lower, upper = 0.0, 5.0
    # Percent-like / probability-like metrics are bounded.
    elif any(token in name for token in ("pct", "percent", "percentage", "probability", "ratio", "confidence", "rate")) and min_v >= 0:
        if max_v <= 1.2:
            lower, upper = 0.0, 1.0
        elif max_v <= 100.5:
            lower, upper = 0.0, 100.0

    # For naturally non-negative metrics, avoid negative forecasts.
    if lower is None and min_v >= 0:
        lower = 0.0

    return lower, upper


def _clamp_values(values: list[float], lower: Any, upper: Any) -> list[float]:
    """Clamp forecast outputs to inferred bounds when available."""
    clamped = []
    for value in values:
        v = float(value)
        if lower is not None:
            v = max(v, float(lower))
        if upper is not None:
            v = min(v, float(upper))
        clamped.append(round(v, 4))
    return clamped


def detect_timeseries_columns(
    dataframe: pd.DataFrame,
    structure: dict,
) -> list:
    """
    Return list of dicts describing valid time series pairs.
    Each dict: { "date_col": str, "value_col": str, "n_points": int }

    Uses structure dict already produced by analyzer.detect_structure()
    so we don't re-scan dtypes from scratch.
    Max 3 pairs to avoid rate limit overuse.
    """
    datetime_cols = list(structure.get("datetime_columns", []))
    numeric_cols = structure.get("numeric_columns", [])
    id_like_cols = {
        col
        for col, pattern in (structure.get("patterns", {}) or {}).items()
        if pattern == "potential_id_column"
    }

    if not datetime_cols:
        # Fallback: infer datetime-like columns from non-numeric columns when
        # upstream structure detection misses timezone-aware or mixed-format timestamps.
        candidate_cols = [
            col for col in dataframe.columns
            if col not in numeric_cols
        ]
        for col in candidate_cols[:8]:
            parsed = pd.to_datetime(dataframe[col], errors="coerce", format="mixed")
            parse_ratio = float(parsed.notna().mean()) if len(parsed) else 0.0
            if parse_ratio >= 0.8:
                datetime_cols.append(col)

    if not datetime_cols or not numeric_cols:
        return []

    pairs = []
    for dt_col in datetime_cols[:2]:
        if dt_col in id_like_cols or dt_col in numeric_cols or _is_id_like_column_name(str(dt_col)):
            continue

        for val_col in numeric_cols[:4]:
            if len(pairs) >= 3:
                break

            pair_df = dataframe[[dt_col, val_col]].copy()
            pair_df[dt_col] = pd.to_datetime(pair_df[dt_col], errors="coerce")
            pair_df[val_col] = pd.to_numeric(pair_df[val_col], errors="coerce")
            pair_df = pair_df.dropna(subset=[dt_col, val_col])

            if len(pair_df) < 20:
                continue
            if pair_df[dt_col].nunique() < 20:
                continue

            year_median = float(pair_df[dt_col].dt.year.median())
            if year_median < 1990 or year_median > 2100:
                continue

            # Guard against numeric ID columns coerced to nanosecond timestamps.
            # Real forecasting timelines should span multiple calendar days.
            if pair_df[dt_col].dt.normalize().nunique() < 3:
                continue

            pairs.append(
                {
                    "date_col": str(dt_col),
                    "value_col": str(val_col),
                    "n_points": int(len(pair_df)),
                }
            )

    return pairs


def _prepare_series(
    dataframe: pd.DataFrame,
    date_col: str,
    value_col: str,
) -> pd.Series:
    """
    Build a clean, sorted, datetime-indexed Series for forecasting.
    - Drops NaN in either column
    - Sorts by date ascending
    - Removes duplicate timestamps by averaging
    - Infers frequency (daily/weekly/monthly) by median gap
    - Returns a Series with DatetimeIndex and inferred freq set
    """
    df = dataframe[[date_col, value_col]].copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna().sort_values(date_col)

    series = df.groupby(date_col)[value_col].mean()

    if len(series) >= 3:
        gaps = series.index.to_series().diff().dropna()
        median_gap = gaps.median()
        total_seconds = median_gap.total_seconds()

        # Avoid collapsing pseudo-time data (e.g., integer IDs cast to timestamps)
        # where the median gap is sub-day and daily resampling would destroy signal.
        if total_seconds < 86400:
            return series

        if total_seconds <= 86400 * 2:
            freq = "D"
        elif total_seconds <= 86400 * 10:
            freq = "W"
        elif total_seconds <= 86400 * 45:
            freq = "MS"
        else:
            freq = "QS"

        try:
            series = series.asfreq(freq, method="ffill")
        except Exception:
            pass

    return series


def _run_forecast(series: pd.Series, n_periods: int, value_col: str = "") -> dict:
    """
    Run ExponentialSmoothing (Holt-Winters) forecast.
    Falls back to simple linear trend if statsmodels fails.

    Returns:
    {
        "method": "holt_winters" | "linear_trend" | "failed",
        "forecast_values": [float, ...],
        "forecast_dates": [str, ...],
        "confidence_low": [float, ...],
        "confidence_high": [float, ...],
        "trend_direction": "up" | "down" | "stable",
        "trend_pct_change": float,
        "last_real_value": float,
        "last_real_date": str,
    }
    """
    import numpy as np

    if series is None or len(series) < 3:
        return {
            "method": "failed",
            "forecast_values": [],
            "forecast_dates": [],
            "confidence_low": [],
            "confidence_high": [],
            "trend_direction": "stable",
            "trend_pct_change": 0.0,
            "last_real_value": 0.0,
            "last_real_date": "",
        }

    last_real_value = float(series.iloc[-1])
    last_real_date = str(series.index[-1].date())
    lower_bound, upper_bound = _infer_value_bounds(series, value_col)

    holt_error = ""
    try:
        from statsmodels.tsa.holtwinters import ExponentialSmoothing

        seasonal_periods = None
        seasonal = None
        if len(series) >= 24:
            seasonal_periods = 12
            seasonal = "add"
        elif len(series) >= 8:
            seasonal_periods = 4
            seasonal = "add"

        model = ExponentialSmoothing(
            series,
            trend="add",
            seasonal=seasonal,
            seasonal_periods=seasonal_periods,
            initialization_method="estimated",
        ).fit(optimized=True, disp=False)

        forecast = model.forecast(n_periods)

        residuals = model.resid
        resid_std = float(np.std(residuals))
        margin = 1.28 * resid_std

        forecast_values = _clamp_values([float(v) for v in forecast], lower_bound, upper_bound)
        confidence_low = _clamp_values([float(v) - margin for v in forecast], lower_bound, upper_bound)
        confidence_high = _clamp_values([float(v) + margin for v in forecast], lower_bound, upper_bound)

        last_idx = series.index[-1]
        freq = series.index.freq or pd.tseries.frequencies.to_offset("D")
        future_dates = [
            str((last_idx + freq * (i + 1)).date())
            for i in range(n_periods)
        ]

        last_forecast = forecast_values[-1]
        pct_change = ((last_forecast - last_real_value) / max(abs(last_real_value), 1e-9)) * 100

        if pct_change > 3:
            trend_direction = "up"
        elif pct_change < -3:
            trend_direction = "down"
        else:
            trend_direction = "stable"

        return {
            "method": "holt_winters",
            "forecast_values": forecast_values,
            "forecast_dates": future_dates,
            "confidence_low": confidence_low,
            "confidence_high": confidence_high,
            "trend_direction": trend_direction,
            "trend_pct_change": round(pct_change, 2),
            "last_real_value": last_real_value,
            "last_real_date": last_real_date,
        }
    except Exception as exc:
        holt_error = str(exc)

    try:
        x = np.arange(len(series))
        y = series.values.astype(float)
        valid = ~np.isnan(y)
        if valid.sum() < 3:
            raise ValueError("Not enough valid points")

        coeffs = np.polyfit(x[valid], y[valid], deg=1)
        slope, intercept = coeffs

        future_x = np.arange(len(series), len(series) + n_periods)
        forecast_values = _clamp_values([float(slope * xi + intercept) for xi in future_x], lower_bound, upper_bound)

        fitted = np.polyval(coeffs, x[valid])
        resid_std = float(np.std(y[valid] - fitted))
        margin = 1.28 * resid_std

        confidence_low = _clamp_values([v - margin for v in forecast_values], lower_bound, upper_bound)
        confidence_high = _clamp_values([v + margin for v in forecast_values], lower_bound, upper_bound)

        last_idx = series.index[-1]
        try:
            freq = series.index.freq or pd.tseries.frequencies.to_offset("D")
            future_dates = [
                str((last_idx + freq * (i + 1)).date())
                for i in range(n_periods)
            ]
        except Exception:
            future_dates = [f"period_{i + 1}" for i in range(n_periods)]

        last_forecast = forecast_values[-1]
        pct_change = ((last_forecast - last_real_value) / max(abs(last_real_value), 1e-9)) * 100

        return {
            "method": "linear_trend",
            "forecast_values": forecast_values,
            "forecast_dates": future_dates,
            "confidence_low": confidence_low,
            "confidence_high": confidence_high,
            "trend_direction": "up" if pct_change > 3 else ("down" if pct_change < -3 else "stable"),
            "trend_pct_change": round(pct_change, 2),
            "last_real_value": last_real_value,
            "last_real_date": last_real_date,
        }
    except Exception as exc:
        return {
            "method": "failed",
            "forecast_values": [],
            "forecast_dates": [],
            "confidence_low": [],
            "confidence_high": [],
            "trend_direction": "stable",
            "trend_pct_change": 0.0,
            "last_real_value": last_real_value,
            "last_real_date": last_real_date,
            "error": f"Holt-Winters error: {holt_error}; Linear fallback error: {str(exc)}",
        }


def _llm_interpret_forecast(
    pair: dict,
    forecast_result: dict,
    key_manager: Any,
) -> str:
    """
    Ask the LLM to interpret the forecast result in plain English.
    One paragraph, non-technical, actionable.
    Follows same pattern as _llm_explain() in anomaly_explainer.py.
    """
    direction = forecast_result["trend_direction"]
    pct = forecast_result["trend_pct_change"]
    method = forecast_result["method"]
    last_val = forecast_result["last_real_value"]
    last_date = forecast_result["last_real_date"]
    n = len(forecast_result["forecast_values"])

    prompt = (
        f"Column '{pair['value_col']}' (tracked over '{pair['date_col']}') "
        f"currently sits at {last_val:.2f} as of {last_date}. "
        f"A {method} forecast for the next {n} periods shows a {direction} trend "
        f"with {abs(pct):.1f}% {'increase' if pct > 0 else 'decrease'}. "
        f"Forecast values: {forecast_result['forecast_values'][:5]}. "
        "In 2-3 sentences, explain what this means for a non-technical business user "
        "and suggest one concrete action they should consider."
    )

    try:
        return key_manager.call_with_retry(
            system_prompt=(
                "You are a business analyst explaining data forecasts clearly. "
                "Be concise, direct, and actionable. No jargon."
            ),
            user_prompt=prompt,
            purpose="general",
        )
    except Exception:
        direction_text = {
            "up": f"trending upward by ~{abs(pct):.1f}%",
            "down": f"trending downward by ~{abs(pct):.1f}%",
            "stable": "remaining relatively stable",
        }.get(direction, "showing mixed signals")
        return (
            f"'{pair['value_col']}' is {direction_text} over the next {n} periods. "
            "Consider monitoring this column closely and comparing against targets."
        )


def forecast_timeseries(
    dataframe: pd.DataFrame,
    structure: dict,
    key_manager: Any,
    n_periods: int = 10,
) -> dict:
    """
    Entry point called from main.py.
    Detects time series columns, runs forecasting, interprets results with LLM.
    Matches pattern of explain_anomalies() in anomaly_explainer.py.

    Returns:
    {
        "timeseries_found": int,
        "forecasts": [
            {
                "date_col": str,
                "value_col": str,
                "n_points": int,
                "n_periods_forecast": int,
                "method": str,
                "trend_direction": "up"|"down"|"stable",
                "trend_pct_change": float,
                "last_real_value": float,
                "last_real_date": str,
                "forecast_values": [float],
                "forecast_dates": [str],
                "confidence_low": [float],
                "confidence_high": [float],
                "historical_values": [float],
                "historical_dates": [str],
                "interpretation": str,
            },
            ...
        ],
        "message": str
    }
    """
    pairs = detect_timeseries_columns(dataframe, structure)

    if not pairs:
        return {
            "timeseries_found": 0,
            "forecasts": [],
            "message": "No time series columns detected in this dataset.",
        }

    forecast_results: Dict[str, Any] = {}
    for pair in pairs:
        try:
            series = _prepare_series(dataframe, pair["date_col"], pair["value_col"])
            result = _run_forecast(series, n_periods, pair["value_col"])

            hist_tail = series.tail(30)
            result["historical_values"] = [round(float(v), 4) for v in hist_tail.values]
            result["historical_dates"] = [str(d.date()) for d in hist_tail.index]

            forecast_results[pair["value_col"]] = (pair, result)
        except Exception as e:
            forecast_results[pair["value_col"]] = (
                pair,
                {
                    "method": "failed",
                    "forecast_values": [],
                    "forecast_dates": [],
                    "confidence_low": [],
                    "confidence_high": [],
                    "trend_direction": "stable",
                    "trend_pct_change": 0.0,
                    "last_real_value": 0.0,
                    "last_real_date": "",
                    "historical_values": [],
                    "historical_dates": [],
                    "error": str(e),
                },
            )

    workers = min(len(forecast_results), 3)
    forecasts: List[Dict[str, Any]] = []

    llm_inputs = {
        value_col: (pair, result)
        for value_col, (pair, result) in forecast_results.items()
        if result.get("method") != "failed" and result.get("forecast_values")
    }

    if llm_inputs and workers > 0:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_map = {
                executor.submit(_llm_interpret_forecast, pair, result, key_manager): value_col
                for value_col, (pair, result) in llm_inputs.items()
            }

            interpretations: Dict[str, str] = {}
            for future in as_completed(future_map):
                value_col = future_map[future]
                pair, _ = llm_inputs[value_col]
                try:
                    interpretations[value_col] = future.result()
                except Exception:
                    interpretations[value_col] = (
                        f"'{pair['value_col']}' forecast completed. "
                        "Review the chart for trend details."
                    )
    else:
        interpretations = {}

    for value_col, (pair, result) in forecast_results.items():
        interpretation = interpretations.get(
            value_col,
            (
                f"'{pair['value_col']}' forecast completed. "
                "Review the chart for trend details."
            ),
        )

        forecasts.append(
            {
                "date_col": pair["date_col"],
                "value_col": pair["value_col"],
                "n_points": pair["n_points"],
                "n_periods_forecast": n_periods,
                "method": result.get("method", "failed"),
                "trend_direction": result.get("trend_direction", "stable"),
                "trend_pct_change": result.get("trend_pct_change", 0.0),
                "last_real_value": result.get("last_real_value", 0.0),
                "last_real_date": result.get("last_real_date", ""),
                "forecast_values": result.get("forecast_values", []),
                "forecast_dates": result.get("forecast_dates", []),
                "confidence_low": result.get("confidence_low", []),
                "confidence_high": result.get("confidence_high", []),
                "historical_values": result.get("historical_values", []),
                "historical_dates": result.get("historical_dates", []),
                "interpretation": interpretation,
            }
        )

    priority = {"up": 2, "down": 2, "stable": 1}
    forecasts.sort(
        key=lambda f: priority.get(f["trend_direction"], 0),
        reverse=True,
    )

    return _to_jsonable(
        {
            "timeseries_found": len(pairs),
            "forecasts": forecasts,
            "message": f"Forecast generated for {len(forecasts)} time series column(s).",
        }
    )
