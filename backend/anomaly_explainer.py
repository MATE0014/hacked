"""Anomaly detection and LLM-based explanations."""
from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List

import pandas as pd


def _severity_from_value(value: float, low: float, high: float, iqr: float) -> str:
    if iqr <= 0:
        return "low"
    if value > high:
        distance = (value - high) / iqr
    else:
        distance = (low - value) / iqr

    if distance >= 3:
        return "high"
    if distance >= 1.5:
        return "medium"
    return "low"


def _llm_explain(col: str, value: Any, low: float, high: float, key_manager: Any) -> Dict[str, Any]:
    prompt = (
        f"Column '{col}' has an outlier at {value} (normal range: {low}-{high}). "
        "Give 2 possible real-world causes and say if this is likely a data error or genuine signal. "
        "One sentence each."
    )

    try:
        text = key_manager.call_with_retry(
            system_prompt="You explain business data anomalies clearly and briefly.",
            user_prompt=prompt,
        )
    except Exception:
        text = (
            "Possible causes include genuine operational variation or collection issues. "
            "Likely classification: needs manual validation."
        )

    lowered = text.lower()
    is_likely_error = "data error" in lowered or "entry error" in lowered or "measurement error" in lowered
    return {
        "explanation": text,
        "is_likely_error": is_likely_error,
    }


def _anomaly_limits() -> Dict[str, int]:
    max_per_column_raw = (os.getenv("ANOMALY_MAX_PER_COLUMN") or "3").strip()
    max_total_raw = (os.getenv("ANOMALY_MAX_TOTAL") or "20").strip()
    workers_raw = (os.getenv("ANOMALY_LLM_WORKERS") or "4").strip()

    try:
        max_per_column = max(1, int(max_per_column_raw))
    except ValueError:
        max_per_column = 3

    try:
        max_total = max(1, int(max_total_raw))
    except ValueError:
        max_total = 20

    try:
        workers = max(1, int(workers_raw))
    except ValueError:
        workers = 4

    return {
        "max_per_column": max_per_column,
        "max_total": max_total,
        "workers": workers,
    }


def _severity_score(value: float, low: float, high: float, iqr: float) -> float:
    if iqr <= 0:
        return 0.0
    if value > high:
        return max(0.0, (value - high) / iqr)
    return max(0.0, (low - value) / iqr)


def explain_anomalies(dataframe: pd.DataFrame, key_manager: Any) -> List[Dict[str, Any]]:
    """Detect outliers and spikes and return human-readable anomaly explanations."""
    anomalies: List[Dict[str, Any]] = []
    candidates: List[Dict[str, Any]] = []
    limits = _anomaly_limits()
    max_per_column = limits["max_per_column"]
    max_total = limits["max_total"]

    numeric_cols = dataframe.select_dtypes(include=["number"]).columns.tolist()
    for col in numeric_cols:
        series = pd.to_numeric(dataframe[col], errors="coerce").dropna()
        if series.empty:
            continue

        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        if iqr <= 0:
            continue

        low = q1 - 1.5 * iqr
        high = q3 + 1.5 * iqr
        outliers = series[(series < low) | (series > high)]
        ranked = sorted(
            outliers.tolist(),
            key=lambda v: _severity_score(float(v), float(low), float(high), float(iqr)),
            reverse=True,
        )[:max_per_column]

        for value in ranked:
            candidates.append(
                {
                    "column": col,
                    "value": float(value),
                    "low": float(low),
                    "high": float(high),
                    "iqr": float(iqr),
                    "score": _severity_score(float(value), float(low), float(high), float(iqr)),
                    "prefix": "",
                }
            )

    datetime_cols = dataframe.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns.tolist()
    for dt_col in datetime_cols:
        ordered = dataframe[[dt_col] + numeric_cols].dropna(subset=[dt_col]).sort_values(dt_col)
        if ordered.empty:
            continue

        for num_col in numeric_cols:
            series = pd.to_numeric(ordered[num_col], errors="coerce")
            rolling_mean = series.rolling(window=10, min_periods=5).mean()
            rolling_std = series.rolling(window=10, min_periods=5).std().replace(0, pd.NA)
            zscores = ((series - rolling_mean) / rolling_std).abs()
            spike_idx = zscores[zscores > 3].dropna().index[:max_per_column]

            for idx in spike_idx:
                val = series.loc[idx]
                if pd.isna(val):
                    continue
                local_mean = rolling_mean.loc[idx]
                local_std = rolling_std.loc[idx]
                if pd.isna(local_mean) or pd.isna(local_std):
                    continue
                low = float(local_mean - 3 * local_std)
                high = float(local_mean + 3 * local_std)

                candidates.append(
                    {
                        "column": num_col,
                        "value": float(val),
                        "low": low,
                        "high": high,
                        "iqr": float(max(1e-9, local_std)),
                        "score": 999.0,
                        "prefix": f"Spike near {dt_col}: ",
                    }
                )

    if not candidates:
        return anomalies

    selected = sorted(candidates, key=lambda item: item["score"], reverse=True)[:max_total]
    workers = min(limits["workers"], len(selected))

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(
                _llm_explain,
                item["column"],
                item["value"],
                item["low"],
                item["high"],
                key_manager,
            ): item
            for item in selected
        }

        for future in as_completed(future_map):
            item = future_map[future]
            try:
                explanation = future.result()
            except Exception:
                explanation = {
                    "explanation": "Potentially unusual behavior; validate against business context.",
                    "is_likely_error": False,
                }

            severity = _severity_from_value(item["value"], item["low"], item["high"], item["iqr"])
            if item["prefix"]:
                severity = "high"

            anomalies.append(
                {
                    "column": item["column"],
                    "value": float(item["value"]),
                    "severity": severity,
                    "explanation": f"{item['prefix']}{explanation['explanation']}",
                    "is_likely_error": explanation["is_likely_error"],
                }
            )

    anomalies.sort(key=lambda a: {"high": 3, "medium": 2, "low": 1}.get(a["severity"], 0), reverse=True)

    return anomalies
