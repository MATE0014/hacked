from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import numpy as np
import pandas as pd


def _check_constant_columns(dataframe: pd.DataFrame) -> list:
    """
    Returns list of issues for constant/near-constant columns.
    Each issue dict:
    {
        "check": "constant_column",
        "column": str,
        "severity": "high" | "medium",
        "detail": {
            "unique_values": int,
            "dominant_value": str,
            "dominant_pct": float,
        }
    }
    Severity: high if truly constant (1 unique), medium if near-constant.
    Skip columns where all values are null (those are caught by missing value
    analysis in DataProcessor already).
    """
    try:
        issues = []

        for column in dataframe.columns:
            series = dataframe[column]
            non_null = series.dropna()
            if non_null.empty:
                continue

            unique_values = int(non_null.nunique(dropna=True))
            value_counts = non_null.value_counts(dropna=True)
            if value_counts.empty:
                continue

            dominant_value = value_counts.index[0]
            dominant_count = int(value_counts.iloc[0])
            dominant_ratio = dominant_count / max(1, int(len(non_null)))
            dominant_pct = float(dominant_ratio * 100.0)
            if not np.isfinite(dominant_pct):
                dominant_pct = 0.0

            if unique_values == 1 or dominant_ratio > 0.95:
                issues.append(
                    {
                        "check": "constant_column",
                        "column": str(column),
                        "severity": "high" if unique_values == 1 else "medium",
                        "detail": {
                            "unique_values": int(unique_values),
                            "dominant_value": str(dominant_value),
                            "dominant_pct": float(round(dominant_pct, 2)),
                        },
                    }
                )

        return issues
    except Exception:
        return []


def _check_duplicate_columns(dataframe: pd.DataFrame) -> list:
    """
    Returns list of issues for suspected duplicate columns.
    Each issue dict:
    {
        "check": "duplicate_column",
        "column": str,           # the suspected duplicate (second of the pair)
        "severity": "high",
        "detail": {
            "duplicate_of": str,        # the original column name
            "similarity_score": float,  # 0.0-1.0
            "similarity_method": "correlation" | "value_overlap",
        }
    }
    Only flag each column once - if A duplicates B, report A as duplicate of B,
    not both ways. Max 5 pairs to avoid O(n^2) blowup on wide datasets.
    Skip ID-like columns (unique ratio > 0.9).
    """
    try:
        issues = []
        flagged_duplicates = set()

        columns = [str(c) for c in dataframe.columns]
        eligible_columns = []

        for column in columns:
            series = dataframe[column].dropna()
            if series.empty:
                continue
            unique_ratio = float(series.nunique(dropna=True) / max(1, len(series)))
            if unique_ratio > 0.9:
                continue
            eligible_columns.append(column)

        max_pairs = 5
        for i in range(len(eligible_columns)):
            if len(issues) >= max_pairs:
                break

            col_a = eligible_columns[i]
            if col_a in flagged_duplicates:
                continue

            for j in range(i + 1, len(eligible_columns)):
                if len(issues) >= max_pairs:
                    break

                col_b = eligible_columns[j]
                if col_b in flagged_duplicates:
                    continue

                series_a = dataframe[col_a]
                series_b = dataframe[col_b]

                is_num_a = pd.api.types.is_numeric_dtype(series_a)
                is_num_b = pd.api.types.is_numeric_dtype(series_b)

                if is_num_a and is_num_b:
                    paired = pd.DataFrame({"a": pd.to_numeric(series_a, errors="coerce"), "b": pd.to_numeric(series_b, errors="coerce")}).dropna()
                    if len(paired) < 5:
                        continue

                    corr = paired["a"].corr(paired["b"])
                    if pd.isna(corr):
                        continue

                    similarity = float(abs(float(corr)))
                    if similarity > 0.98:
                        issues.append(
                            {
                                "check": "duplicate_column",
                                "column": str(col_b),
                                "severity": "high",
                                "detail": {
                                    "duplicate_of": str(col_a),
                                    "similarity_score": float(round(similarity, 4)),
                                    "similarity_method": "correlation",
                                },
                            }
                        )
                        flagged_duplicates.add(col_b)
                        continue

                if (not is_num_a) and (not is_num_b):
                    norm_a = set(
                        str(v).strip().lower()
                        for v in series_a.dropna().tolist()
                        if str(v).strip() != ""
                    )
                    norm_b = set(
                        str(v).strip().lower()
                        for v in series_b.dropna().tolist()
                        if str(v).strip() != ""
                    )

                    if not norm_a or not norm_b:
                        continue

                    overlap = len(norm_a.intersection(norm_b)) / max(1, min(len(norm_a), len(norm_b)))
                    similarity = float(overlap)
                    if similarity > 0.95:
                        issues.append(
                            {
                                "check": "duplicate_column",
                                "column": str(col_b),
                                "severity": "high",
                                "detail": {
                                    "duplicate_of": str(col_a),
                                    "similarity_score": float(round(similarity, 4)),
                                    "similarity_method": "value_overlap",
                                },
                            }
                        )
                        flagged_duplicates.add(col_b)

        return issues
    except Exception:
        return []


def _check_class_imbalance(dataframe: pd.DataFrame) -> list:
    """
    Returns list of issues for imbalanced target-like columns.
    Each issue dict:
    {
        "check": "class_imbalance",
        "column": str,
        "severity": "high" | "medium",
        "detail": {
            "majority_class": str,
            "majority_pct": float,
            "minority_class": str,
            "minority_pct": float,
            "imbalance_ratio": float,
            "class_distribution": dict,   # top 10 classes with counts
        }
    }
    Severity: high if ratio > 10, medium if ratio > 4.
    """
    try:
        issues = []
        target_keywords = [
            "target",
            "label",
            "class",
            "category",
            "outcome",
            "status",
            "type",
            "flag",
            "result",
            "grade",
        ]

        for column in dataframe.columns:
            column_name = str(column)
            lowered_name = column_name.lower()
            if not any(k in lowered_name for k in target_keywords):
                continue

            series = dataframe[column].dropna()
            if series.empty:
                continue

            unique_count = int(series.nunique(dropna=True))
            if unique_count < 2 or unique_count > 20:
                continue

            is_object_like = pd.api.types.is_object_dtype(series) or pd.api.types.is_categorical_dtype(series)
            is_small_int = pd.api.types.is_integer_dtype(series) and unique_count <= 10
            if not (is_object_like or is_small_int):
                continue

            distribution = series.value_counts(dropna=True)
            if len(distribution) < 2:
                continue

            majority_class = distribution.index[0]
            minority_class = distribution.index[-1]
            majority_count = int(distribution.iloc[0])
            minority_count = int(distribution.iloc[-1])
            if minority_count <= 0:
                continue

            total = int(distribution.sum())
            majority_pct = float((majority_count / max(1, total)) * 100.0)
            minority_pct = float((minority_count / max(1, total)) * 100.0)
            imbalance_ratio = float(majority_count / minority_count)

            if imbalance_ratio > 10.0 or imbalance_ratio > 4.0:
                severity = "high" if imbalance_ratio > 10.0 else "medium"
                top_dist = distribution.head(10)
                class_distribution = {str(k): int(v) for k, v in top_dist.items()}

                issues.append(
                    {
                        "check": "class_imbalance",
                        "column": column_name,
                        "severity": severity,
                        "detail": {
                            "majority_class": str(majority_class),
                            "majority_pct": float(round(majority_pct, 2)),
                            "minority_class": str(minority_class),
                            "minority_pct": float(round(minority_pct, 2)),
                            "imbalance_ratio": float(round(imbalance_ratio, 4)),
                            "class_distribution": class_distribution,
                        },
                    }
                )

        return issues
    except Exception:
        return []


def _check_wrong_dtypes(dataframe: pd.DataFrame) -> list:
    """
    Returns list of issues for suspicious dtype mismatches.
    Each issue dict:
    {
        "check": "wrong_dtype",
        "column": str,
        "severity": "medium" | "low",
        "detail": {
            "current_dtype": str,
            "suggested_dtype": str,
            "reason": str,
            "sample_values": list,    # first 3 non-null values as strings
        }
    }
    Max 1 issue per column - pick the most severe reason if multiple apply.
    Severity: medium if it will affect analysis correctness, low if cosmetic.
    """
    try:
        issues = []
        name_markers = ["id", "code", "zip", "phone", "year"]

        for column in dataframe.columns:
            series = dataframe[column]
            non_null = series.dropna()
            if non_null.empty:
                continue

            sample_values = [str(v) for v in non_null.head(3).tolist()]
            current_dtype = str(series.dtype)
            column_name = str(column)
            lowered_name = column_name.lower()

            candidates = []

            if any(marker in lowered_name for marker in name_markers) and pd.api.types.is_float_dtype(series):
                suggested_dtype = "integer" if "year" in lowered_name else "string"
                candidates.append(
                    {
                        "severity": "medium",
                        "suggested_dtype": suggested_dtype,
                        "reason": "Column name suggests identifier-like data but dtype is float",
                    }
                )

            if pd.api.types.is_object_dtype(series):
                as_text = non_null.astype(str).str.strip()
                non_empty = as_text[as_text != ""]

                if not non_empty.empty:
                    numeric_parse_ratio = float(pd.to_numeric(non_empty, errors="coerce").notna().mean())
                    if numeric_parse_ratio > 0.8:
                        candidates.append(
                            {
                                "severity": "medium",
                                "suggested_dtype": "numeric",
                                "reason": ">80% of values are parseable as numbers",
                            }
                        )

                    date_parse_ratio = float(pd.to_datetime(non_empty, errors="coerce").notna().mean())
                    if date_parse_ratio > 0.8:
                        candidates.append(
                            {
                                "severity": "medium",
                                "suggested_dtype": "datetime",
                                "reason": ">80% of values are parseable as dates",
                            }
                        )

            if pd.api.types.is_float_dtype(series):
                numeric_non_null = pd.to_numeric(non_null, errors="coerce").dropna()
                if not numeric_non_null.empty:
                    decimal_parts = (numeric_non_null - np.floor(numeric_non_null)).abs()
                    decimal_variance = float(decimal_parts.var()) if len(decimal_parts) > 1 else float(decimal_parts.iloc[0] if len(decimal_parts) == 1 else 0.0)
                    if np.isclose(decimal_variance, 0.0) and bool((decimal_parts == 0).all()):
                        candidates.append(
                            {
                                "severity": "low",
                                "suggested_dtype": "integer",
                                "reason": "Float column appears to contain whole numbers only",
                            }
                        )

            if not candidates:
                continue

            chosen = sorted(candidates, key=lambda c: {"medium": 2, "low": 1}.get(c["severity"], 0), reverse=True)[0]
            issues.append(
                {
                    "check": "wrong_dtype",
                    "column": column_name,
                    "severity": str(chosen["severity"]),
                    "detail": {
                        "current_dtype": current_dtype,
                        "suggested_dtype": str(chosen["suggested_dtype"]),
                        "reason": str(chosen["reason"]),
                        "sample_values": [str(v) for v in sample_values],
                    },
                }
            )

        return issues
    except Exception:
        return []


def _check_label_leakage(dataframe: pd.DataFrame) -> list:
    """
    Returns list of suspected label leakage issues.
    Each issue dict:
    {
        "check": "label_leakage",
        "column": str,           # the leaking column
        "severity": "high",      # always high - leakage is always critical
        "detail": {
            "target_column": str,
            "correlation": float | None,
            "reason": str,
        }
    }
    This is always severity high. Max 3 issues total.
    """
    try:
        issues = []
        max_issues = 3
        target_keywords = [
            "target",
            "label",
            "class",
            "category",
            "outcome",
            "status",
            "type",
            "flag",
            "result",
            "grade",
        ]
        suspicious_name_keywords = ["future", "next", "result", "outcome", "final", "score"]

        target_like_columns = []
        for column in dataframe.columns:
            column_name = str(column)
            lowered_name = column_name.lower()
            if not any(k in lowered_name for k in target_keywords):
                continue

            series = dataframe[column].dropna()
            if series.empty:
                continue

            unique_count = int(series.nunique(dropna=True))
            if unique_count < 2 or unique_count > 20:
                continue

            is_object_like = pd.api.types.is_object_dtype(series) or pd.api.types.is_categorical_dtype(series)
            is_small_int = pd.api.types.is_integer_dtype(series) and unique_count <= 10
            if is_object_like or is_small_int:
                target_like_columns.append(column_name)

        if not target_like_columns:
            return issues

        flagged_columns = set()
        numeric_columns = [str(c) for c in dataframe.select_dtypes(include=["number"]).columns.tolist()]

        for target_column in target_like_columns:
            if len(issues) >= max_issues:
                break

            target_series_full = dataframe[target_column]
            target_numeric = pd.to_numeric(target_series_full, errors="coerce")
            target_is_numeric = pd.api.types.is_numeric_dtype(target_series_full)

            for numeric_column in numeric_columns:
                if len(issues) >= max_issues:
                    break
                if numeric_column == target_column or numeric_column in flagged_columns:
                    continue

                feature_series = pd.to_numeric(dataframe[numeric_column], errors="coerce")

                if target_is_numeric:
                    paired = pd.DataFrame({"target": target_numeric, "feature": feature_series}).dropna()
                    if len(paired) < 5:
                        continue

                    corr = paired["target"].corr(paired["feature"])
                    if pd.isna(corr):
                        continue

                    corr_value = float(abs(float(corr)))
                    if corr_value > 0.95:
                        issues.append(
                            {
                                "check": "label_leakage",
                                "column": str(numeric_column),
                                "severity": "high",
                                "detail": {
                                    "target_column": str(target_column),
                                    "correlation": float(round(corr_value, 4)),
                                    "reason": "Numeric column with >0.95 correlation to target column",
                                },
                            }
                        )
                        flagged_columns.add(numeric_column)
                else:
                    paired = pd.DataFrame({"target": target_series_full, "feature": feature_series}).dropna()
                    if len(paired) < 5:
                        continue

                    ranges = paired.groupby("target")["feature"].agg(["min", "max"]).dropna()
                    if len(ranges) < 2:
                        continue

                    intervals = sorted(
                        [(float(row["min"]), float(row["max"])) for _, row in ranges.iterrows()],
                        key=lambda x: x[0],
                    )
                    has_overlap = False
                    for idx in range(len(intervals) - 1):
                        left_max = intervals[idx][1]
                        right_min = intervals[idx + 1][0]
                        if left_max >= right_min:
                            has_overlap = True
                            break

                    if not has_overlap:
                        issues.append(
                            {
                                "check": "label_leakage",
                                "column": str(numeric_column),
                                "severity": "high",
                                "detail": {
                                    "target_column": str(target_column),
                                    "correlation": None,
                                    "reason": "Numeric column separates target classes into non-overlapping ranges",
                                },
                            }
                        )
                        flagged_columns.add(numeric_column)

        if len(issues) < max_issues:
            first_target = str(target_like_columns[0])
            for column in dataframe.columns:
                if len(issues) >= max_issues:
                    break

                column_name = str(column)
                lowered_name = column_name.lower()
                if column_name in target_like_columns or column_name in flagged_columns:
                    continue

                if any(k in lowered_name for k in suspicious_name_keywords):
                    issues.append(
                        {
                            "check": "label_leakage",
                            "column": column_name,
                            "severity": "high",
                            "detail": {
                                "target_column": first_target,
                                "correlation": None,
                                "reason": "Column name suggests post-outcome or future information",
                            },
                        }
                    )
                    flagged_columns.add(column_name)

        return issues[:max_issues]
    except Exception:
        return []


def _llm_explain_issue(issue: dict, key_manager: Any) -> str:
    """
    Generate a plain-English explanation for a single data quality issue.
    One short paragraph. Non-technical. Tells user why it matters
    and what to do about it.
    Follows exact same pattern as _llm_explain() in anomaly_explainer.py.
    """
    check = issue["check"]
    col = issue["column"]
    detail = issue["detail"]

    prompts = {
        "constant_column": (
            f"Column '{col}' has {detail.get('unique_values', 1)} unique value(s). "
            f"The dominant value '{detail.get('dominant_value')}' appears in "
            f"{detail.get('dominant_pct', 100):.1f}% of rows. "
            "Explain in 2 sentences why this is a problem for data analysis "
            "and what the user should do."
        ),
        "duplicate_column": (
            f"Column '{col}' appears to be a duplicate of '{detail.get('duplicate_of')}' "
            f"with {detail.get('similarity_score', 1.0) * 100:.0f}% similarity. "
            "Explain in 2 sentences why duplicate columns cause problems "
            "and whether to keep or drop one."
        ),
        "class_imbalance": (
            f"Column '{col}' has severe class imbalance: "
            f"'{detail.get('majority_class')}' is {detail.get('majority_pct', 0):.1f}% "
            f"of data, while '{detail.get('minority_class')}' is only "
            f"{detail.get('minority_pct', 0):.1f}%. "
            "Explain in 2 sentences what this means for ML models trained on this data."
        ),
        "wrong_dtype": (
            f"Column '{col}' has dtype '{detail.get('current_dtype')}' but "
            f"should likely be '{detail.get('suggested_dtype')}'. "
            f"Reason: {detail.get('reason')}. "
            "Explain in 2 sentences how this wrong type affects analysis."
        ),
        "label_leakage": (
            f"Column '{col}' may leak the target '{detail.get('target_column')}' "
            f"with correlation {detail.get('correlation')}. "
            f"Reason: {detail.get('reason')}. "
            "Explain in 2 sentences why label leakage is a critical ML problem "
            "and what the user should do immediately."
        ),
    }

    prompt = prompts.get(
        check,
        f"Data quality issue in column '{col}': {check}. Detail: {detail}. "
        "Explain the problem and fix in 2 sentences.",
    )

    try:
        return key_manager.call_with_retry(
            system_prompt=(
                "You explain data quality issues clearly to non-technical users. "
                "Be direct and actionable. No jargon. 2 sentences max."
            ),
            user_prompt=prompt,
            purpose="general",
        )
    except Exception:
        fallbacks = {
            "constant_column": (
                f"Column '{col}' contains almost no variation, making it useless for analysis. "
                "Consider dropping it to reduce noise in your dataset."
            ),
            "duplicate_column": (
                f"Column '{col}' appears to carry the same information as "
                f"'{detail.get('duplicate_of', 'another column')}'. "
                "Keep only one to avoid misleading results."
            ),
            "class_imbalance": (
                f"Column '{col}' has far more of one class than others, "
                "which can make ML models biased toward the majority class. "
                "Consider resampling or using class weights."
            ),
            "wrong_dtype": (
                f"Column '{col}' may be stored as the wrong data type, "
                "which can cause incorrect calculations or sorting. "
                "Consider converting it to the suggested type."
            ),
            "label_leakage": (
                f"Column '{col}' may contain future information that would not be "
                "available at prediction time, causing artificially inflated model accuracy. "
                "Remove it before training any model."
            ),
        }
        return fallbacks.get(check, f"Data quality issue detected in column '{col}'. Review and fix before analysis.")


def _compute_overall_score(issues: list) -> dict:
    """
    Compute an overall data quality score 0-100.

    Deduction per issue by severity:
      high   -> -15 points each
      medium -> -7 points each
      low    -> -3 points each

    Floor at 0. Start at 100.

    Also return a grade:
      90-100 -> "Excellent"
      75-89  -> "Good"
      60-74  -> "Fair"
      40-59  -> "Poor"
      0-39   -> "Critical"

    Returns:
    {
        "score": int,
        "grade": str,
        "high_count": int,
        "medium_count": int,
        "low_count": int,
        "total_issues": int,
    }
    """
    deductions = {"high": 15, "medium": 7, "low": 3}
    score = 100
    counts = {"high": 0, "medium": 0, "low": 0}

    for issue in issues:
        sev = str(issue.get("severity", "low"))
        score -= int(deductions.get(sev, 3))
        counts[sev] = int(counts.get(sev, 0)) + 1

    score = max(0, int(score))

    if score >= 90:
        grade = "Excellent"
    elif score >= 75:
        grade = "Good"
    elif score >= 60:
        grade = "Fair"
    elif score >= 40:
        grade = "Poor"
    else:
        grade = "Critical"

    return {
        "score": int(score),
        "grade": str(grade),
        "high_count": int(counts["high"]),
        "medium_count": int(counts["medium"]),
        "low_count": int(counts["low"]),
        "total_issues": int(len(issues)),
    }


def score_data_quality(dataframe: pd.DataFrame, key_manager: Any) -> dict:
    """
    Entry point called from main.py.
    Runs all 5 checks, explains each issue via LLM in parallel,
    computes overall score.

    Matches pattern of forecast_timeseries() and explain_anomalies().

    Returns:
    {
        "overall": {
            "score": int,            # 0-100
            "grade": str,            # Excellent/Good/Fair/Poor/Critical
            "high_count": int,
            "medium_count": int,
            "low_count": int,
            "total_issues": int,
        },
        "issues": [
            {
                "check": str,        # check type name
                "column": str,
                "severity": str,     # high / medium / low
                "detail": dict,      # check-specific detail fields
                "explanation": str,  # LLM plain-English explanation
            },
            ...
        ],
        "checks_run": [str, ...],    # list of check names that ran
        "message": str,              # summary sentence
    }
    """
    raw_issues = []
    checks_run = []

    for check_fn, check_name in [
        (_check_constant_columns, "constant_column"),
        (_check_duplicate_columns, "duplicate_column"),
        (_check_class_imbalance, "class_imbalance"),
        (_check_wrong_dtypes, "wrong_dtype"),
        (_check_label_leakage, "label_leakage"),
    ]:
        try:
            found = check_fn(dataframe)
            raw_issues.extend(found)
            checks_run.append(check_name)
        except Exception as e:
            checks_run.append(f"{check_name}(failed: {str(e)[:60]})")

    severity_rank = {"high": 3, "medium": 2, "low": 1}
    raw_issues.sort(key=lambda i: severity_rank.get(str(i.get("severity", "low")), 0), reverse=True)

    issues_for_llm = raw_issues[:15]

    explained_map: dict = {}
    workers = min(len(issues_for_llm), 4)

    if issues_for_llm and getattr(key_manager, "clients", None):
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_map = {
                executor.submit(_llm_explain_issue, issue, key_manager): idx
                for idx, issue in enumerate(issues_for_llm)
            }
            for future in as_completed(future_map):
                idx = future_map[future]
                try:
                    explained_map[idx] = str(future.result())
                except Exception:
                    explained_map[idx] = "Data quality issue detected. Review this column before analysis."
    else:
        for idx, issue in enumerate(issues_for_llm):
            explained_map[idx] = str(_llm_explain_issue(issue, key_manager))

    final_issues = []
    for idx, issue in enumerate(issues_for_llm):
        detail_clean = {}
        for key, value in issue["detail"].items():
            if isinstance(value, float):
                if np.isfinite(value):
                    detail_clean[str(key)] = float(value)
                else:
                    detail_clean[str(key)] = None
            elif isinstance(value, (np.integer,)):
                detail_clean[str(key)] = int(value)
            elif isinstance(value, (np.floating,)):
                detail_clean[str(key)] = float(value) if np.isfinite(value) else None
            elif isinstance(value, dict):
                detail_clean[str(key)] = {
                    str(k): (int(v) if isinstance(v, (np.integer,)) else (float(v) if isinstance(v, (np.floating, float)) and np.isfinite(v) else (None if isinstance(v, (np.floating, float)) and not np.isfinite(v) else v)))
                    for k, v in value.items()
                }
            elif isinstance(value, list):
                detail_clean[str(key)] = [str(v) for v in value]
            else:
                detail_clean[str(key)] = value if isinstance(value, (int, bool, str, type(None))) else str(value)

        final_issues.append(
            {
                "check": str(issue["check"]),
                "column": str(issue["column"]),
                "severity": str(issue["severity"]),
                "detail": detail_clean,
                "explanation": str(
                    explained_map.get(idx, "Review this column before proceeding with analysis.")
                ),
            }
        )

    overall = _compute_overall_score(raw_issues)

    if overall["total_issues"] == 0:
        message = "No data quality issues detected. Your dataset looks clean."
    elif overall["grade"] in ("Excellent", "Good"):
        message = (
            f"Minor issues found ({overall['total_issues']} total). "
            f"Dataset quality is {overall['grade'].lower()} - safe to analyze."
        )
    else:
        message = (
            f"{overall['high_count']} critical issue(s) detected. "
            "Fix these before training any model or drawing conclusions."
        )

    return {
        "overall": overall,
        "issues": final_issues,
        "checks_run": [str(c) for c in checks_run],
        "message": str(message),
    }
