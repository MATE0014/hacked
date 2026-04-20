"""Chunked processing for large CSV files with LLM-assisted insight synthesis."""
from __future__ import annotations

import math
import json
from io import StringIO
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional

import pandas as pd


MAX_CHUNK_BYTES = 50 * 1024 * 1024


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, (int, float, str, bool)) or value is None:
        return value
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return str(value)


def _build_chunk_stats(chunk: pd.DataFrame) -> Dict[str, Any]:
    numeric_cols = chunk.select_dtypes(include=["number"]).columns.tolist()
    categorical_cols = chunk.select_dtypes(exclude=["number"]).columns.tolist()

    describe_df = chunk.describe(include="all").fillna("")
    describe_stats = {
        col: {k: _to_jsonable(v) for k, v in stats.items()}
        for col, stats in describe_df.to_dict().items()
    }

    null_pct = ((chunk.isna().sum() / max(1, len(chunk))) * 100).round(2).to_dict()
    top_values: Dict[str, Any] = {}
    for col in categorical_cols:
        top_values[col] = {
            str(k): int(v)
            for k, v in chunk[col].astype(str).value_counts(dropna=False).head(3).items()
        }

    iqr_outliers: Dict[str, int] = {}
    for col in numeric_cols:
        series = pd.to_numeric(chunk[col], errors="coerce").dropna()
        if series.empty:
            iqr_outliers[col] = 0
            continue
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            iqr_outliers[col] = 0
            continue
        low = q1 - 1.5 * iqr
        high = q3 + 1.5 * iqr
        outlier_count = int(((series < low) | (series > high)).sum())
        iqr_outliers[col] = outlier_count

    return {
        "rows": int(len(chunk)),
        "columns": int(len(chunk.columns)),
        "describe": describe_stats,
        "null_percent": {k: _to_jsonable(v) for k, v in null_pct.items()},
        "top_values": top_values,
        "iqr_outliers": iqr_outliers,
    }


def _iter_csv_chunks_by_size(filepath: str, max_chunk_bytes: int = MAX_CHUNK_BYTES):
    """Yield DataFrame chunks parsed from <= max_chunk_bytes CSV text segments."""
    with open(filepath, "r", encoding="utf-8", errors="replace", newline="") as csv_file:
        header = csv_file.readline()
        if not header:
            return

        header_bytes = len(header.encode("utf-8"))
        lines: List[str] = []
        payload_bytes = header_bytes

        for line in csv_file:
            line_bytes = len(line.encode("utf-8"))

            if lines and payload_bytes + line_bytes > max_chunk_bytes:
                payload = header + "".join(lines)
                yield pd.read_csv(StringIO(payload))
                lines = [line]
                payload_bytes = header_bytes + line_bytes
                continue

            lines.append(line)
            payload_bytes += line_bytes

        if lines:
            payload = header + "".join(lines)
            yield pd.read_csv(StringIO(payload))


def _call_llm_for_chunk(chunk_id: int, stats: Dict[str, Any], key_manager: Any) -> str:
    prompt = f"Chunk {chunk_id} stats: {json.dumps(stats)[:6000]}. List 2 patterns or anomalies. Be brief."
    try:
        return key_manager.call_with_retry(
            system_prompt="You are an expert data analyst. Respond with concise findings.",
            user_prompt=prompt,
            purpose="chunk",
        )
    except Exception:
        return "Pattern summary unavailable due to temporary LLM limit."


def _synthesize_insights(chunk_insights: List[str], key_manager: Any) -> Dict[str, Any]:
    fallback = {
        "findings": ["Chunk-level processing completed, but synthesis is in fallback mode."],
        "recommendations": ["Retry synthesis when API limits are lower."],
        "data_quality_score": 70,
    }

    if not chunk_insights:
        return fallback

    def _parse_payload(response_text: str) -> Dict[str, Any]:
        parsed = json.loads(response_text)
        findings = parsed.get("findings") if isinstance(parsed, dict) else None
        recommendations = parsed.get("recommendations") if isinstance(parsed, dict) else None
        score = parsed.get("data_quality_score") if isinstance(parsed, dict) else None
        return {
            "findings": findings if isinstance(findings, list) else [],
            "recommendations": recommendations if isinstance(recommendations, list) else [],
            "data_quality_score": int(score) if isinstance(score, (int, float)) else fallback["data_quality_score"],
        }

    def _combine_group(group_name: str, group_insights: List[str], purpose: str) -> Dict[str, Any]:
        group_prompt = (
            f"Group {group_name} has {len(group_insights)} chunk insights: {group_insights}. "
            "Synthesize top 3 findings and 2 recommendations. Return JSON with keys: "
            "findings (list), recommendations (list), data_quality_score (0-100)."
        )
        response = key_manager.call_with_retry(
            system_prompt="You are a senior analytics consultant. Always return valid JSON.",
            user_prompt=group_prompt,
            purpose=purpose,
        )
        return _parse_payload(response)

    midpoint = max(1, len(chunk_insights) // 2)
    left_group = chunk_insights[:midpoint]
    right_group = chunk_insights[midpoint:]

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            left_future = executor.submit(_combine_group, "A", left_group, "summary")
            right_future = (
                executor.submit(_combine_group, "B", right_group, "backup")
                if right_group
                else None
            )

            left_result = left_future.result()
            right_result = right_future.result() if right_future else {
                "findings": [],
                "recommendations": [],
                "data_quality_score": left_result["data_quality_score"],
            }

        final_prompt = (
            "Merge these two partial synthesis results into one final executive summary output. "
            "Return JSON with keys: findings (top 5 list), recommendations (top 3 list), "
            "data_quality_score (0-100).\n\n"
            f"Group A: {left_result}\n"
            f"Group B: {right_result}"
        )

        final_response = key_manager.call_with_retry(
            system_prompt="You are a senior analytics consultant. Always return valid JSON.",
            user_prompt=final_prompt,
            purpose="summary",
        )
        final_parsed = _parse_payload(final_response)

        if not final_parsed["findings"]:
            final_parsed["findings"] = (left_result["findings"] + right_result["findings"])[:5] or fallback["findings"]
        if not final_parsed["recommendations"]:
            final_parsed["recommendations"] = (
                left_result["recommendations"] + right_result["recommendations"]
            )[:3] or fallback["recommendations"]

        if not isinstance(final_parsed["data_quality_score"], int):
            final_parsed["data_quality_score"] = int(
                round((left_result["data_quality_score"] + right_result["data_quality_score"]) / 2)
            )

        return final_parsed

    except Exception:
        return {
            "findings": fallback["findings"],
            "recommendations": fallback["recommendations"],
            "data_quality_score": fallback["data_quality_score"],
        }


def process_large_file(filepath: str, key_manager: Any) -> Dict[str, Any]:
    """Process a large CSV file in chunks and synthesize cross-chunk insights."""
    chunk_tasks = []

    with ThreadPoolExecutor(max_workers=3) as executor:
        for chunk_id, chunk in enumerate(_iter_csv_chunks_by_size(filepath, MAX_CHUNK_BYTES), start=1):
            chunk_tasks.append(
                executor.submit(_process_chunk, chunk_id, chunk, key_manager)
            )

    chunk_results = []
    for future in as_completed(chunk_tasks):
        chunk_results.append(future.result())

    chunk_results.sort(key=lambda item: item["chunk_id"])
    per_chunk_insights = [item["insight"] for item in chunk_results]
    synthesis = _synthesize_insights(per_chunk_insights, key_manager)

    return {
        "chunk_count": len(chunk_results),
        "findings": synthesis["findings"],
        "recommendations": synthesis["recommendations"],
        "data_quality_score": synthesis["data_quality_score"],
        "per_chunk_insights": per_chunk_insights,
    }


def process_large_file_with_progress(
    filepath: str,
    key_manager: Any,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> Dict[str, Any]:
    """Process a large CSV file and emit chunk progress via callback."""
    file_size = Path(filepath).stat().st_size
    estimated_total_chunks = max(1, math.ceil(file_size / MAX_CHUNK_BYTES))

    chunks = list(_iter_csv_chunks_by_size(filepath, MAX_CHUNK_BYTES))
    total_chunks = max(estimated_total_chunks, len(chunks))

    if total_chunks == 0:
        return {
            "chunk_count": 0,
            "findings": [],
            "recommendations": [],
            "data_quality_score": 0,
            "per_chunk_insights": [],
        }

    chunk_tasks = []
    with ThreadPoolExecutor(max_workers=3) as executor:
        for chunk_id, chunk in enumerate(chunks, start=1):
            chunk_tasks.append(executor.submit(_process_chunk, chunk_id, chunk, key_manager))

    chunk_results = []
    completed = 0
    for future in as_completed(chunk_tasks):
        chunk_results.append(future.result())
        completed += 1
        if progress_callback:
            progress_callback(completed, total_chunks)

    chunk_results.sort(key=lambda item: item["chunk_id"])
    per_chunk_insights = [item["insight"] for item in chunk_results]
    synthesis = _synthesize_insights(per_chunk_insights, key_manager)

    return {
        "chunk_count": len(chunk_results),
        "findings": synthesis["findings"],
        "recommendations": synthesis["recommendations"],
        "data_quality_score": synthesis["data_quality_score"],
        "per_chunk_insights": per_chunk_insights,
    }


def _process_chunk(chunk_id: int, chunk: pd.DataFrame, key_manager: Any) -> Dict[str, Any]:
    stats = _build_chunk_stats(chunk)
    insight = _call_llm_for_chunk(chunk_id, stats, key_manager)
    return {
        "chunk_id": chunk_id,
        "stats": stats,
        "insight": insight,
    }
