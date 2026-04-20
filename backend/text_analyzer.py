"""Utilities for NLP-style analysis of free-text columns."""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List

import pandas as pd


MOCK_SENTIMENT = {
    "overall_sentiment": "mixed",
    "positive_pct": 40,
    "negative_pct": 25,
    "neutral_pct": 35,
    "confidence": "low",
    "summary": "Mock mode: connect Groq API for real sentiment analysis.",
}

MOCK_TOPICS = {
    "topics": [
        {"topic": "General Content", "keywords": ["data", "value", "result"], "frequency": "high"}
    ],
    "dominant_topic": "General Content",
}

MOCK_KEY_PHRASES = {
    "summary": "Mock mode active. Connect Groq API keys for real text analysis.",
    "key_phrases": ["connect api", "groq key", "text analysis", "mock mode", "insightflow"],
}


def _sample_texts(series: pd.Series, n: int, separator: str = "\n- ") -> str:
    """Sample n rows and join as a numbered list for prompt context."""
    sampled = series.sample(min(n, len(series)), random_state=42).tolist()
    return separator + separator.join(str(t)[:200] for t in sampled)


def _strip_markdown_fences(text: str) -> str:
    """Remove markdown code fences often returned by LLMs."""
    cleaned = (text or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def _parse_json_response(text: str) -> Dict[str, Any]:
    """Parse LLM JSON response and fail fast on malformed content."""
    cleaned = _strip_markdown_fences(text)
    return json.loads(cleaned)


def detect_text_columns(dataframe: pd.DataFrame, structure: dict) -> list:
    """
    Return list of column names that contain free text worth analyzing.
    Uses the structure dict already produced by analyzer.detect_structure().
    """
    id_cols = set(
        col for col, pattern in structure.get("patterns", {}).items() if pattern == "potential_id_column"
    )
    text_cols = []
    for col in structure.get("categorical_columns", []):
        if col not in dataframe.columns:
            continue

        col_name = str(col).strip().lower()
        is_explicit_id_name = col_name == "id" or col_name.endswith("_id")

        # Only exclude detected IDs when the column name itself is ID-like.
        # Free-text columns can be highly unique and should still be analyzed.
        if col in id_cols and is_explicit_id_name:
            continue

        series = dataframe[col].dropna().astype(str)
        if len(series) < 10:
            continue
        avg_len = series.str.len().mean()
        unique_ratio = series.nunique() / len(series)

        has_sentence_like_text = float((series.str.count(r"\s+") >= 2).mean()) > 0.35
        if (avg_len > 20 and unique_ratio > 0.2) or has_sentence_like_text:
            text_cols.append(col)
    return text_cols


def analyze_text_column(col: str, series: pd.Series, key_manager: Any) -> dict:
    """Run sentiment, topic, and key-phrase analysis on a single text column."""

    def _is_mock_mode_active() -> bool:
        clients = getattr(key_manager, "clients", None)
        return not bool(clients)

    sentiment_result: Dict[str, Any]
    topics_result: Dict[str, Any]
    key_phrases_result: Dict[str, Any]

    # 1) Sentiment analysis
    sentiment_user_prompt = (
        f"Analyze the sentiment of these text samples from column '{col}'.\n"
        f"Texts: {_sample_texts(series, 200)}\n\n"
        "Return ONLY this JSON, no explanation:\n"
        "{\n"
        '  "overall_sentiment": "positive" | "negative" | "neutral" | "mixed",\n'
        '  "positive_pct": <0-100>,\n'
        '  "negative_pct": <0-100>,\n'
        '  "neutral_pct": <0-100>,\n'
        '  "confidence": "high" | "medium" | "low",\n'
        '  "summary": ""\n'
        "}"
    )
    try:
        if _is_mock_mode_active():
            raise RuntimeError("Mock mode active")
        sentiment_raw = key_manager.call_with_retry(
            "You are a sentiment analysis expert. Respond only in valid JSON.",
            sentiment_user_prompt,
            purpose="general",
        )
        sentiment_result = _parse_json_response(sentiment_raw)
    except Exception:
        sentiment_result = dict(MOCK_SENTIMENT)

    # 2) Topic extraction
    topics_user_prompt = (
        f"Extract the main topics from these text samples from column '{col}'.\n"
        f"Texts: {_sample_texts(series, 150)}\n\n"
        "Return ONLY this JSON, no explanation:\n"
        "{\n"
        '  "topics": [\n'
        '    {"topic": "", "keywords": ["kw1", "kw2", "kw3"], "frequency": "high" | "medium" | "low"}\n'
        "  ],\n"
        '  "dominant_topic": ""\n'
        "}"
    )
    try:
        if _is_mock_mode_active():
            raise RuntimeError("Mock mode active")
        topics_raw = key_manager.call_with_retry(
            "You are a topic modeling expert. Respond only in valid JSON.",
            topics_user_prompt,
            purpose="general",
        )
        topics_result = _parse_json_response(topics_raw)
    except Exception:
        topics_result = dict(MOCK_TOPICS)

    # 3) Key phrases / summary
    summary_user_prompt = (
        f"Summarize the key themes and important phrases from this text column '{col}'.\n"
        f"Texts: {_sample_texts(series, 100, separator='\n')}\n\n"
        "Provide:\n"
        "1. A 2-sentence summary of what this column contains\n"
        "2. Top 5 most meaningful phrases or terms found\n\n"
        "Format as JSON:\n"
        "{\n"
        '  "summary": "<2 sentence summary>",\n'
        '  "key_phrases": ["phrase1", "phrase2", "phrase3", "phrase4", "phrase5"]\n'
        "}"
    )
    try:
        if _is_mock_mode_active():
            raise RuntimeError("Mock mode active")
        summary_raw = key_manager.call_with_retry(
            "You are a text summarization expert.",
            summary_user_prompt,
            purpose="general",
        )
        key_phrases_result = _parse_json_response(summary_raw)
    except Exception:
        key_phrases_result = dict(MOCK_KEY_PHRASES)

    return {
        "column": col,
        "row_count": len(series),
        "avg_length": float(series.str.len().mean()),
        "sentiment": sentiment_result,
        "topics": topics_result,
        "key_phrases": key_phrases_result,
        "sample_preview": series.head(3).tolist(),
    }


def analyze_all_text_columns(dataframe: pd.DataFrame, structure: dict, key_manager: Any) -> dict:
    """
    Entry point called from main.py.
    Detects text columns and runs NLP on each.
    Returns structured result ready for JSON response.
    """
    text_cols = detect_text_columns(dataframe, structure)

    if not text_cols:
        return {
            "text_columns_found": 0,
            "message": "No free-text columns detected in this dataset.",
            "columns": [],
        }

    results = []
    for col in text_cols[:5]:
        series = dataframe[col].dropna().astype(str)
        result = analyze_text_column(col, series, key_manager)
        results.append(result)

    return {
        "text_columns_found": len(text_cols),
        "analyzed_columns": len(results),
        "columns": results,
    }
