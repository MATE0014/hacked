"""Async MongoDB persistence for analysis metadata and summaries."""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _upload_ttl_seconds() -> int:
    value = (os.getenv("MONGODB_UPLOAD_TTL_SECONDS") or "3600").strip()
    try:
        parsed = int(value)
    except ValueError:
        return 3600
    return max(60, parsed)


def _compact_analysis_payload(analysis: Dict[str, Any]) -> Dict[str, Any]:
    metadata = analysis.get("metadata") or {}
    structure = analysis.get("structure") or {}
    statistics = analysis.get("statistics") or {}

    return {
        "success": analysis.get("success"),
        "summary": str(analysis.get("summary") or "")[:2000],
        "insights": str(analysis.get("insights") or "")[:6000],
        "metadata": {
            "original_filename": metadata.get("original_filename"),
            "rows": metadata.get("rows"),
            "columns": (metadata.get("columns") or [])[:200],
        },
        "shape": {
            "numeric_columns": len((structure.get("numeric_columns") or [])),
            "categorical_columns": len((structure.get("categorical_columns") or [])),
        },
        "statistics_keys": list((statistics or {}).keys())[:40],
    }


class MongoStore:
    def __init__(self) -> None:
        self.client: Optional[AsyncIOMotorClient] = None
        self.db = None
        self.enabled = False

    async def connect(self) -> None:
        uri = (os.getenv("MONGODB_URI") or "").strip()
        if not uri:
            print("MongoDB not configured (MONGODB_URI missing). Running without persistent DB.")
            return

        db_name = (os.getenv("MONGODB_DB") or "insightflow").strip()

        self.client = AsyncIOMotorClient(
            uri,
            appname="InsightFlow",
            maxPoolSize=int(os.getenv("MONGODB_MAX_POOL_SIZE", "30")),
            minPoolSize=int(os.getenv("MONGODB_MIN_POOL_SIZE", "5")),
            serverSelectionTimeoutMS=int(os.getenv("MONGODB_SERVER_SELECTION_MS", "3000")),
            connectTimeoutMS=int(os.getenv("MONGODB_CONNECT_TIMEOUT_MS", "3000")),
            socketTimeoutMS=int(os.getenv("MONGODB_SOCKET_TIMEOUT_MS", "10000")),
            retryWrites=True,
        )
        self.db = self.client[db_name]

        try:
            await self.client.admin.command("ping")
            await self._ensure_indexes()
            self.enabled = True
            print(f"MongoDB connected: db={db_name}")
        except Exception as exc:
            print(f"MongoDB unavailable: {exc}")
            self.enabled = False

    async def close(self) -> None:
        if self.client is not None:
            self.client.close()
        self.client = None
        self.db = None
        self.enabled = False

    async def _ensure_indexes(self) -> None:
        if self.db is None:
            return
        await self.db.uploads.create_index([("created_at", DESCENDING)])
        await self.db.uploads.create_index([("metadata.original_filename", DESCENDING)])
        await self.db.uploads.create_index(
            [("expires_at", ASCENDING)],
            expireAfterSeconds=0,
            name="uploads_expires_at_ttl",
        )
        await self.db.analyses.create_index([("created_at", DESCENDING)])
        await self.db.analyses.create_index([("metadata.original_filename", DESCENDING)])
        await self.db.reports.create_index([("created_at", DESCENDING)])

    async def save_upload(self, metadata: Dict[str, Any]) -> None:
        if not self.enabled or self.db is None:
            return
        try:
            now = _now_utc()
            payload = {
                "created_at": now,
                "expires_at": datetime.fromtimestamp(now.timestamp() + _upload_ttl_seconds(), tz=timezone.utc),
                "metadata": {
                    "original_filename": metadata.get("original_filename"),
                    "rows": metadata.get("rows"),
                    "columns": (metadata.get("columns") or [])[:200],
                },
            }
            await self.db.uploads.insert_one(payload)
        except Exception as exc:
            print(f"Mongo save_upload failed: {exc}")

    async def save_analysis(self, analysis_result: Dict[str, Any]) -> None:
        if not self.enabled or self.db is None:
            return
        try:
            payload = {
                "created_at": _now_utc(),
                "analysis": _compact_analysis_payload(analysis_result),
            }
            await self.db.analyses.insert_one(payload)
        except Exception as exc:
            print(f"Mongo save_analysis failed: {exc}")

    async def save_report(self, report_payload: Dict[str, Any], dataset_name: str) -> None:
        if not self.enabled or self.db is None:
            return
        try:
            payload = {
                "created_at": _now_utc(),
                "dataset_name": dataset_name,
                "report": {
                    "executive_summary": str(report_payload.get("executive_summary") or "")[:6000],
                    "data_quality": report_payload.get("data_quality") or {},
                    "chart_recommendations": (report_payload.get("chart_recommendations") or [])[:20],
                },
            }
            await self.db.reports.insert_one(payload)
        except Exception as exc:
            print(f"Mongo save_report failed: {exc}")