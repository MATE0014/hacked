"""
Pydantic schemas for request/response validation
"""

from pydantic import BaseModel
from typing import Optional, Dict, Any, List


class ChatRequest(BaseModel):
    """Schema for chat questions"""
    question: str
    history: Optional[List[Dict[str, str]]] = None


class ChatResponse(BaseModel):
    """Schema for chat responses"""
    success: bool
    question: str
    answer: str
    history: Optional[List[Dict[str, str]]] = None


class DataAnalysisResponse(BaseModel):
    """Schema for analysis responses"""
    success: bool
    metadata: Dict[str, Any]
    statistics: Dict[str, Any]
    structure: Dict[str, Any]
    charts: Dict[str, Any]
    insights: str
    summary: str


class UploadResponse(BaseModel):
    """Schema for upload responses"""
    success: bool
    message: str
    metadata: Dict[str, Any]
    sample_data: List[Dict[str, Any]]
