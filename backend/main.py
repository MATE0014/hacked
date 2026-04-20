"""
Main FastAPI application for AI-powered data analysis system
No API costs - completely free to use!
"""
import asyncio
import os
import tempfile
import traceback
import json
import shutil
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response, StreamingResponse
import pandas as pd
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from data_processor import DataProcessor
from analyzer import Analyzer
from llm_engine import LLMEngine
from file_processor import process_large_file, process_large_file_with_progress
from anomaly_explainer import explain_anomalies
from mongo_store import MongoStore
from report_generator import generate_report, render_report_pdf_bytes
from schemas import DataAnalysisResponse, ChatRequest, ChatResponse

# Initialize FastAPI app
app = FastAPI(title="AI Data Analysis System", version="1.0.0")

default_origins = [
    "http://localhost:3000",
    "http://localhost:3001",
    "https://insightflowz.vercel.app",
]
configured_origins = os.getenv("CORS_ORIGINS", "")
allowed_origins = [origin.strip() for origin in configured_origins.split(",") if origin.strip()] or default_origins

# Configure CORS for frontend communication
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global storage for current dataset (in production, use database)
current_dataset = {}
current_dataframe = None
analyzer = None
llm_engine = None
current_report = None
current_stats = None
current_anomalies = None
mongo_store = MongoStore()

# Initialize upload directory. In serverless environments (e.g. Vercel),
# only the temp directory is writable.
is_serverless_runtime = os.getenv("VERCEL") == "1" or bool(os.getenv("AWS_LAMBDA_FUNCTION_NAME"))
upload_root = Path(tempfile.gettempdir()) if is_serverless_runtime else Path(".")
UPLOAD_DIR = upload_root / "uploaded_files"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
CHUNK_UPLOAD_DIR = UPLOAD_DIR / "chunks"
CHUNK_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


async def _write_upload_to_disk(upload_file: UploadFile, destination: Path) -> None:
    """Persist an uploaded file to disk incrementally to avoid loading it fully into memory."""
    with open(destination, "wb") as output:
        while True:
            chunk = await upload_file.read(1024 * 1024)
            if not chunk:
                break
            output.write(chunk)


async def _process_uploaded_dataset(file_path: Path, original_filename: str):
    """Load, clean, and store dataset state for downstream analysis endpoints."""
    global current_dataframe, current_dataset, analyzer, current_report, current_stats, current_anomalies

    extension = file_path.suffix.lower()
    if extension == '.csv':
        df = await asyncio.to_thread(pd.read_csv, file_path)
    else:
        df = await asyncio.to_thread(pd.read_excel, file_path)

    processor = DataProcessor()
    cleaned_df, processing_report = await asyncio.to_thread(processor.process, df)

    current_dataframe = cleaned_df
    current_dataset = {
        "original_filename": original_filename,
        "rows": len(cleaned_df),
        "columns": list(cleaned_df.columns),
        "dtypes": cleaned_df.dtypes.astype(str).to_dict(),
        "processing_report": processing_report,
        "file_path": str(file_path)
    }

    analyzer = Analyzer(cleaned_df)
    current_report = None
    current_stats = None
    current_anomalies = None

    asyncio.create_task(mongo_store.save_upload(current_dataset))

    sample_data = json.loads(
        cleaned_df.head(5).to_json(orient='records', date_format='iso')
    )

    print(f"✓ Dataset uploaded and processed: {original_filename}")

    return {
        "success": True,
        "message": "Dataset uploaded and processed successfully",
        "metadata": current_dataset,
        "sample_data": sample_data
    }


@app.on_event("startup")
async def startup_event():
    """Initialize LLM engine on startup"""
    global llm_engine
    llm_engine = LLMEngine()
    await mongo_store.connect()
    print("\n" + "="*60)
    print("✓ Data Analysis System Ready!")
    print("✓ Using FREE LLM APIs (Groq/HuggingFace/Gemini)")
    print("="*60 + "\n")


@app.on_event("shutdown")
async def shutdown_event():
    """Close database connections gracefully."""
    await mongo_store.close()


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@app.post("/upload")
async def upload_dataset(file: UploadFile = File(...)):
    """
    Upload and process dataset (CSV or Excel)
    
    Returns: Dataset metadata and initial analysis
    """
    try:
        original_filename = Path(file.filename or "uploaded_file").name
        extension = Path(original_filename).suffix.lower()

        # Validate file type
        if extension not in {'.csv', '.xlsx'}:
            raise HTTPException(status_code=400, detail="Only CSV and Excel files are supported")

        # Save uploaded file
        unique_filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex}{extension}"
        file_path = UPLOAD_DIR / unique_filename
        await _write_upload_to_disk(file, file_path)
        return await _process_uploaded_dataset(file_path, original_filename)
        
    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")


@app.post("/upload/chunk")
async def upload_dataset_chunk(
    upload_id: str = Form(...),
    chunk_index: int = Form(...),
    total_chunks: int = Form(...),
    filename: str = Form(...),
    file: UploadFile = File(...),
):
    """Upload one chunk of a large dataset; chunks are assembled by /upload/complete."""
    try:
        if not upload_id.strip():
            raise HTTPException(status_code=400, detail="upload_id is required")
        if chunk_index < 0:
            raise HTTPException(status_code=400, detail="chunk_index must be >= 0")
        if total_chunks <= 0:
            raise HTTPException(status_code=400, detail="total_chunks must be > 0")

        original_filename = Path(filename or "uploaded_file").name
        extension = Path(original_filename).suffix.lower()
        if extension not in {'.csv', '.xlsx'}:
            raise HTTPException(status_code=400, detail="Only CSV and Excel files are supported")

        session_dir = CHUNK_UPLOAD_DIR / upload_id
        session_dir.mkdir(parents=True, exist_ok=True)
        metadata_path = session_dir / "metadata.json"

        metadata = {
            "filename": original_filename,
            "total_chunks": total_chunks,
            "extension": extension,
        }

        if metadata_path.exists():
            with open(metadata_path, "r", encoding="utf-8") as meta_file:
                existing = json.load(meta_file)
            if (
                existing.get("filename") != metadata["filename"]
                or int(existing.get("total_chunks", 0)) != total_chunks
            ):
                raise HTTPException(status_code=400, detail="Chunk metadata mismatch for upload_id")
        else:
            with open(metadata_path, "w", encoding="utf-8") as meta_file:
                json.dump(metadata, meta_file)

        chunk_path = session_dir / f"{chunk_index:06d}.part"
        await _write_upload_to_disk(file, chunk_path)

        return {
            "success": True,
            "upload_id": upload_id,
            "chunk_index": chunk_index,
            "total_chunks": total_chunks,
        }
    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error uploading chunk: {str(e)}")


@app.post("/upload/complete")
async def complete_chunked_upload(upload_id: str = Form(...)):
    """Assemble uploaded chunks and process the resulting dataset."""
    try:
        if not upload_id.strip():
            raise HTTPException(status_code=400, detail="upload_id is required")

        session_dir = CHUNK_UPLOAD_DIR / upload_id
        metadata_path = session_dir / "metadata.json"
        if not session_dir.exists() or not metadata_path.exists():
            raise HTTPException(status_code=404, detail="Upload session not found")

        with open(metadata_path, "r", encoding="utf-8") as meta_file:
            metadata = json.load(meta_file)

        original_filename = Path(metadata.get("filename") or "uploaded_file").name
        total_chunks = int(metadata.get("total_chunks") or 0)
        extension = Path(original_filename).suffix.lower()
        if extension not in {'.csv', '.xlsx'}:
            raise HTTPException(status_code=400, detail="Only CSV and Excel files are supported")
        if total_chunks <= 0:
            raise HTTPException(status_code=400, detail="Invalid chunk metadata")

        unique_filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex}{extension}"
        final_path = UPLOAD_DIR / unique_filename

        with open(final_path, "wb") as output_file:
            for chunk_index in range(total_chunks):
                chunk_path = session_dir / f"{chunk_index:06d}.part"
                if not chunk_path.exists():
                    raise HTTPException(status_code=400, detail=f"Missing chunk {chunk_index}")
                with open(chunk_path, "rb") as chunk_file:
                    shutil.copyfileobj(chunk_file, output_file)

        response = await _process_uploaded_dataset(final_path, original_filename)
        shutil.rmtree(session_dir, ignore_errors=True)
        return response
    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error completing chunked upload: {str(e)}")


@app.get("/analyze")
async def analyze_dataset():
    """
    Perform comprehensive analysis on uploaded dataset
    
    Returns: Statistics, insights, chart data
    """
    global current_dataframe, analyzer, llm_engine, current_stats
    
    if current_dataframe is None:
        raise HTTPException(status_code=400, detail="No dataset uploaded. Please upload a file first.")
    
    try:
        # Run CPU-heavy analysis in worker threads.
        stats = await asyncio.to_thread(analyzer.get_statistics)
        current_stats = stats
        
        # Detect column types and structure
        structure = await asyncio.to_thread(analyzer.detect_structure)
        
        # Generate charts data
        charts_data = await asyncio.to_thread(analyzer.generate_charts_data)
        
        # Generate LLM insights
        insights = await asyncio.to_thread(
            llm_engine.generate_insights,
            dataframe=current_dataframe,
            statistics=stats,
            structure=structure
        )
        
        analysis_result = {
            "success": True,
            "metadata": current_dataset,
            "statistics": stats,
            "structure": structure,
            "charts": charts_data,
            "insights": insights,
            "summary": f"Analyzed {len(current_dataframe)} rows and {len(current_dataframe.columns)} columns. "
                      f"Found {len([c for c, t in structure['column_types'].items() if t == 'numeric'])} numeric, "
                      f"{len([c for c, t in structure['column_types'].items() if t == 'categorical'])} categorical columns."
        }

        # Persist a compact analysis snapshot asynchronously.
        asyncio.create_task(mongo_store.save_analysis(analysis_result))
        
        return analysis_result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error analyzing dataset: {str(e)}")


@app.post("/chat")
async def chat_about_data(request: ChatRequest):
    """
    Natural language Q&A about the dataset
    
    Args: ChatRequest with user question
    Returns: AI-generated answer about the data
    """
    global current_dataframe, llm_engine
    
    if current_dataframe is None:
        raise HTTPException(status_code=400, detail="No dataset uploaded. Please upload a file first.")
    
    try:
        response = await asyncio.to_thread(
            llm_engine.answer_question,
            question=request.question,
            dataframe=current_dataframe,
            history=request.history,
        )

        history = request.history[:] if request.history else []
        history.append({"role": "user", "content": request.question})
        history.append({"role": "assistant", "content": response})
        
        return {
            "success": True,
            "question": request.question,
            "answer": response,
            "history": history,
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing question: {str(e)}")


@app.post("/api/chat")
async def api_chat_about_data(request: ChatRequest):
    """Alias route for chat with conversation memory support."""
    return await chat_about_data(request)


@app.post("/api/analyze-large")
async def analyze_large_dataset():
    """Process large CSV files with chunked analysis and LLM synthesis."""
    global current_dataset, llm_engine

    if not current_dataset:
        raise HTTPException(status_code=400, detail="No dataset uploaded. Please upload a file first.")

    file_path = current_dataset.get("file_path")
    if not file_path:
        raise HTTPException(status_code=400, detail="No file path found for current dataset")

    if not str(file_path).lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Large-file endpoint currently supports CSV files only")

    try:
        result = await asyncio.to_thread(process_large_file, file_path, llm_engine.key_manager)
        return {
            "success": True,
            "result": result,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in large-file analysis: {str(e)}")


@app.get("/api/analyze-large/stream")
async def analyze_large_dataset_stream():
    """Stream large-file analysis progress and final result as SSE events."""
    global current_dataset, llm_engine

    if not current_dataset:
        raise HTTPException(status_code=400, detail="No dataset uploaded. Please upload a file first.")

    file_path = current_dataset.get("file_path")
    if not file_path:
        raise HTTPException(status_code=400, detail="No file path found for current dataset")

    if not str(file_path).lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Large-file stream currently supports CSV files only")

    def event_generator():
        progress_events = []

        def on_progress(done: int, total: int):
            percent = int((done / max(1, total)) * 100)
            progress_events.append(
                {
                    "stage": f"chunk_{done}_of_{total}",
                    "percent": percent,
                }
            )

        try:
            yield f"data: {json.dumps({'stage': 'starting', 'percent': 0})}\n\n"
            result = process_large_file_with_progress(file_path, llm_engine.key_manager, on_progress)

            for event in progress_events:
                yield f"data: {json.dumps(event)}\n\n"

            yield f"data: {json.dumps({'stage': 'complete', 'percent': 100, 'result': result})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'stage': 'error', 'percent': 0, 'message': str(e)})}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/api/anomalies")
async def anomalies():
    """Detect and explain anomalies in the currently loaded dataset."""
    global current_dataframe, llm_engine, current_anomalies

    if current_dataframe is None:
        raise HTTPException(status_code=400, detail="No dataset uploaded. Please upload a file first.")

    try:
        if current_anomalies is not None:
            return {
                "success": True,
                "anomalies": current_anomalies,
                "count": len(current_anomalies),
            }

        anomaly_items = await asyncio.to_thread(explain_anomalies, current_dataframe, llm_engine.key_manager)
        current_anomalies = anomaly_items
        return {
            "success": True,
            "anomalies": anomaly_items,
            "count": len(anomaly_items),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating anomaly explanations: {str(e)}")


@app.post("/api/report")
async def report():
    """Generate a narrative report and export PDF for current dataset."""
    global current_dataframe, analyzer, llm_engine, current_report, current_stats

    if current_dataframe is None:
        raise HTTPException(status_code=400, detail="No dataset uploaded. Please upload a file first.")

    try:
        if current_report is not None:
            return {
                "success": True,
                "report": current_report,
            }

        if current_stats is None:
            current_stats = await asyncio.to_thread(
                analyzer.get_statistics if analyzer is not None else Analyzer(current_dataframe).get_statistics
            )

        stats = current_stats
        current_report = await asyncio.to_thread(generate_report, current_dataframe, stats, llm_engine.key_manager)
        asyncio.create_task(
            mongo_store.save_report(
                current_report,
                current_dataset.get("original_filename") or "dataset",
            )
        )
        return {
            "success": True,
            "report": current_report,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating report: {str(e)}")


@app.post("/api/report/download")
async def download_report():
    """Download report PDF on-demand without saving it to local server storage."""
    global current_dataframe, analyzer, llm_engine, current_report, current_dataset, current_stats

    if current_dataframe is None:
        raise HTTPException(status_code=400, detail="No dataset uploaded. Please upload a file first.")

    try:
        if current_report is None:
            if current_stats is None:
                current_stats = await asyncio.to_thread(
                    analyzer.get_statistics if analyzer is not None else Analyzer(current_dataframe).get_statistics
                )
            stats = current_stats
            current_report = await asyncio.to_thread(generate_report, current_dataframe, stats, llm_engine.key_manager)

        pdf_bytes = await asyncio.to_thread(
            render_report_pdf_bytes,
            executive_summary=current_report.get("executive_summary", ""),
            data_quality=current_report.get("data_quality", {}),
            chart_recommendations=current_report.get("chart_recommendations", []),
            dataset_name=current_dataset.get("original_filename", "Dataset"),
            anomalies=current_anomalies or [],
        )

        source_name = (current_dataset.get("original_filename") or "dataset").rsplit(".", 1)[0]
        filename = f"insightflow_report_{source_name}.pdf"
        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
        }
        return Response(content=pdf_bytes, media_type="application/pdf", headers=headers)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error downloading report: {str(e)}")


@app.get("/dataset-info")
async def get_dataset_info():
    """Get current dataset metadata"""
    if not current_dataset:
        raise HTTPException(status_code=400, detail="No dataset loaded")
    
    return {
        "success": True,
        "metadata": current_dataset
    }


@app.post("/clear-dataset")
async def clear_dataset():
    """Clear the current dataset from memory"""
    global current_dataframe, current_dataset, analyzer, current_report, current_stats, current_anomalies
    
    current_dataframe = None
    current_dataset = {}
    analyzer = None
    current_report = None
    current_stats = None
    current_anomalies = None
    
    return {"success": True, "message": "Dataset cleared"}


from text_analyzer import analyze_all_text_columns
from forecaster import forecast_timeseries


@app.post("/api/text-analysis")
async def text_analysis():
    """Run NLP analysis on free-text columns in the current dataset."""
    global current_dataframe, analyzer, llm_engine

    if current_dataframe is None:
        raise HTTPException(
            status_code=400,
            detail="No dataset uploaded. Please upload a file first."
        )

    try:
        structure = await asyncio.to_thread(analyzer.detect_structure)
        result = await asyncio.to_thread(
            analyze_all_text_columns,
            current_dataframe,
            structure,
            llm_engine.key_manager
        )
        return {"success": True, "text_analysis": result}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error running text analysis: {str(e)}"
        )


@app.post("/api/forecast")
async def forecast():
    """Run predictive forecasting on time series columns in the current dataset."""
    global current_dataframe, analyzer, llm_engine, current_dataset

    if current_dataframe is None:
        # Recover from in-memory reset (for example, dev hot-reload) using the
        # latest uploaded file path when available.
        file_path = current_dataset.get("file_path") if current_dataset else None

        if not file_path:
            candidates = sorted(
                [
                    p for p in UPLOAD_DIR.glob("*")
                    if p.is_file() and p.suffix.lower() in {".csv", ".xlsx"}
                ],
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            file_path = str(candidates[0]) if candidates else None

        if file_path:
            try:
                suffix = Path(file_path).suffix.lower()
                if suffix == ".csv":
                    raw_df = await asyncio.to_thread(pd.read_csv, file_path)
                else:
                    raw_df = await asyncio.to_thread(pd.read_excel, file_path)

                processor = DataProcessor()
                cleaned_df, processing_report = await asyncio.to_thread(processor.process, raw_df)

                current_dataframe = cleaned_df
                analyzer = Analyzer(cleaned_df)
                current_dataset = {
                    "original_filename": Path(file_path).name,
                    "rows": len(cleaned_df),
                    "columns": list(cleaned_df.columns),
                    "dtypes": cleaned_df.dtypes.astype(str).to_dict(),
                    "processing_report": processing_report,
                    "file_path": str(file_path),
                }
            except Exception:
                current_dataframe = None
                analyzer = None

        if current_dataframe is None:
            raise HTTPException(
                status_code=400,
                detail="No dataset uploaded. Please upload a file first."
            )

    try:
        structure = await asyncio.to_thread(analyzer.detect_structure)
        result = await asyncio.to_thread(
            forecast_timeseries,
            current_dataframe,
            structure,
            llm_engine.key_manager,
        )
        return {"success": True, "forecast": result}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error running forecast: {str(e)}"
        )


from data_quality import score_data_quality


@app.post("/api/data-quality")
async def data_quality():
    """Score data quality and explain all detected issues."""
    global current_dataframe, llm_engine

    if current_dataframe is None:
        raise HTTPException(
            status_code=400,
            detail="No dataset uploaded. Please upload a file first."
        )

    try:
        result = await asyncio.to_thread(
            score_data_quality,
            current_dataframe,
            llm_engine.key_manager,
        )
        return {"success": True, "data_quality": result}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error scoring data quality: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
