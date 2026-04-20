'use client';

import { useEffect, useState } from 'react';
import { RefreshCw } from 'lucide-react';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faDatabase, faWandMagicSparkles } from '@fortawesome/free-solid-svg-icons';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { Progress } from '@/components/ui/progress';
import FileUploadZone from '@/components/FileUploadZone';
import AnalysisResults from '@/components/AnalysisResults';
import ChatInterface from '@/components/ChatInterface';
import ProgressStream from '@/components/ProgressStream';
import { useAnalysisStore } from '@/lib/analysis-store';
import { API_BASE_URL, buildApiUrl } from '@/lib/api';

const LARGE_FILE_THRESHOLD_BYTES = 50 * 1024 * 1024;
const CHUNK_UPLOAD_THRESHOLD_BYTES = 4 * 1024 * 1024;
const CHUNK_UPLOAD_PART_BYTES = 3 * 1024 * 1024;
const PREFER_CHUNK_UPLOAD = API_BASE_URL.startsWith('/_/backend');
type UploadPhase = 'idle' | 'uploading' | 'processing-upload' | 'analyzing';

const formatBytes = (bytes: number) => {
  if (!Number.isFinite(bytes) || bytes <= 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const exponent = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const value = bytes / 1024 ** exponent;
  return `${value >= 100 ? value.toFixed(0) : value.toFixed(1)} ${units[exponent]}`;
};

const clampProgress = (value: number) => Math.min(100, Math.max(0, value));

const uploadWithProgress = (
  file: File,
  onProgress: (loaded: number, total: number) => void,
): Promise<void> =>
  new Promise((resolve, reject) => {
    const parseFailedUploadMessage = (status: number, responseText: string) => {
      let message = `Failed to upload file (HTTP ${status || 'unknown'})`;

      try {
        const parsed = JSON.parse(responseText || '{}');
        if (typeof parsed?.detail === 'string' && parsed.detail.trim()) {
          return parsed.detail;
        }
      } catch {
        // Fall back to raw text below.
      }

      const text = (responseText || '').trim();
      if (text) {
        return `${message}: ${text.slice(0, 200)}`;
      }

      return message;
    };

    const uploadSingleRequest = () => {
      const formData = new FormData();
      formData.append('file', file);

      const xhr = new XMLHttpRequest();
      xhr.open('POST', buildApiUrl('/upload'));

      xhr.upload.onprogress = (event) => {
        if (event.lengthComputable) {
          onProgress(event.loaded, event.total);
        }
      };

      xhr.onload = () => {
        if (xhr.status >= 200 && xhr.status < 300) {
          resolve();
          return;
        }

        if (xhr.status === 413) {
          uploadChunked().then(resolve).catch(reject);
          return;
        }

        reject(new Error(parseFailedUploadMessage(xhr.status, xhr.responseText || '')));
      };

      xhr.onerror = () => reject(new Error('Upload failed due to a network error'));
      xhr.onabort = () => reject(new Error('Upload was canceled'));

      xhr.send(formData);
    };

    const uploadChunked = async () => {
      const uploadId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
      const totalChunks = Math.max(1, Math.ceil(file.size / CHUNK_UPLOAD_PART_BYTES));

      for (let chunkIndex = 0; chunkIndex < totalChunks; chunkIndex += 1) {
        const start = chunkIndex * CHUNK_UPLOAD_PART_BYTES;
        const end = Math.min(file.size, start + CHUNK_UPLOAD_PART_BYTES);
        const chunkBlob = file.slice(start, end);

        const chunkFormData = new FormData();
        chunkFormData.append('upload_id', uploadId);
        chunkFormData.append('chunk_index', String(chunkIndex));
        chunkFormData.append('total_chunks', String(totalChunks));
        chunkFormData.append('filename', file.name);
        chunkFormData.append('file', chunkBlob, `${file.name}.part${chunkIndex}`);

        const chunkResponse = await fetch(buildApiUrl('/upload/chunk'), {
          method: 'POST',
          body: chunkFormData,
        });

        if (!chunkResponse.ok) {
          const responseText = await chunkResponse.text();
          throw new Error(parseFailedUploadMessage(chunkResponse.status, responseText));
        }

        onProgress(end, file.size);
      }

      const completeFormData = new FormData();
      completeFormData.append('upload_id', uploadId);

      const completeResponse = await fetch(buildApiUrl('/upload/complete'), {
        method: 'POST',
        body: completeFormData,
      });

      if (!completeResponse.ok) {
        const responseText = await completeResponse.text();
        throw new Error(parseFailedUploadMessage(completeResponse.status, responseText));
      }

      onProgress(file.size, file.size);
    };

    if (PREFER_CHUNK_UPLOAD || file.size > CHUNK_UPLOAD_THRESHOLD_BYTES) {
      uploadChunked().then(resolve).catch(reject);
      return;
    }

    uploadSingleRequest();
  });

export default function DataAnalyzerPage() {
  const [file, setFile] = useState<File | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [analysisData, setAnalysisData] = useState<any>(null);
  const [showChat, setShowChat] = useState(false);
  const [runLargeStream, setRunLargeStream] = useState(false);
  const [uploadPhase, setUploadPhase] = useState<UploadPhase>('idle');
  const [uploadProgress, setUploadProgress] = useState(0);
  const [uploadedBytes, setUploadedBytes] = useState(0);
  const [uploadTotalBytes, setUploadTotalBytes] = useState(0);
  const hasReliableByteProgress = uploadTotalBytes > 0 && uploadedBytes > 0 && uploadedBytes <= uploadTotalBytes;
  const byteUploadProgress = uploadTotalBytes > 0 ? (uploadedBytes / uploadTotalBytes) * 100 : 0;
  const displayedProgress = uploadPhase === 'uploading' ? clampProgress(byteUploadProgress) : uploadProgress;
  const setLatestAnalysis = useAnalysisStore((state) => state.setLatestAnalysis);
  const clearLatestAnalysis = useAnalysisStore((state) => state.clearLatestAnalysis);

  useEffect(() => {
    if (uploadPhase === 'idle') return;

    if (uploadPhase === 'uploading') {
      // During upload, progress should be driven only by real byte progress events.
      return;
    }

    if (uploadPhase === 'processing-upload') {
      const timer = window.setInterval(() => {
        setUploadProgress((prev) => {
          if (prev >= 88) return prev;
          return Math.min(88, prev + Math.max(0.5, (88 - prev) / 6));
        });
      }, 240);

      return () => window.clearInterval(timer);
    }

    const ceiling = 97;
    const timer = window.setInterval(() => {
      setUploadProgress((prev) => {
        if (prev >= ceiling) return prev;
        return Math.min(ceiling, prev + Math.max(1, Math.floor((ceiling - prev) / 10)));
      });
    }, 250);

    return () => window.clearInterval(timer);
  }, [uploadPhase]);

  const shouldAutoChunkAnalyze = (uploadedFile: File) => {
    return uploadedFile.name.toLowerCase().endsWith('.csv') && uploadedFile.size > LARGE_FILE_THRESHOLD_BYTES;
  };

  const handleFileUpload = async (uploadedFile: File) => {
    setFile(uploadedFile);
    setLoading(true);
    setError(null);
    setUploadPhase('uploading');
    setUploadProgress(0);
    setUploadedBytes(0);
    setUploadTotalBytes(uploadedFile.size || 0);

    try {
      await uploadWithProgress(uploadedFile, (loaded, total) => {
        setUploadedBytes(loaded);
        setUploadTotalBytes(total);
        if (total > 0 && loaded >= total) {
          setUploadProgress((prev) => Math.max(prev, 70));
          setUploadPhase('processing-upload');
        }
      });

      setUploadPhase('analyzing');
      setUploadProgress((prev) => Math.max(prev, 90));

      // Perform analysis
      const analysisResponse = await fetch(buildApiUrl('/analyze'));

      if (!analysisResponse.ok) {
        throw new Error('Failed to analyze dataset');
      }

      const analysis = await analysisResponse.json();
      setAnalysisData(analysis);
      setLatestAnalysis(analysis);
      setShowChat(true);
      setRunLargeStream(shouldAutoChunkAnalyze(uploadedFile));
      setUploadProgress(100);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
      console.error(err);
    } finally {
      setLoading(false);
      window.setTimeout(() => {
        setUploadPhase('idle');
        setUploadProgress(0);
        setUploadedBytes(0);
        setUploadTotalBytes(0);
      }, 400);
    }
  };

  const handleClearDataset = async () => {
    try {
      await fetch(buildApiUrl('/clear-dataset'), { method: 'POST' });
      setAnalysisData(null);
      setFile(null);
      setShowChat(false);
      setError(null);
      setRunLargeStream(false);
      clearLatestAnalysis();
    } catch (err) {
      setError('Failed to clear dataset');
    }
  };

  const handleLargeStreamComplete = (result: any) => {
    const merged = {
      ...analysisData,
      large_file_result: result,
    };
    setAnalysisData(merged);
    setLatestAnalysis(merged);
    setRunLargeStream(false);
  };

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h2 className="font-heading text-2xl text-white">Analyzer</h2>
          <p className="mt-1 text-sm text-[#938EA0]">
            Upload a dataset and generate AI-powered statistical insights.
          </p>
        </div>
        {analysisData && (
          <div className="flex items-center gap-2">
            <Button
              onClick={handleClearDataset}
              variant="outline"
              className="gap-2"
            >
              <RefreshCw className="h-4 w-4" />
              New Analysis
            </Button>
          </div>
        )}
      </div>

      {error && (
        <Card className="border-red-500/20 bg-red-500/10 p-4">
          <p className="text-sm text-red-300">{error}</p>
        </Card>
      )}

      {loading && (
        <Card className="border-white/6 bg-linear-to-r from-[#1A1B20] to-[#16171C] p-4">
          <div className="flex items-center justify-between gap-3">
            <div className="flex items-center gap-2">
              <span className="inline-block h-2.5 w-2.5 rounded-full bg-brand-teal animate-pulse" aria-hidden="true" />
              <p className="font-heading text-lg text-white">
                {uploadPhase === 'uploading'
                  ? 'Uploading file...'
                  : uploadPhase === 'processing-upload'
                    ? 'Finalizing upload on server...'
                    : 'Analyzing file...'}
              </p>
            </div>
            <p className="text-sm font-semibold text-[#CAC4D7]">
              {uploadPhase === 'processing-upload' ? 'Working...' : `${Math.round(displayedProgress)}%`}
            </p>
          </div>
          {(uploadPhase === 'uploading' || uploadPhase === 'processing-upload') && hasReliableByteProgress && (
            <p className="mt-1 text-xs text-[#938EA0]">
              {formatBytes(uploadedBytes)} / {formatBytes(uploadTotalBytes || (file?.size ?? 0))}
            </p>
          )}
          <Progress value={displayedProgress} className="mt-3 h-2.5 rounded-full" />
        </Card>
      )}

      {!analysisData ? (
        <div className="space-y-6">
          <FileUploadZone
            onFileUpload={handleFileUpload}
            loading={loading}
          />

          <Card className="border-white/6 bg-[#1A1B20] p-6">
            <div className="mb-5 flex items-center gap-2.5">
              <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-brand-teal/15">
                <FontAwesomeIcon icon={faWandMagicSparkles} className="h-4 w-4 text-brand-teal" />
              </div>
              <h3 className="font-heading text-lg text-white">Quick Start</h3>
            </div>
            <div className="grid gap-4 md:grid-cols-3">
              <div className="rounded-xl border border-white/6 bg-white/2 p-4">
                <div className="mb-2 flex items-center gap-2">
                  <div className="flex h-7 w-7 items-center justify-center rounded-full bg-brand-teal/15 text-xs font-semibold text-brand-teal">
                    1
                  </div>
                  <p className="text-sm font-medium text-white">Upload Data</p>
                </div>
                <p className="text-xs text-[#938EA0]">CSV or Excel files are supported.</p>
              </div>
              <div className="rounded-xl border border-white/6 bg-white/2 p-4">
                <div className="mb-2 flex items-center gap-2">
                  <div className="flex h-7 w-7 items-center justify-center rounded-full bg-brand-teal/15 text-xs font-semibold text-brand-teal">
                    2
                  </div>
                  <p className="text-sm font-medium text-white">Run Analysis</p>
                </div>
                <p className="text-xs text-[#938EA0]">The backend profiles and validates your dataset.</p>
              </div>
              <div className="rounded-xl border border-white/6 bg-white/2 p-4">
                <div className="mb-2 flex items-center gap-2">
                  <div className="flex h-7 w-7 items-center justify-center rounded-full bg-brand-teal/15 text-xs font-semibold text-brand-teal">
                    3
                  </div>
                  <p className="text-sm font-medium text-white">Explore Insights</p>
                </div>
                <p className="text-xs text-[#938EA0]">Review charts, summary stats, and ask follow-up questions.</p>
              </div>
            </div>
          </Card>

          <div className="grid gap-4 sm:grid-cols-2">
            <Card className="border-white/6 bg-[#1A1B20] p-4">
              <div className="flex items-start gap-3">
                <div className="mt-0.5 flex h-8 w-8 items-center justify-center rounded-lg bg-brand-teal/15">
                  <FontAwesomeIcon icon={faDatabase} className="h-4 w-4 text-brand-teal" />
                </div>
                <div>
                  <p className="text-sm font-medium text-white">Supported Inputs</p>
                  <p className="mt-1 text-xs text-[#938EA0]">Use CSV, XLS, or XLSX files for best compatibility.</p>
                </div>
              </div>
            </Card>
            <Card className="border-white/6 bg-[#1A1B20] p-4">
              <div className="flex items-start gap-3">
                <div className="mt-0.5 flex h-8 w-8 items-center justify-center rounded-lg bg-brand-teal/15">
                  <FontAwesomeIcon icon={faWandMagicSparkles} className="h-4 w-4 text-brand-teal" />
                </div>
                <div>
                  <p className="text-sm font-medium text-white">AI Summaries</p>
                  <p className="mt-1 text-xs text-[#938EA0]">Narrative findings are generated from computed dataset metrics.</p>
                </div>
              </div>
            </Card>
          </div>
        </div>
      ) : (
        <div className="space-y-6">
          <ProgressStream enabled={runLargeStream} onComplete={handleLargeStreamComplete} />
          <AnalysisResults data={analysisData} />

          {showChat && (
            <Card className="border-white/6 bg-[#1A1B20]">
              <ChatInterface />
            </Card>
          )}
        </div>
      )}
    </div>
  );
}