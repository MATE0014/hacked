'use client';

import { useCallback, useState } from 'react';
import { Upload, File, AlertCircle } from 'lucide-react';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';

interface FileUploadZoneProps {
  onFileUpload: (file: File) => void;
  loading?: boolean;
}

export default function FileUploadZone({
  onFileUpload,
  loading = false,
}: FileUploadZoneProps) {
  const [isDragging, setIsDragging] = useState(false);
  const [fileName, setFileName] = useState<string | null>(null);

  const handleDragEnter = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(true);
  };

  const handleDragLeave = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
  };

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
  };

  const processFile = (file: File) => {
    if (!file.name.endsWith('.csv') && !file.name.endsWith('.xlsx')) {
      alert('Please upload a CSV or Excel file');
      return;
    }

    setFileName(file.name);
    onFileUpload(file);
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);

    const files = e.dataTransfer.files;
    const firstFile = files.item(0);
    if (firstFile) {
      processFile(firstFile);
    }
  };

  const handleFileInput = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.currentTarget.files;
    const firstFile = files?.item(0);
    if (firstFile) {
      processFile(firstFile);
    }
  };

  return (
    <Card
      className={`border-2 border-dashed p-12 text-center cursor-pointer transition-all ${
        isDragging
          ? 'border-brand-teal/60 bg-brand-teal/10'
          : 'border-white/8 bg-[#1A1B20] hover:border-brand-teal/40'
      } ${loading ? 'opacity-50 cursor-not-allowed' : ''}`}
      onDragEnter={handleDragEnter}
      onDragLeave={handleDragLeave}
      onDragOver={handleDragOver}
      onDrop={handleDrop}
    >
      <input
        type="file"
        id="file-upload"
        className="hidden"
        onChange={handleFileInput}
        accept=".csv,.xlsx,.xls"
        disabled={loading}
      />

      <label htmlFor="file-upload" className="cursor-pointer block">
        <div className="flex flex-col items-center gap-4">
          {loading ? (
            <>
              <div className="w-16 h-16 rounded-full bg-brand-teal/20 flex items-center justify-center animate-spin">
                <div className="w-12 h-12 rounded-full border-2 border-transparent border-t-brand-teal border-r-brand-teal"></div>
              </div>
              <div>
                <p className="font-heading text-lg text-white">
                  Processing your file...
                </p>
                <p className="text-sm text-[#938EA0]">
                  {fileName ? `Analyzing ${fileName}` : 'Please wait'}
                </p>
              </div>
            </>
          ) : fileName ? (
            <>
              <File className="w-16 h-16 text-brand-teal" />
              <div>
                <p className="font-heading text-lg text-white">
                  File Ready
                </p>
                <p className="text-sm text-[#938EA0]">{fileName}</p>
              </div>
            </>
          ) : (
            <>
              <Upload className="w-16 h-16 text-brand-teal" />
              <div>
                <p className="font-heading text-lg text-white">
                  Drag & drop your file here
                </p>
                <p className="text-sm text-[#938EA0]">
                  or click to select CSV / Excel
                </p>
              </div>
              <div className="mt-4 flex items-center gap-2 rounded-lg bg-white/3 p-3">
                <AlertCircle className="w-4 h-4 text-[#938EA0]" />
                <span className="text-xs text-[#938EA0]">
                  Supports: CSV and Excel files
                </span>
              </div>
            </>
          )}
        </div>
      </label>
    </Card>
  );
}
