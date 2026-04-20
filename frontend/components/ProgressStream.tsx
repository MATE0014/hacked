'use client';

import { useEffect, useRef, useState } from 'react';
import { Card } from '@/components/ui/card';
import { Progress } from '@/components/ui/progress';
import { buildApiUrl } from '@/lib/api';

interface ProgressEvent {
  stage: string;
  percent: number;
  result?: any;
  message?: string;
}

interface ProgressStreamProps {
  enabled: boolean;
  onComplete?: (result: any) => void;
}

export default function ProgressStream({ enabled, onComplete }: ProgressStreamProps) {
  const [percent, setPercent] = useState(0);
  const [stage, setStage] = useState('idle');
  const [error, setError] = useState<string | null>(null);
  const eventSourceRef = useRef<EventSource | null>(null);

  useEffect(() => {
    if (!enabled) {
      return;
    }

    const source = new EventSource(buildApiUrl('/api/analyze-large/stream'));
    eventSourceRef.current = source;

    source.onmessage = (event) => {
      try {
        const payload: ProgressEvent = JSON.parse(event.data);
        setError(null);
        setPercent(payload.percent ?? 0);
        setStage(payload.stage || 'processing');

        if (payload.stage === 'complete') {
          source.close();
          onComplete?.(payload.result);
        }

        if (payload.stage === 'error') {
          setError(payload.message || 'Stream failed');
          source.close();
        }
      } catch {
        setError('Unable to parse progress event');
      }
    };

    source.onerror = () => {
      setError('Progress stream disconnected');
      source.close();
    };

    return () => {
      source.close();
    };
  }, [enabled, onComplete]);

  if (!enabled) {
    return null;
  }

  const label = stage.startsWith('chunk_')
    ? `Analyzing ${stage.split('_').join(' ')}...`
    : stage === 'complete'
    ? 'Large-file analysis complete'
    : stage === 'error'
    ? 'Large-file analysis error'
    : 'Preparing large-file analysis...';

  return (
    <Card className="border-white/6 bg-[#1A1B20] p-4">
      <div className="flex items-center justify-between gap-4">
        <p className="text-sm text-[#CAC4D7]">{label}</p>
        <p className="text-sm font-semibold text-white">{percent}%</p>
      </div>
      <Progress value={percent} className="mt-3 h-2" />
      {error && <p className="mt-2 text-xs text-red-300">{error}</p>}
    </Card>
  );
}
