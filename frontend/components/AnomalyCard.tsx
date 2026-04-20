'use client';

import { Badge } from '@/components/ui/badge';
import { Card } from '@/components/ui/card';

interface AnomalyCardProps {
  column: string;
  value: string | number;
  severity: 'low' | 'medium' | 'high' | string;
  explanation: string;
  is_likely_error: boolean;
}

const severityStyles: Record<string, string> = {
  high: 'bg-red-500/15 text-red-300 border-red-500/30',
  medium: 'bg-yellow-500/15 text-yellow-300 border-yellow-500/30',
  low: 'bg-zinc-500/15 text-zinc-300 border-zinc-500/30',
};

export default function AnomalyCard({
  column,
  value,
  severity,
  explanation,
  is_likely_error,
}: AnomalyCardProps) {
  const style = severityStyles[severity] || severityStyles.low;

  return (
    <Card className="border-white/6 bg-[#1A1B20] p-4">
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-sm text-[#938EA0]">Column</p>
          <p className="text-base font-semibold text-white">{column}</p>
        </div>
        <Badge className={`border ${style}`}>{severity.toUpperCase()}</Badge>
      </div>

      <div className="mt-3 rounded-lg bg-white/3 p-3">
        <p className="text-xs text-[#938EA0]">Value</p>
        <p className="text-sm text-[#CAC4D7] break-all">{String(value)}</p>
      </div>

      <p className="mt-3 text-sm text-[#CAC4D7] whitespace-pre-wrap">{explanation}</p>

      <Badge
        className={`mt-3 border ${
          is_likely_error
            ? 'border-red-500/30 bg-red-500/15 text-red-300'
            : 'border-emerald-500/30 bg-emerald-500/15 text-emerald-300'
        }`}
      >
        {is_likely_error ? 'Likely data error' : 'Genuine signal'}
      </Badge>
    </Card>
  );
}
