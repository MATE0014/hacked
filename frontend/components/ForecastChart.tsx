'use client';

import { ArrowDownRight, ArrowUpRight, Minus } from 'lucide-react';
import {
  Area,
  CartesianGrid,
  ComposedChart,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { Badge } from '@/components/ui/badge';
import { Card } from '@/components/ui/card';

export interface ForecastChartData {
  date_col: string;
  value_col: string;
  n_points: number;
  n_periods_forecast: number;
  method: string;
  trend_direction: 'up' | 'down' | 'stable' | string;
  trend_pct_change: number;
  last_real_value: number;
  last_real_date: string;
  forecast_values: number[];
  forecast_dates: string[];
  confidence_low: number[];
  confidence_high: number[];
  historical_values: number[];
  historical_dates: string[];
  interpretation: string;
}

interface ForecastChartProps {
  forecast: ForecastChartData;
}

interface ChartPoint {
  date: string;
  historical?: number;
  forecastPath?: number;
  confidenceBase?: number;
  confidenceRange?: number;
}

export default function ForecastChart({ forecast }: ForecastChartProps) {
  const chartData = buildChartData(forecast);
  const trend = getTrendPresentation(forecast.trend_direction, forecast.trend_pct_change);

  return (
    <Card className="border-white/6 bg-[#1A1B20] p-6">
      <div className="mb-4 flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="text-xs uppercase tracking-wide text-[#938EA0]">{forecast.date_col}</p>
          <h3 className="font-heading text-lg text-white">Forecast: {forecast.value_col}</h3>
          <p className="mt-1 text-xs text-[#938EA0]">
            Method: {forecast.method} • {forecast.n_points} points
          </p>
        </div>

        <Badge className={trend.badgeClass}>
          <trend.Icon className="h-3.5 w-3.5" />
          {trend.label}
        </Badge>
      </div>

      <ResponsiveContainer width="100%" height={320}>
        <ComposedChart data={chartData} margin={{ top: 8, right: 16, left: 0, bottom: 8 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
          <XAxis dataKey="date" stroke="#938EA0" minTickGap={24} />
          <YAxis
            stroke="#938EA0"
            tickFormatter={(value) => formatCompactNumber(Number(value))}
            width={72}
          />
          <Tooltip
            formatter={(value, name) => [formatValue(Number(value)), labelForKey(String(name ?? ''))]}
            contentStyle={{
              backgroundColor: '#1A1B20',
              border: '1px solid rgba(255,255,255,0.08)',
              borderRadius: '8px',
            }}
            labelStyle={{ color: '#CAC4D7' }}
          />

          <Area
            type="monotone"
            dataKey="confidenceBase"
            stackId="confidence"
            stroke="none"
            fill="transparent"
            connectNulls
          />
          <Area
            type="monotone"
            dataKey="confidenceRange"
            stackId="confidence"
            stroke="none"
            fill="rgba(20, 184, 166, 0.2)"
            connectNulls
          />

          <Line
            type="monotone"
            dataKey="historical"
            stroke="#06B6D4"
            strokeWidth={2.5}
            dot={false}
            name="Historical"
            connectNulls
          />
          <Line
            type="monotone"
            dataKey="forecastPath"
            stroke="#14B8A6"
            strokeWidth={2.5}
            strokeDasharray="7 5"
            dot={false}
            name="Forecast"
            connectNulls
          />
        </ComposedChart>
      </ResponsiveContainer>

      <p className="mt-3 text-sm text-[#CAC4D7] leading-relaxed">{forecast.interpretation}</p>
    </Card>
  );
}

function buildChartData(forecast: ForecastChartData): ChartPoint[] {
  const points: ChartPoint[] = [];

  for (let i = 0; i < forecast.historical_values.length; i += 1) {
    points.push({
      date: forecast.historical_dates[i] || `hist_${i + 1}`,
      historical: Number(forecast.historical_values[i]),
    });
  }

  if (forecast.historical_values.length > 0) {
    points.push({
      date: forecast.last_real_date || 'last_real',
      historical: Number(forecast.last_real_value),
      forecastPath: Number(forecast.last_real_value),
    });
  }

  for (let i = 0; i < forecast.forecast_values.length; i += 1) {
    const low = Number(forecast.confidence_low[i]);
    const high = Number(forecast.confidence_high[i]);

    points.push({
      date: forecast.forecast_dates[i] || `period_${i + 1}`,
      forecastPath: Number(forecast.forecast_values[i]),
      confidenceBase: Number.isFinite(low) ? low : undefined,
      confidenceRange:
        Number.isFinite(low) && Number.isFinite(high)
          ? Math.max(0, high - low)
          : undefined,
    });
  }

  return points;
}

function getTrendPresentation(direction: string, pct: number) {
  if (direction === 'up') {
    return {
      Icon: ArrowUpRight,
      label: `Up ${Math.abs(pct).toFixed(1)}%`,
      badgeClass: 'border-emerald-500/35 bg-emerald-500/15 text-emerald-300',
    };
  }

  if (direction === 'down') {
    return {
      Icon: ArrowDownRight,
      label: `Down ${Math.abs(pct).toFixed(1)}%`,
      badgeClass: 'border-rose-500/35 bg-rose-500/15 text-rose-300',
    };
  }

  return {
    Icon: Minus,
    label: `Stable ${Math.abs(pct).toFixed(1)}%`,
    badgeClass: 'border-zinc-500/35 bg-zinc-500/15 text-zinc-300',
  };
}

function labelForKey(value: string): string {
  if (value === 'historical') {
    return 'Historical';
  }
  if (value === 'forecastPath') {
    return 'Forecast';
  }
  if (value === 'confidenceBase' || value === 'confidenceRange') {
    return 'Confidence';
  }
  return value;
}

function formatValue(value: number): string {
  if (!Number.isFinite(value)) {
    return '-';
  }
  return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function formatCompactNumber(value: number): string {
  if (!Number.isFinite(value)) {
    return '';
  }

  if (Math.abs(value) >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(1)}M`;
  }
  if (Math.abs(value) >= 1_000) {
    return `${(value / 1_000).toFixed(1)}K`;
  }
  return value.toFixed(0);
}