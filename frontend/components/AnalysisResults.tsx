'use client';

import type { ReactNode } from 'react';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { Download, Loader2 } from 'lucide-react';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faDatabase, faHashtag, faTable, faTags } from '@fortawesome/free-solid-svg-icons';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Progress } from '@/components/ui/progress';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import AnomalyCard from '@/components/AnomalyCard';
import ForecastChart, { type ForecastChartData } from '@/components/ForecastChart';
import { buildApiUrl } from '@/lib/api';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
} from 'recharts';

interface AnalysisResultsProps {
  data: any;
}

interface AnomalyItem {
  column: string;
  value: string | number;
  severity: 'low' | 'medium' | 'high' | string;
  explanation: string;
  is_likely_error: boolean;
}

interface ReportPayload {
  executive_summary: string;
  data_quality: {
    null_percent: number;
    duplicate_percent: number;
    outlier_rate: number;
    completeness_score: number;
  };
  chart_recommendations: Array<{
    chart_type: string;
    x: string;
    y: string;
    reason: string;
  }>;
}

interface TextTopicItem {
  topic: string;
  keywords?: string[];
  frequency?: 'high' | 'medium' | 'low' | string;
}

interface TextColumnAnalysis {
  column: string;
  row_count: number;
  avg_length: number;
  sentiment?: {
    overall_sentiment?: string;
    positive_pct?: number;
    negative_pct?: number;
    neutral_pct?: number;
    confidence?: string;
    summary?: string;
  };
  topics?: {
    topics?: TextTopicItem[];
    dominant_topic?: string;
  };
}

interface TextAnalysisPayload {
  text_columns_found?: number;
  analyzed_columns?: number;
  message?: string;
  columns?: TextColumnAnalysis[];
}

interface ForecastPayload {
  timeseries_found: number;
  forecasts: ForecastChartData[];
  message: string;
}

const COLORS = ['#3b82f6', '#06b6d4', '#22d3ee', '#f59e0b', '#10b981', '#0ea5a4'];

export default function AnalysisResults({ data }: AnalysisResultsProps) {
  const [activeTab, setActiveTab] = useState('overview');

  const [anomalyLoading, setAnomalyLoading] = useState(false);
  const [anomalyError, setAnomalyError] = useState<string | null>(null);
  const [anomalies, setAnomalies] = useState<AnomalyItem[] | null>(null);

  const [reportLoading, setReportLoading] = useState(false);
  const [reportError, setReportError] = useState<string | null>(null);
  const [report, setReport] = useState<ReportPayload | null>(null);
  const [downloadLoading, setDownloadLoading] = useState(false);

  const [textAnalysisLoading, setTextAnalysisLoading] = useState(false);
  const [textAnalysisError, setTextAnalysisError] = useState<string | null>(null);
  const [textAnalysis, setTextAnalysis] = useState<TextAnalysisPayload | null>(data?.text_analysis ?? null);
  const [autoFetchAttempted, setAutoFetchAttempted] = useState(false);
  const [forecastLoading, setForecastLoading] = useState(false);
  const [forecastError, setForecastError] = useState<string | null>(null);
  const [forecastData, setForecastData] = useState<ForecastPayload | null>(data?.forecast ?? null);

  useEffect(() => {
    setTextAnalysis(data?.text_analysis ?? null);
    setTextAnalysisError(null);
    setAutoFetchAttempted(false);
    setForecastData(data?.forecast ?? null);
    setForecastError(null);
  }, [data]);

  const fetchForecast = useCallback(async () => {
    if (forecastLoading) {
      return;
    }

    setForecastLoading(true);
    setForecastError(null);

    try {
      const response = await fetch(buildApiUrl('/api/forecast'), { method: 'POST' });
      if (!response.ok) {
        let message = 'Failed to generate forecast';
        try {
          const errorPayload = await response.json();
          if (typeof errorPayload?.detail === 'string' && errorPayload.detail.trim()) {
            message = errorPayload.detail;
          }
        } catch {
          // Keep fallback message when backend error payload is not JSON.
        }
        throw new Error(message);
      }

      const payload = await response.json();
      const nextForecast: ForecastPayload = payload?.forecast ?? {
        timeseries_found: 0,
        forecasts: [],
        message: 'No forecast data returned.',
      };
      setForecastData(nextForecast);
    } catch (error) {
      setForecastError(error instanceof Error ? error.message : 'Failed to generate forecast');
    } finally {
      setForecastLoading(false);
    }
  }, [forecastLoading]);

  const fetchTextAnalysis = useCallback(async () => {
    if (textAnalysisLoading) {
      return;
    }

    setTextAnalysisLoading(true);
    setTextAnalysisError(null);

    try {
      const response = await fetch(buildApiUrl('/api/text-analysis'), { method: 'POST' });
      if (!response.ok) {
        throw new Error('Failed to load text analysis');
      }

      const payload = await response.json();
      const nextTextAnalysis: TextAnalysisPayload = payload?.text_analysis ?? {
        text_columns_found: 0,
        columns: [],
      };
      setTextAnalysis(nextTextAnalysis);
    } catch (error) {
      setTextAnalysisError(error instanceof Error ? error.message : 'Failed to load text analysis');
    } finally {
      setTextAnalysisLoading(false);
    }
  }, [textAnalysisLoading]);

  useEffect(() => {
    if (textAnalysis || textAnalysisLoading || autoFetchAttempted) {
      return;
    }

    setAutoFetchAttempted(true);
    void fetchTextAnalysis();
  }, [textAnalysis, textAnalysisLoading, autoFetchAttempted, fetchTextAnalysis]);

  const fetchAnomalies = useCallback(async () => {
    setAnomalyLoading(true);
    setAnomalyError(null);
    try {
      const response = await fetch(buildApiUrl('/api/anomalies'), { method: 'POST' });
      if (!response.ok) {
        throw new Error('Failed to load anomalies');
      }
      const payload = await response.json();
      setAnomalies(payload.anomalies || []);
    } catch (error) {
      setAnomalyError(error instanceof Error ? error.message : 'Failed to load anomalies');
    } finally {
      setAnomalyLoading(false);
    }
  }, []);

  const fetchReport = useCallback(async () => {
    setReportLoading(true);
    setReportError(null);
    try {
      const response = await fetch(buildApiUrl('/api/report'), { method: 'POST' });
      if (!response.ok) {
        throw new Error('Failed to load report');
      }
      const payload = await response.json();
      setReport(payload.report || null);
    } catch (error) {
      setReportError(error instanceof Error ? error.message : 'Failed to load report');
    } finally {
      setReportLoading(false);
    }
  }, []);

  const handleDownloadReport = useCallback(async () => {
    setDownloadLoading(true);
    setReportError(null);
    try {
      const response = await fetch(buildApiUrl('/api/report/download'), { method: 'POST' });
      if (!response.ok) {
        throw new Error('Failed to download report PDF');
      }

      const blob = await response.blob();
      const contentDisposition = response.headers.get('Content-Disposition') || '';
      const filenameMatch = contentDisposition.match(/filename=\"?([^\";]+)\"?/i);
      const filename = filenameMatch?.[1] || 'insightflow_report.pdf';

      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    } catch (error) {
      setReportError(error instanceof Error ? error.message : 'Failed to download report PDF');
    } finally {
      setDownloadLoading(false);
    }
  }, []);

  const handleTabChange = useCallback(
    (nextTab: string) => {
      setActiveTab(nextTab);

      if (nextTab === 'anomalies' && anomalies === null && !anomalyLoading) {
        void fetchAnomalies();
      }
      if (nextTab === 'report' && report === null && !reportLoading) {
        void fetchReport();
      }
    },
    [anomalies, anomalyLoading, report, reportLoading, fetchAnomalies, fetchReport]
  );

  const summaryParagraphs = useMemo(() => {
    if (!report?.executive_summary) return [];
    return report.executive_summary
      .split('\n')
      .map((part) => part.trim())
      .filter(Boolean);
  }, [report]);

  if (!data) return null;

  const metadata = data?.metadata ?? { rows: 0, columns: [] };
  const statistics = data?.statistics ?? { numeric_analysis: {} };
  const structure = data?.structure ?? { numeric_columns: [], categorical_columns: [] };
  const charts = data?.charts ?? { distributions: {}, top_categories: {} };
  const insights = typeof data?.insights === 'string' ? data.insights : 'No AI insights available yet.';
  const summary = typeof data?.summary === 'string' ? data.summary : 'Analysis summary is not available.';
  const mergedData = useMemo(() => ({ ...data, text_analysis: textAnalysis }), [data, textAnalysis]);
  const textAnalysisColumns = mergedData?.text_analysis?.columns ?? [];

  return (
    <Tabs value={activeTab} onValueChange={handleTabChange}>
      <TabsList className="mb-6 inline-flex h-auto w-auto flex-wrap items-center justify-center gap-1.5 rounded-full border border-border bg-[#1A1B20]/80 p-1.5 shadow-sm backdrop-blur-sm sm:flex-nowrap">
        <TabsTrigger 
          value="overview" 
          className="rounded-full px-6 py-2.5 text-sm font-heading font-medium tracking-wide text-[#CAC4D7] transition-all hover:text-white data-[state=active]:bg-primary data-[state=active]:text-primary-foreground data-[state=active]:shadow-sm"
        >
          Overview
        </TabsTrigger>
        <TabsTrigger 
          value="anomalies" 
          className="rounded-full px-6 py-2.5 text-sm font-heading font-medium tracking-wide text-[#CAC4D7] transition-all hover:text-white data-[state=active]:bg-primary data-[state=active]:text-primary-foreground data-[state=active]:shadow-sm"
        >
          Anomalies
        </TabsTrigger>
        <TabsTrigger 
          value="report" 
          className="rounded-full px-6 py-2.5 text-sm font-heading font-medium tracking-wide text-[#CAC4D7] transition-all hover:text-white data-[state=active]:bg-primary data-[state=active]:text-primary-foreground data-[state=active]:shadow-sm"
        >
          Report
        </TabsTrigger>
      </TabsList>

      <TabsContent value="overview" className="space-y-6">
        <Card className="border-white/6 bg-[#1A1B20] p-6">
          <div className="mb-2 flex items-center justify-between gap-3">
            <h2 className="font-heading text-2xl text-white">Analysis Summary</h2>
            <Button
              type="button"
              onClick={handleDownloadReport}
              disabled={downloadLoading}
              className="group relative overflow-hidden"
            >
              <span className="pointer-events-none absolute inset-0 bg-linear-to-r from-white/0 via-white/20 to-white/0 opacity-0 transition-opacity group-hover:opacity-100" />
              <span className="relative inline-flex items-center gap-2">
                {downloadLoading ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <Download className="h-4 w-4" />
                )}
                {downloadLoading ? 'Preparing...' : 'Download Report'}
              </span>
            </Button>
          </div>
          <p className="text-[#CAC4D7]">{summary}</p>
        </Card>

        <Card className="border-white/6 bg-linear-to-br from-[#1A1B20] to-[#16171C] p-6">
          <h2 className="font-heading text-2xl text-white mb-4">AI Insights</h2>
          <div className="prose prose-invert max-w-none text-[#CAC4D7]">
            {renderInsightsMarkdown(insights)}
          </div>
        </Card>

        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <StatCard
            label="Total Rows"
            value={metadata.rows}
            icon={<FontAwesomeIcon icon={faDatabase} className="h-7 w-7 text-brand-teal" />}
          />
          <StatCard
            label="Total Columns"
            value={metadata.columns.length}
            icon={<FontAwesomeIcon icon={faTable} className="h-7 w-7 text-brand-teal" />}
          />
          <StatCard
            label="Data Types"
            value={`${structure.numeric_columns.length} Numeric`}
            icon={<FontAwesomeIcon icon={faHashtag} className="h-7 w-7 text-brand-teal" />}
          />
          <StatCard
            label="Categorical"
            value={`${structure.categorical_columns.length} Columns`}
            icon={<FontAwesomeIcon icon={faTags} className="h-7 w-7 text-brand-teal" />}
          />
        </div>

        <Card className="border-white/6 bg-[#1A1B20] p-6">
          <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
            <div>
              <h3 className="font-heading text-xl text-white">Predictive Forecast</h3>
              <p className="mt-1 text-sm text-[#938EA0]">
                Historical values are shown as a solid line, forecast as dashed, with confidence shading.
              </p>
            </div>
            <Button
              type="button"
              variant="outline"
              onClick={() => void fetchForecast()}
              disabled={forecastLoading}
            >
              {forecastLoading ? 'Generating...' : 'Generate Forecast'}
            </Button>
          </div>

          {forecastError && (
            <p className="text-sm text-red-300">{forecastError}</p>
          )}

          {!forecastError && forecastData && forecastData.forecasts.length === 0 && (
            <p className="text-sm text-[#938EA0]">{forecastData.message || 'No forecastable time series were found.'}</p>
          )}

          {forecastData && forecastData.forecasts.length > 0 && (
            <div className="grid gap-4 xl:grid-cols-2">
              {forecastData.forecasts.map((forecastItem, idx) => (
                <ForecastChart key={`${forecastItem.value_col}-${idx}`} forecast={forecastItem} />
              ))}
            </div>
          )}
        </Card>

        {Object.keys(statistics.numeric_analysis || {}).length > 0 && (
          <Card className="border-white/6 bg-[#1A1B20] p-6">
            <h3 className="font-heading text-xl text-white mb-4">Numeric Analysis</h3>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-white/8">
                    <th className="text-left text-[#CAC4D7] py-2 px-3">Column</th>
                    <th className="text-right text-[#CAC4D7] py-2 px-3">Mean</th>
                    <th className="text-right text-[#CAC4D7] py-2 px-3">Median</th>
                    <th className="text-right text-[#CAC4D7] py-2 px-3">Std Dev</th>
                    <th className="text-right text-[#CAC4D7] py-2 px-3">Min</th>
                    <th className="text-right text-[#CAC4D7] py-2 px-3">Max</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(statistics.numeric_analysis).map(([col, stats]: any) => (
                    <tr key={col} className="border-b border-white/4">
                      <td className="text-[#CAC4D7] py-2 px-3 font-medium">{col}</td>
                      <td className="text-[#938EA0] text-right py-2 px-3">{stats.mean.toFixed(2)}</td>
                      <td className="text-[#938EA0] text-right py-2 px-3">{stats.median.toFixed(2)}</td>
                      <td className="text-[#938EA0] text-right py-2 px-3">{stats.std.toFixed(2)}</td>
                      <td className="text-[#938EA0] text-right py-2 px-3">{stats.min.toFixed(2)}</td>
                      <td className="text-[#938EA0] text-right py-2 px-3">{stats.max.toFixed(2)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>
        )}

        {charts.distributions && Object.keys(charts.distributions).length > 0 && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {Object.entries(charts.distributions).map(([colName, chartData]: any) => (
              <Card key={colName} className="border-white/6 bg-[#1A1B20] p-6">
                <h3 className="font-heading text-lg text-white mb-4">Distribution: {colName}</h3>
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={transformDistributionData(chartData)}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
                    <XAxis
                      dataKey="name"
                      stroke="#938EA0"
                      tickFormatter={formatDistributionLabel}
                      angle={-35}
                      textAnchor="end"
                      height={90}
                    />
                    <YAxis stroke="#938EA0" />
                    <Tooltip
                      labelFormatter={formatDistributionLabel}
                      contentStyle={{
                        backgroundColor: '#1A1B20',
                        border: '1px solid rgba(255,255,255,0.08)',
                        borderRadius: '8px',
                      }}
                      labelStyle={{ color: '#CAC4D7' }}
                    />
                    <Bar dataKey="value" fill="#14B8A6" />
                  </BarChart>
                </ResponsiveContainer>
              </Card>
            ))}
          </div>
        )}

        {charts.top_categories && Object.keys(charts.top_categories).length > 0 && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {Object.entries(charts.top_categories).map(([colName, categoryData]: any) => (
              <Card key={colName} className="border-white/6 bg-[#1A1B20] p-6">
                <h3 className="font-heading text-lg text-white mb-4">Top Values: {colName}</h3>
                <ResponsiveContainer width="100%" height={300}>
                  <PieChart>
                    <Pie
                      data={transformCategoryData(categoryData)}
                      cx="50%"
                      cy="50%"
                      labelLine={false}
                      label={({ name, percent }) => `${name} ${((percent ?? 0) * 100).toFixed(0)}%`}
                      outerRadius={100}
                      fill="#8884d8"
                      dataKey="value"
                    >
                      {categoryData.labels?.map((_: unknown, index: number) => (
                        <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip
                      contentStyle={{
                        backgroundColor: '#1A1B20',
                        border: '1px solid rgba(255,255,255,0.08)',
                        borderRadius: '8px',
                      }}
                      labelStyle={{ color: '#CAC4D7' }}
                    />
                  </PieChart>
                </ResponsiveContainer>
              </Card>
            ))}
          </div>
        )}

        <Card className="border-white/6 bg-[#1A1B20] p-6">
          <div className="mb-4 flex items-center justify-between gap-3">
            <h3 className="font-heading text-xl text-white">Text NLP Analysis</h3>
            <Button
              type="button"
              variant="outline"
              onClick={() => void fetchTextAnalysis()}
              disabled={textAnalysisLoading}
            >
              {textAnalysisLoading ? 'Refreshing...' : 'Refresh NLP'}
            </Button>
          </div>

          <p className="mb-4 text-xs text-[#938EA0]">
            Topics show what people are talking about. Sentiment shows how they felt while discussing those topics,
            so issue labels can still appear in mostly positive feedback.
          </p>

          {textAnalysisLoading && !textAnalysis && (
            <p className="text-sm text-[#CAC4D7]">Running text analysis...</p>
          )}

          {textAnalysisError && (
            <p className="text-sm text-red-300">{textAnalysisError}</p>
          )}

          {!textAnalysisLoading && !textAnalysisError && textAnalysisColumns.length === 0 && (
            <p className="text-sm text-[#938EA0]">
              {mergedData?.text_analysis?.message || 'No free-text columns were detected for NLP analysis.'}
            </p>
          )}

          {textAnalysisColumns.length > 0 && (
            <div className="grid gap-4 xl:grid-cols-2">
              {textAnalysisColumns.map((columnData: TextColumnAnalysis, idx: number) => {
                const donutData = [
                  { name: 'Positive', value: Number(columnData.sentiment?.positive_pct ?? 0), color: '#10b981' },
                  { name: 'Neutral', value: Number(columnData.sentiment?.neutral_pct ?? 0), color: '#f59e0b' },
                  { name: 'Negative', value: Number(columnData.sentiment?.negative_pct ?? 0), color: '#ef4444' },
                ];

                return (
                  <div key={`${columnData.column}-${idx}`} className="rounded-xl border border-white/8 bg-white/2 p-4">
                    <div className="mb-3 flex items-start justify-between gap-3">
                      <div>
                        <p className="text-xs uppercase tracking-wide text-[#938EA0]">Column</p>
                        <p className="text-lg font-semibold text-white">{columnData.column}</p>
                      </div>
                      <div className="text-right">
                        <p className="text-xs text-[#938EA0]">Overall tone</p>
                        <p className="text-sm font-medium text-white capitalize">
                          {columnData.sentiment?.overall_sentiment || 'mixed'}
                        </p>
                      </div>
                    </div>

                    <div className="grid gap-4 sm:grid-cols-[190px_1fr] sm:items-center">
                      <ResponsiveContainer width="100%" height={190}>
                        <PieChart>
                          <Pie
                            data={donutData}
                            dataKey="value"
                            nameKey="name"
                            cx="50%"
                            cy="50%"
                            innerRadius={54}
                            outerRadius={78}
                            stroke="rgba(255,255,255,0.08)"
                            strokeWidth={1}
                          >
                            {donutData.map((item) => (
                              <Cell key={item.name} fill={item.color} />
                            ))}
                          </Pie>
                          <Tooltip
                            formatter={(value) => `${value ?? 0}%`}
                            contentStyle={{
                              backgroundColor: '#1A1B20',
                              border: '1px solid rgba(255,255,255,0.08)',
                              borderRadius: '8px',
                            }}
                            labelStyle={{ color: '#CAC4D7' }}
                          />
                        </PieChart>
                      </ResponsiveContainer>

                      <div className="space-y-3">
                        <div className="grid grid-cols-3 gap-2 text-xs">
                          {donutData.map((item) => (
                            <div key={`${columnData.column}-${item.name}`} className="rounded-md bg-white/4 p-2 text-center">
                              <p className="text-[#938EA0]">{item.name}</p>
                              <p className="font-semibold text-white">{item.value}%</p>
                            </div>
                          ))}
                        </div>

                        <div>
                          <p className="text-xs uppercase tracking-wide text-[#938EA0]">Topics (what was discussed)</p>
                          <p className="mt-1 text-[11px] text-[#938EA0]">
                            Topic names are themes, not good or bad labels.
                          </p>
                          <div className="mt-2 flex flex-wrap gap-2">
                            {(columnData.topics?.topics || []).map((topic: TextTopicItem, topicIdx: number) => (
                              <span
                                key={`${columnData.column}-topic-${topicIdx}`}
                                className={`inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-medium ${topicFrequencyClass(topic.frequency)}`}
                              >
                                {topic.topic}
                              </span>
                            ))}
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </Card>
      </TabsContent>

      <TabsContent value="anomalies" className="space-y-4">
        {anomalyLoading && (
          <Card className="border-white/6 bg-[#1A1B20] p-4">
            <p className="text-sm text-[#CAC4D7]">Loading anomaly explanations...</p>
          </Card>
        )}

        {anomalyError && (
          <Card className="border-red-500/30 bg-red-500/10 p-4">
            <p className="text-sm text-red-300">{anomalyError}</p>
          </Card>
        )}

        {!anomalyLoading && !anomalyError && (anomalies || []).length === 0 && (
          <Card className="border-white/6 bg-[#1A1B20] p-4">
            <p className="text-sm text-[#CAC4D7]">No anomaly explanations were returned.</p>
          </Card>
        )}

        <div className="grid gap-4 md:grid-cols-2">
          {(anomalies || []).map((item, idx) => (
            <AnomalyCard
              key={`${item.column}-${idx}`}
              column={item.column}
              value={item.value}
              severity={item.severity}
              explanation={item.explanation}
              is_likely_error={item.is_likely_error}
            />
          ))}
        </div>
      </TabsContent>

      <TabsContent value="report" className="space-y-4">
        {reportLoading && (
          <Card className="border-white/6 bg-[#1A1B20] p-4">
            <p className="text-sm text-[#CAC4D7]">Generating storytelling report...</p>
          </Card>
        )}

        {reportError && (
          <Card className="border-red-500/30 bg-red-500/10 p-4">
            <p className="text-sm text-red-300">{reportError}</p>
          </Card>
        )}

        {!reportLoading && report && (
          <>
            <Card className="border-white/6 bg-[#1A1B20] p-6">
              <h3 className="font-heading text-xl text-white mb-4">Executive Summary</h3>
              <div className="space-y-3 text-[#CAC4D7]">
                {summaryParagraphs.map((paragraph, idx) => (
                  <p key={idx} className="leading-relaxed">
                    {paragraph}
                  </p>
                ))}
              </div>
            </Card>

            <Card className="border-white/6 bg-[#1A1B20] p-6">
              <div className="flex items-center justify-between gap-4 mb-3">
                <h3 className="font-heading text-xl text-white">Data Quality Score</h3>
                <span className="text-lg font-bold text-white">
                  {report.data_quality?.completeness_score ?? 0}/100
                </span>
              </div>
              <Progress value={report.data_quality?.completeness_score ?? 0} className="h-2" />
              <div className="mt-4 grid gap-3 sm:grid-cols-3 text-sm">
                <MetricTile label="Null %" value={`${report.data_quality?.null_percent ?? 0}%`} />
                <MetricTile label="Duplicate %" value={`${report.data_quality?.duplicate_percent ?? 0}%`} />
                <MetricTile label="Outlier Rate" value={`${report.data_quality?.outlier_rate ?? 0}%`} />
              </div>
            </Card>

            <Card className="border-white/6 bg-[#1A1B20] p-6">
              <div className="mb-4 flex items-center justify-between gap-3">
                <h3 className="font-heading text-xl text-white">Chart Recommendations</h3>
              </div>
              <div className="grid gap-3 md:grid-cols-3">
                {(report.chart_recommendations || []).map((rec, idx) => (
                  <button
                    key={`${rec.chart_type}-${idx}`}
                    type="button"
                    className="relative isolate overflow-hidden text-left rounded-xl p-4 text-white transition-colors before:pointer-events-none before:absolute before:inset-0 before:bg-linear-to-r before:from-white/0 before:via-white/20 before:to-white/0 before:opacity-0 before:transition-opacity hover:bg-white/6 hover:before:opacity-100"
                  >
                    <p className="text-sm font-semibold text-white">{rec.chart_type}</p>
                    <p className="mt-1 text-xs text-[#938EA0]">x: {rec.x}</p>
                    <p className="text-xs text-[#938EA0]">y: {rec.y}</p>
                    <p className="mt-2 text-sm text-[#CAC4D7]">{rec.reason}</p>
                  </button>
                ))}
              </div>
            </Card>
          </>
        )}
      </TabsContent>
    </Tabs>
  );
}

function StatCard({ label, value, icon }: { label: string; value: string | number; icon: ReactNode }) {
  return (
    <Card className="border-white/6 bg-[#1A1B20] p-4">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-[#938EA0] text-sm">{label}</p>
          <p className="font-heading text-2xl text-white">{value}</p>
        </div>
        <span>{icon}</span>
      </div>
    </Card>
  );
}

function MetricTile({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg bg-white/3 p-3">
      <p className="text-[#938EA0]">{label}</p>
      <p className="text-white font-semibold">{value}</p>
    </div>
  );
}

function transformDistributionData(data: any) {
  return data.labels?.map((label: string, idx: number) => ({ name: label, value: data.values[idx] })) || [];
}

function formatDistributionLabel(label: unknown) {
  const normalizedLabel = typeof label === 'string' ? label : String(label ?? '');
  if (!normalizedLabel) return '';

  // Pandas bin labels are usually formatted like "(10.2, 20.5]"; shorten to improve chart readability.
  const match = normalizedLabel.match(/[\[(]\s*([-+]?\d*\.?\d+)\s*,\s*([-+]?\d*\.?\d+)\s*[\])]/);
  if (match) {
    const start = Number(match[1]);
    const end = Number(match[2]);
    if (Number.isFinite(start) && Number.isFinite(end)) {
      return `${start.toFixed(1)}-${end.toFixed(1)}`;
    }
  }

  return normalizedLabel;
}

function transformCategoryData(data: any) {
  return data.labels?.map((label: string, idx: number) => ({ name: label, value: data.values[idx] })) || [];
}

function renderInsightsMarkdown(markdown: string) {
  const blocks = markdown
    .split(/\n\s*\n/)
    .map((block) => block.trim())
    .filter(Boolean);

  return blocks.map((block, idx) => {
    const lines = block
      .split('\n')
      .map((line) => line.trim())
      .filter(Boolean);

    const isList = lines.every((line) => /^([-*]|\d+\.)\s+/.test(line));

    if (isList) {
      const isOrdered = lines.every((line) => /^\d+\.\s+/.test(line));
      const ListTag = isOrdered ? 'ol' : 'ul';

      return (
        <ListTag key={`insights-list-${idx}`} className="my-4 pl-5 marker:text-[#938EA0]">
          {lines.map((line, itemIdx) => {
            const content = line.replace(/^([-*]|\d+\.)\s+/, '');
            return (
              <li key={`insights-list-item-${idx}-${itemIdx}`} className="my-1 leading-relaxed text-[#CAC4D7]">
                {renderInlineMarkdown(content)}
              </li>
            );
          })}
        </ListTag>
      );
    }

    return (
      <p key={`insights-paragraph-${idx}`} className="my-4 leading-relaxed text-[#CAC4D7]">
        {lines.map((line, lineIdx) => (
          <span key={`insights-line-${idx}-${lineIdx}`}>
            {lineIdx > 0 && <br />}
            {renderInlineMarkdown(line)}
          </span>
        ))}
      </p>
    );
  });
}

function renderInlineMarkdown(text: string): ReactNode[] {
  const segments = text.split(/(\*\*[^*]+\*\*)/g).filter(Boolean);

  return segments.map((segment, idx) => {
    if (segment.startsWith('**') && segment.endsWith('**')) {
      return (
        <strong key={`insights-strong-${idx}`} className="font-semibold text-white">
          {segment.slice(2, -2)}
        </strong>
      );
    }

    return <span key={`insights-text-${idx}`}>{segment}</span>;
  });
}

function topicFrequencyClass(frequency?: string) {
  if (frequency === 'high') {
    return 'border-emerald-500/30 bg-emerald-500/15 text-emerald-200';
  }
  if (frequency === 'medium') {
    return 'border-amber-500/30 bg-amber-500/15 text-amber-200';
  }
  if (frequency === 'low') {
    return 'border-sky-500/30 bg-sky-500/15 text-sky-200';
  }
  return 'border-white/15 bg-white/8 text-[#E3E2E8]';
}
