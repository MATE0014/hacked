import { create } from "zustand";
import { createJSONStorage, persist } from "zustand/middleware";

export type AnalysisResult = {
  success?: boolean;
  metadata?: {
    original_filename?: string;
    rows?: number;
    columns?: string[];
  };
  summary?: string;
  insights?: string;
  [key: string]: unknown;
};

type AnalysisState = {
  latestAnalysis: AnalysisResult | null;
  hasHydrated: boolean;
  setHasHydrated: (state: boolean) => void;
  setLatestAnalysis: (analysis: AnalysisResult) => void;
  clearLatestAnalysis: () => void;
};

const MAX_INSIGHTS_LENGTH = 2000;

function compactAnalysisForStorage(analysis: AnalysisResult): AnalysisResult {
  const metadata = analysis?.metadata ?? {};

  return {
    success: analysis?.success,
    metadata: {
      original_filename: metadata?.original_filename,
      rows: metadata?.rows,
      columns: Array.isArray(metadata?.columns) ? metadata.columns.slice(0, 200) : [],
    },
    summary: typeof analysis?.summary === "string" ? analysis.summary : undefined,
    insights:
      typeof analysis?.insights === "string"
        ? analysis.insights.slice(0, MAX_INSIGHTS_LENGTH)
        : undefined,
  };
}

export const useAnalysisStore = create<AnalysisState>()(
  persist(
    (set) => ({
      latestAnalysis: null,
      hasHydrated: false,
      setHasHydrated: (state) => set({ hasHydrated: state }),
      setLatestAnalysis: (analysis) => set({ latestAnalysis: compactAnalysisForStorage(analysis) }),
      clearLatestAnalysis: () => set({ latestAnalysis: null }),
    }),
    {
      name: "insightflow-analysis",
      storage: createJSONStorage(() => localStorage),
      partialize: (state) => ({
        latestAnalysis: state.latestAnalysis
          ? compactAnalysisForStorage(state.latestAnalysis)
          : null,
      }),
      onRehydrateStorage: () => (state) => {
        state?.setHasHydrated(true);
      },
    }
  )
);
