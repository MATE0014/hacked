"use client";
import { useRef } from "react";
import { useRouter } from "next/navigation";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import {
  faBolt,
  faBroom,
  faChartLine,
  faComments,
  faFileLines,
  faMagnifyingGlass,
} from "@fortawesome/free-solid-svg-icons";

const capabilities = [
  {
    icon: faBroom,
    title: "Automatic Data Cleaning",
    description:
      "Removes duplicates, fills missing values with statistical imputation, " +
      "infers correct data types, and strips whitespace - before any analysis runs.",
  },
  {
    icon: faMagnifyingGlass,
    title: "Data Quality Scoring",
    description:
      "Detects label leakage, class imbalance, constant columns, " +
      "duplicate-but-renamed columns, and wrong data types. Scores your dataset " +
      "0-100 and explains every issue in plain English.",
  },
  {
    icon: faChartLine,
    title: "Predictive Forecasting",
    description:
      "Auto-detects time series columns and runs Holt-Winters forecasting " +
      "with 80% confidence bands. Tells you trend direction, % change, " +
      "and one concrete business action to take.",
  },
  {
    icon: faComments,
    title: "Natural Language Queries",
    description:
      "Ask questions in plain English. A LangChain Pandas agent writes and runs " +
      "actual Python on your dataframe - exact computed answers, not AI guesses. " +
      "Remembers conversation context for follow-up questions.",
  },
  {
    icon: faFileLines,
    title: "NLP Text Analysis",
    description:
      "Automatically detects free-text columns and runs sentiment analysis, " +
      "topic extraction, and key phrase detection. PowerBI treats text as a " +
      "category label - we understand what it means.",
  },
  {
    icon: faBolt,
    title: "Large File Processing",
    description:
      "Handles 100GB+ files by chunking and processing in parallel across " +
      "9 Groq API keys simultaneously. Synthesizes findings across all chunks " +
      "into a single executive report.",
  },
];

export default function AnalyticsPage() {
  const router = useRouter();
  const featuresRef = useRef<HTMLDivElement>(null);

  const scrollToFeatures = () => {
    featuresRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
  };

  return (
    <div
      style={{
        maxWidth: 1100,
        margin: "0 auto",
        color: "#fff",
        fontFamily: "var(--font-dm-sans), sans-serif",
      }}
    >
      <div
        style={{
          minHeight: "calc(100vh - 64px)",
          display: "flex",
          flexDirection: "column",
          justifyContent: "center",
          alignItems: "center",
          gap: 12,
          textAlign: "center",
        }}
      >
        <h2
          style={{
            fontSize: 48,
            fontWeight: 800,
            marginBottom: 6,
            fontFamily: "var(--font-syne), sans-serif",
            lineHeight: 1.05,
          }}
        >
          Analytical History Coming Soon
        </h2>
        <p style={{ color: "#888", fontSize: 18, maxWidth: 700 }}>
          Till then, look at our features below.
        </p>
        <button
          onClick={scrollToFeatures}
          className="relative isolate mt-2 w-full overflow-hidden rounded-full border border-brand-teal/60 bg-transparent px-6 py-3 font-heading text-base font-semibold tracking-wide text-brand-teal transition before:pointer-events-none before:absolute before:inset-0 before:bg-linear-to-r before:from-white/0 before:via-white/20 before:to-white/0 before:opacity-0 before:transition-opacity hover:border-brand-teal hover:bg-brand-teal/10 hover:text-[#8CF2E5] hover:before:opacity-100 sm:w-auto"
        >
          View Features
        </button>
      </div>

      <div ref={featuresRef} style={{ marginBottom: 24 }}>
        <h3
          style={{
            marginTop: 16,
            marginBottom: 6,
            fontSize: 24,
            fontWeight: 800,
            fontFamily: "var(--font-syne), sans-serif",
          }}
        >
          Current Capabilities
        </h3>
        <p style={{ color: "#888", fontSize: 14 }}>
          Everything InsightFlow does automatically when you upload a dataset.
        </p>
      </div>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))",
          gap: 16,
        }}
      >
        {capabilities.map((cap) => (
          <div
            key={cap.title}
            style={{
              background: "#141414",
              border: "1px solid rgba(255,255,255,0.08)",
              borderRadius: 12,
              padding: 16,
            }}
          >
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
              <span style={{ fontSize: 20, color: "#14B8A6" }}>
                <FontAwesomeIcon icon={cap.icon} />
              </span>
            </div>

            <h3
              style={{
                fontSize: 17,
                fontWeight: 700,
                marginBottom: 8,
                color: "#fff",
                fontFamily: "var(--font-syne), sans-serif",
              }}
            >
              {cap.title}
            </h3>

            <p style={{ color: "#888", fontSize: 14, lineHeight: 1.6 }}>{cap.description}</p>
          </div>
        ))}
      </div>

      <div
        style={{
          marginTop: 24,
          background: "#141414",
          border: "1px solid rgba(255,255,255,0.08)",
          borderRadius: 12,
          padding: 20,
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 12,
          flexWrap: "wrap",
        }}
      >
        <div>
          <h4
            style={{
              margin: 0,
              fontSize: 18,
              color: "#fff",
              fontFamily: "var(--font-syne), sans-serif",
            }}
          >
            Ready to see these in action?
          </h4>
          <p style={{ margin: "6px 0 0", color: "#888", fontSize: 14 }}>
            Upload any CSV or Excel file and all 6 run automatically.
          </p>
        </div>

        <button
          onClick={() => router.push("/analyzer")}
          className="relative isolate mt-1 w-full overflow-hidden rounded-full border border-brand-teal/60 bg-transparent px-6 py-3 font-heading text-base font-semibold tracking-wide text-brand-teal transition before:pointer-events-none before:absolute before:inset-0 before:bg-linear-to-r before:from-white/0 before:via-white/20 before:to-white/0 before:opacity-0 before:transition-opacity hover:border-brand-teal hover:bg-brand-teal/10 hover:text-[#8CF2E5] hover:before:opacity-100 sm:w-auto"
        >
          Analyze Now -&gt;
        </button>
      </div>
    </div>
  );
}
