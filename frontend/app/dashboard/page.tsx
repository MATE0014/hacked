"use client";

import { useRouter } from "next/navigation";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import {
  faChartLine,
  faComments,
  faDatabase,
  faDollarSign,
  faFileLines,
  faShieldHalved,
  faTriangleExclamation,
} from "@fortawesome/free-solid-svg-icons";

const cardClass = "rounded-2xl border border-white/6 bg-[#1A1B20] p-5";

export default function DashboardPage() {
  const router = useRouter();
  const onAnalyze = () => router.push("/analyzer");
  const comparisonRows = [
    {
      left: "Handles 100GB+ files",
      right: "Crashes on large files",
      leftIcon: faDatabase,
      rightIcon: faTriangleExclamation,
    },
    {
      left: "Auto-detects data quality issues",
      right: "No data validation",
      leftIcon: faShieldHalved,
      rightIcon: faTriangleExclamation,
    },
    {
      left: "NLP on text columns",
      right: "Text = just a category label",
      leftIcon: faFileLines,
      rightIcon: faFileLines,
    },
    {
      left: "Forecasts with recommendations",
      right: "Manual chart config required",
      leftIcon: faChartLine,
      rightIcon: faChartLine,
    },
    {
      left: "Free, no setup",
      right: "$30/user/month + setup time",
      leftIcon: faDollarSign,
      rightIcon: faDollarSign,
    },
    {
      left: "Plain English answers",
      right: "Requires DAX / SQL knowledge",
      leftIcon: faComments,
      rightIcon: faComments,
    },
  ];

  return (
    <div className="space-y-8 text-white">
      <section className="mx-auto max-w-4xl rounded-3xl border border-white/6 bg-[#1A1B20] px-6 py-12 text-center sm:px-10">
        <h1 className="font-heading text-balance text-3xl font-semibold leading-tight sm:text-5xl">
          Turn your data into decisions - instantly.
        </h1>
        <p className="mx-auto mt-4 max-w-3xl text-sm leading-relaxed text-[#999] sm:text-base">
          Upload any CSV or Excel file. InsightFlow cleans it, analyzes it, forecasts trends,
          and tells you exactly what to do next. No SQL. No formulas. No data team required.
        </p>
        <button
          onClick={onAnalyze}
          className="relative isolate mt-7 w-full overflow-hidden rounded-full border border-brand-teal/60 bg-transparent px-6 py-3 font-heading text-base font-semibold tracking-wide text-brand-teal transition before:pointer-events-none before:absolute before:inset-0 before:bg-linear-to-r before:from-white/0 before:via-white/20 before:to-white/0 before:opacity-0 before:transition-opacity hover:border-brand-teal hover:bg-brand-teal/10 hover:text-[#8CF2E5] hover:before:opacity-100 sm:w-auto"
        >
          Analyze Now -&gt;
        </button>
      </section>

      <section className="space-y-4">
        <h2 className="font-heading text-xl font-semibold">Why InsightFlow</h2>
        <div className="grid gap-4 md:grid-cols-3">
          <article className={cardClass}>
            <div className="mb-4 h-10 w-10 rounded-lg bg-brand-teal/15 p-2 text-brand-teal">
              <svg viewBox="0 0 24 24" fill="none" className="h-full w-full" aria-hidden="true">
                <path d="M8 8a4 4 0 118 0v1a3 3 0 013 3v2h-2v3H7v-3H5v-2a3 3 0 013-3V8z" stroke="currentColor" strokeWidth="1.6" />
                <path d="M9 20h6" stroke="currentColor" strokeWidth="1.6" />
              </svg>
            </div>
            <h3 className="font-heading text-lg font-semibold">AI Analyst, not just charts</h3>
            <p className="mt-2 text-sm leading-relaxed text-[#999]">
              Ask questions in plain English. Get exact answers computed directly on your data
              - not AI guesses.
            </p>
          </article>

          <article className={cardClass}>
            <div className="mb-4 h-10 w-10 rounded-lg bg-[#14b8a6]/15 p-2 text-[#5eead4]">
              <svg viewBox="0 0 24 24" fill="none" className="h-full w-full" aria-hidden="true">
                <path d="M12 3l7 4v5c0 5-3 8-7 9-4-1-7-4-7-9V7l7-4z" stroke="currentColor" strokeWidth="1.6" />
                <path d="M9.5 12.5l1.8 1.8 3.2-3.2" stroke="currentColor" strokeWidth="1.6" />
              </svg>
            </div>
            <h3 className="font-heading text-lg font-semibold">Finds problems before you do</h3>
            <p className="mt-2 text-sm leading-relaxed text-[#999]">
              Detects label leakage, class imbalance, duplicate columns, and wrong data types
              automatically before you analyze.
            </p>
          </article>

          <article className={cardClass}>
            <div className="mb-4 h-10 w-10 rounded-lg bg-brand-teal/15 p-2 text-brand-teal">
              <svg viewBox="0 0 24 24" fill="none" className="h-full w-full" aria-hidden="true">
                <path d="M4 17l5-5 3 3 7-7" stroke="currentColor" strokeWidth="1.6" />
                <path d="M14 8h5v5" stroke="currentColor" strokeWidth="1.6" />
              </svg>
            </div>
            <h3 className="font-heading text-lg font-semibold">Predicts what happens next</h3>
            <p className="mt-2 text-sm leading-relaxed text-[#999]">
              Holt-Winters forecasting on time series columns with confidence bands and
              plain-English business recommendations.
            </p>
          </article>
        </div>
      </section>

      <section className={cardClass}>
        <h2 className="font-heading text-xl font-semibold">How it works</h2>
        <div className="mt-5 grid gap-4 md:grid-cols-3">
          {[
            { step: "1", label: "Upload", body: "Drop any CSV or Excel file" },
            { step: "2", label: "Analyze", body: "AI cleans, scores, forecasts, and explains" },
            { step: "3", label: "Decide", body: "Get recommendations, not just charts" },
          ].map((item, index) => (
            <div key={item.step} className="relative rounded-xl bg-white/2 p-4">
              <span className="mb-3 inline-flex h-7 w-7 items-center justify-center rounded-full bg-brand-teal/15 text-sm font-semibold text-brand-teal">
                {item.step}
              </span>
              <h3 className="font-heading text-lg font-medium">{item.label}</h3>
              <p className="mt-1 text-sm text-[#999]">{item.body}</p>
              {index < 2 && (
                <span className="pointer-events-none absolute -right-2 top-1/2 hidden h-px w-4 bg-brand-teal/40 md:block" />
              )}
            </div>
          ))}
        </div>
      </section>

      <section className={cardClass}>
        <h2 className="font-heading text-xl font-semibold">InsightFlow vs PowerBI</h2>
        <p className="mt-1 text-sm text-[#8D95A3]">
          Same goals, very different experience.
        </p>
        <div className="mt-4 overflow-hidden rounded-2xl border border-white/10 bg-[#0F1118] shadow-[0_18px_40px_rgba(0,0,0,0.35)]">
          <div className="grid grid-cols-2 bg-linear-to-r from-[#0D1319] to-[#121921] text-sm font-semibold">
            <div className="border-r border-white/10 px-4 py-3 text-brand-teal">InsightFlow</div>
            <div className="px-4 py-3 text-[#A2ACBB]">PowerBI</div>
          </div>
          {comparisonRows.map((row, index) => (
            <div
              key={row.left}
              className={`grid grid-cols-2 text-sm transition-colors hover:bg-white/3 ${
                index % 2 === 0 ? "bg-[#121620]" : "bg-[#0E121A]"
              }`}
            >
              <div className="border-r border-white/6 px-4 py-3 text-white">
                <span className="inline-flex items-start gap-2">
                  <FontAwesomeIcon
                    icon={row.leftIcon}
                    className="mt-0.5 h-3.5 w-3.5 text-brand-teal"
                  />
                  <span>{row.left}</span>
                </span>
              </div>
              <div className="px-4 py-3 text-[#97A0AE]">
                <span className="inline-flex items-start gap-2">
                  <FontAwesomeIcon
                    icon={row.rightIcon}
                    className="mt-0.5 h-3.5 w-3.5 text-[#758096]"
                  />
                  <span>{row.right}</span>
                </span>
              </div>
            </div>
          ))}
        </div>
      </section>

      <section className="pb-4 text-center">
        <button
          onClick={onAnalyze}
          className="relative isolate w-full overflow-hidden rounded-full border border-brand-teal/60 bg-transparent px-6 py-3 font-heading text-base font-semibold tracking-wide text-brand-teal transition before:pointer-events-none before:absolute before:inset-0 before:bg-linear-to-r before:from-white/0 before:via-white/20 before:to-white/0 before:opacity-0 before:transition-opacity hover:border-brand-teal hover:bg-brand-teal/10 hover:text-[#8CF2E5] hover:before:opacity-100 sm:w-auto"
        >
          Start Analyzing -&gt;
        </button>
        <p className="mt-3 text-xs text-[#888]">No account needed. Works with any CSV or Excel file.</p>
      </section>
    </div>
  );
}
