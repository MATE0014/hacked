"use client";
import { useEffect, useState } from "react";

export default function SettingsPage() {
  const [health, setHealth] = useState<"checking" | "healthy" | "error">("checking");
  const API = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

  useEffect(() => {
    fetch(`${API}/health`)
      .then((r) => r.json())
      .then((d) => setHealth(d?.status === "healthy" ? "healthy" : "error"))
      .catch(() => setHealth("error"));
  }, [API]);

  const providers = [
    {
      name: "Groq (Primary)",
      description: "9 keys - 4 chunk, 1 synthesis, 4 backup",
      model: "llama-3.3-70b-versatile",
      status: health === "healthy" ? "active" : "checking",
      color: "#10B981",
    },
    {
      name: "Google Gemini",
      description: "Fallback layer 1",
      model: "gemini-1.5-flash",
      status: "standby",
      color: "#F59E0B",
    },
    {
      name: "HuggingFace",
      description: "Fallback layer 2",
      model: "Mistral-7B-Instruct",
      status: "standby",
      color: "#F59E0B",
    },
  ];

  const features = [
    { name: "Chunked large file processing", status: "active" },
    { name: "LangChain Pandas agent (NLQ)", status: "active" },
    { name: "Holt-Winters forecasting", status: "active" },
    { name: "Data quality scoring", status: "active" },
    { name: "NLP text analysis", status: "active" },
    { name: "Anomaly detection + explanation", status: "active" },
    { name: "PDF report export", status: "active" },
    { name: "MongoDB large file storage", status: "active" },
  ];

  const statusDot = (s: string) =>
    (
      {
        active: { color: "#10B981", label: "Active" },
        standby: { color: "#F59E0B", label: "Standby" },
        checking: { color: "#888", label: "Checking..." },
        error: { color: "#EF4444", label: "Error" },
      } as const
    )[s as "active" | "standby" | "checking" | "error"] ?? { color: "#888", label: s };

  return (
    <div
      style={{
        maxWidth: 1000,
        margin: "0 auto",
        color: "#fff",
        fontFamily: "var(--font-dm-sans), sans-serif",
      }}
    >
      <div style={{ marginBottom: 24 }}>
        <h2
          style={{
            fontSize: 28,
            fontWeight: 800,
            marginBottom: 6,
            fontFamily: "var(--font-syne), sans-serif",
          }}
        >
          System Status
        </h2>
        <p style={{ color: "#888", fontSize: 14 }}>
          Live status of all InsightFlow components and API providers.
        </p>
      </div>

      <div
        style={{
          background: "#141414",
          border: "1px solid rgba(255,255,255,0.08)",
          borderRadius: 12,
          padding: 16,
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 12,
          marginBottom: 20,
        }}
      >
        <div>
          <h3
            style={{
              margin: 0,
              color: "#fff",
              fontSize: 16,
              fontFamily: "var(--font-syne), sans-serif",
            }}
          >
            Backend API
          </h3>
          <p style={{ margin: "6px 0 0", color: "#888", fontSize: 14 }}>
            {health === "healthy"
              ? "FastAPI running on localhost:8000"
              : health === "error"
                ? "Cannot reach backend - is it running?"
                : "Checking connection..."}
          </p>
        </div>

        <span
          style={{
            background: health === "healthy" ? "#10B98122" : health === "error" ? "#EF444422" : "#88888822",
            color: health === "healthy" ? "#10B981" : health === "error" ? "#EF4444" : "#888",
            border: `1px solid ${health === "healthy" ? "#10B98155" : health === "error" ? "#EF444455" : "#88888855"}`,
            borderRadius: 999,
            padding: "4px 10px",
            fontSize: 12,
            fontWeight: 600,
            whiteSpace: "nowrap",
          }}
        >
          {health === "healthy" ? "Healthy" : health === "error" ? "Offline" : "Checking"}
        </span>
      </div>

      <h3
        style={{
          fontSize: 18,
          color: "#fff",
          marginBottom: 10,
          fontFamily: "var(--font-syne), sans-serif",
        }}
      >
        AI Providers
      </h3>

      <div style={{ display: "grid", gap: 10, marginBottom: 24 }}>
        {providers.map((p) => {
          const dot = statusDot(p.status);
          return (
            <div
              key={p.name}
              style={{
                background: "#141414",
                border: "1px solid rgba(255,255,255,0.08)",
                borderRadius: 12,
                padding: 14,
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                gap: 12,
              }}
            >
              <div>
                <p
                  style={{
                    margin: 0,
                    color: "#fff",
                    fontSize: 15,
                    fontWeight: 700,
                    fontFamily: "var(--font-syne), sans-serif",
                  }}
                >
                  {p.name}
                </p>
                <p style={{ margin: "4px 0 0", color: "#888", fontSize: 13 }}>
                  {p.description} · {p.model}
                </p>
              </div>

              <span
                style={{
                  background: `${dot.color}22`,
                  color: dot.color,
                  border: `1px solid ${dot.color}55`,
                  borderRadius: 999,
                  padding: "4px 10px",
                  fontSize: 12,
                  fontWeight: 600,
                  whiteSpace: "nowrap",
                }}
              >
                {dot.label}
              </span>
            </div>
          );
        })}
      </div>

      <h3
        style={{
          fontSize: 18,
          color: "#fff",
          marginBottom: 10,
          fontFamily: "var(--font-syne), sans-serif",
        }}
      >
        Features
      </h3>

      <div
        style={{
          background: "#141414",
          border: "1px solid rgba(255,255,255,0.08)",
          borderRadius: 12,
          overflow: "hidden",
        }}
      >
        {features.map((f, i) => (
          <div
            key={f.name}
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              padding: "12px 14px",
              borderTop: i > 0 ? "1px solid rgba(255,255,255,0.05)" : "none",
            }}
          >
            <span style={{ color: "#fff", fontSize: 14 }}>{f.name}</span>
            <span
              style={{
                color: "#10B981",
                fontSize: 12,
                fontWeight: 600,
                border: "1px solid #10B98155",
                borderRadius: 999,
                padding: "3px 9px",
                background: "#10B98122",
              }}
            >
              Active
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
