"""
LLM integration using Groq SDK
"""
import itertools
import json
import os
import re
import time
from difflib import get_close_matches
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv
from langchain_classic.agents.agent_types import AgentType
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain_groq import ChatGroq

try:
    from groq import Groq
except Exception:
    Groq = None

load_dotenv()


class KeyManager:
    """Manage multiple Groq keys with round-robin rotation and retry-on-429."""

    def __init__(self, model_name: str = "llama-3.1-8b-instant"):
        self.model_name = model_name
        self.indexed_keys = self._load_indexed_keys()
        self.fallback_key = (os.getenv("GROQ_API_KEY") or "").strip() or None

        self.clients_by_index = self._build_clients_by_index(self.indexed_keys)
        self.fallback_client = self._build_single_client(self.fallback_key)

        self.chunk_primary_clients = [
            self.clients_by_index[idx]
            for idx in [1, 2, 3, 4]
            if idx in self.clients_by_index
        ]
        self.summary_primary_clients = [
            self.clients_by_index[idx]
            for idx in [5]
            if idx in self.clients_by_index
        ]
        self.general_primary_clients = [
            self.clients_by_index[idx]
            for idx in [1, 2, 3, 4, 5]
            if idx in self.clients_by_index
        ]
        self.backup_clients = [
            self.clients_by_index[idx]
            for idx in [6, 7, 8, 9]
            if idx in self.clients_by_index
        ]

        all_clients = []
        all_clients.extend(self.general_primary_clients)
        all_clients.extend(self.backup_clients)
        if self.fallback_client is not None:
            all_clients.append(self.fallback_client)
        self.clients = all_clients

    def _load_indexed_keys(self) -> Dict[int, str]:
        keys: Dict[int, str] = {}
        for idx in range(1, 10):
            key = (os.getenv(f"GROQ_API_KEY_{idx}") or "").strip()
            if key:
                keys[idx] = key
        return keys

    def _build_clients_by_index(self, keys: Dict[int, str]) -> Dict[int, Any]:
        if not keys or Groq is None:
            return {}

        clients: Dict[int, Any] = {}
        for idx, key in keys.items():
            try:
                clients[idx] = Groq(api_key=key)
            except Exception as exc:
                print(f"Skipping invalid Groq key {idx}: {exc}")
        return clients

    def _build_single_client(self, key: Optional[str]) -> Optional[Any]:
        if not key or Groq is None:
            return None
        try:
            return Groq(api_key=key)
        except Exception as exc:
            print(f"Skipping fallback GROQ_API_KEY: {exc}")
            return None

    def _is_rate_limit_error(self, exc: Exception) -> bool:
        status_code = getattr(exc, "status_code", None)
        if status_code == 429:
            return True

        text = str(exc).lower()
        return "429" in text or "rate limit" in text or "too many requests" in text

    def call_with_retry(
        self,
        system_prompt: str,
        user_prompt: str,
        history: Optional[List[Dict[str, str]]] = None,
        max_retries: Optional[int] = None,
        purpose: str = "general",
    ) -> str:
        """Call Groq completion using role-based key pools and backup failover."""
        if not self.clients:
            raise RuntimeError("No Groq clients available")

        primary, backups = self._resolve_client_pools(purpose)
        ordered_clients = primary + backups
        if not ordered_clients:
            raise RuntimeError("No Groq clients available for selected purpose")

        history_messages = []
        if history:
            for msg in history[-6:]:
                role = msg.get("role", "user")
                content = msg.get("content", "")
                if role in {"system", "user", "assistant"} and content:
                    history_messages.append({"role": role, "content": content})

        attempts = max_retries if max_retries is not None else len(ordered_clients)
        attempts = max(1, attempts)
        client_cycle = itertools.cycle(ordered_clients)

        for _ in range(attempts):
            client = next(client_cycle)
            try:
                messages = [{"role": "system", "content": system_prompt}]
                messages.extend(history_messages)
                messages.append({"role": "user", "content": user_prompt})

                completion = client.chat.completions.create(
                    model=self.model_name,
                    messages=messages,
                    temperature=0.2,
                    max_tokens=700,
                )
                return completion.choices[0].message.content.strip()
            except Exception as exc:
                if self._is_rate_limit_error(exc):
                    time.sleep(2)
                    continue
                raise

        raise RuntimeError(f"All Groq keys exhausted for purpose='{purpose}' due to rate limits")

    def _resolve_client_pools(self, purpose: str) -> Tuple[List[Any], List[Any]]:
        """Return primary pool and backup pool by workload purpose."""
        if purpose == "chunk":
            primary = self.chunk_primary_clients
        elif purpose == "summary":
            primary = self.summary_primary_clients
        elif purpose == "backup":
            primary = self.backup_clients
        else:
            primary = self.general_primary_clients

        if not primary and self.fallback_client is not None:
            primary = [self.fallback_client]

        backups = list(self.backup_clients)
        if self.fallback_client is not None and self.fallback_client not in primary:
            backups.append(self.fallback_client)

        return list(primary), backups


class LLMEngine:
    def __init__(self):
        """Initialize Groq LLM engine with rotating API keys."""
        self.key_manager = KeyManager()
        self.model_name = self.key_manager.model_name
        self.mock_mode = not bool(self.key_manager.clients)

        if self.mock_mode:
            print(
                """
============================================================
INFO: GROQ API KEY NOT FOUND
------------------------------------------------------------
============================================================
"""
            )
        else:
            print("Groq LLM engine initialized")
            print(
                f"Model: {self.model_name} | "
                f"Chunk keys: {len(self.key_manager.chunk_primary_clients)} | "
                f"Summary keys: {len(self.key_manager.summary_primary_clients)} | "
                f"Backups: {len(self.key_manager.backup_clients)}"
            )

        # LangChain LLM - only used for NLQ agent and insights prompt template.
        # Uses primary Groq key while KeyManager continues parallel/backup flows elsewhere.
        if not self.mock_mode:
            primary_key = os.getenv("GROQ_API_KEY_1") or os.getenv("GROQ_API_KEY")
            self.langchain_llm = ChatGroq(
                api_key=primary_key,
                model=self.model_name,
                temperature=0.2,
                max_tokens=900,
            )
        else:
            self.langchain_llm = None

        # Lightweight rolling chat history (last 6 user-assistant exchanges).
        self.chat_history: List[Dict[str, str]] = []

        # External fallbacks if Groq is unavailable or exhausted.
        self.gemini_api_key = (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip()
        self.gemini_model = (os.getenv("GEMINI_MODEL") or "gemini-1.5-flash").strip()

        self.hf_api_key = (
            os.getenv("HUGGINGFACE_API_KEY")
            or os.getenv("HF_API_KEY")
            or os.getenv("HUGGINGFACEHUB_API_TOKEN")
            or ""
        ).strip()
        self.hf_model = (os.getenv("HF_MODEL") or "mistralai/Mistral-7B-Instruct-v0.2").strip()

    def _normalize_chat_answer(self, answer: str) -> str:
        """Keep chat responses concise and remove repeated identity intros."""
        cleaned = (answer or "").strip()
        if not cleaned:
            return "I can analyze your data once a valid question is provided."

        # Some model responses repeat this intro multiple times; strip any leading repeats.
        cleaned = re.sub(
            r"^(?:\s*I\s+am\s+InsightFlow\s+AI\s*\.\s*)+",
            "",
            cleaned,
            flags=re.IGNORECASE,
        ).strip()

        return cleaned or "I can analyze your data once a valid question is provided."

    def _normalize_column_token(self, value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", (value or "").lower())

    def _resolve_column_name(self, raw_name: str, columns: List[str]) -> Optional[str]:
        if not raw_name:
            return None

        by_lower = {col.lower(): col for col in columns}
        by_norm = {self._normalize_column_token(col): col for col in columns}

        lower = raw_name.strip().lower()
        norm = self._normalize_column_token(raw_name)

        if lower in by_lower:
            return by_lower[lower]
        if norm in by_norm:
            return by_norm[norm]

        close_lower = get_close_matches(lower, list(by_lower.keys()), n=1, cutoff=0.84)
        if close_lower:
            return by_lower[close_lower[0]]

        close_norm = get_close_matches(norm, list(by_norm.keys()), n=1, cutoff=0.84)
        if close_norm:
            return by_norm[close_norm[0]]

        return None

    def _extract_column_from_question(self, question: str, dataframe: pd.DataFrame) -> Optional[str]:
        columns = dataframe.columns.tolist()

        # Highest-confidence path: explicit quoted column names.
        quoted_matches = re.findall(r"['\"]([^'\"]+)['\"]", question)
        for candidate in quoted_matches:
            resolved = self._resolve_column_name(candidate, columns)
            if resolved:
                return resolved

        lower_question = question.lower()

        # Match full column names directly if user did not use quotes.
        for col in sorted(columns, key=len, reverse=True):
            if col.lower() in lower_question:
                return col

        # Heuristic: try phrase after "of" or "for".
        phrase_match = re.search(
            r"(?:of|for)\s+([a-zA-Z0-9_\-\s]{1,80})(?:\?|\.|,|$)",
            question,
            flags=re.IGNORECASE,
        )
        if phrase_match:
            candidate = phrase_match.group(1).strip()
            resolved = self._resolve_column_name(candidate, columns)
            if resolved:
                return resolved

        return None

    def _format_value(self, value: Any) -> str:
        if isinstance(value, (int, float)):
            return f"{value:,.2f}"
        return str(value)

    def _get_numeric_like_columns(self, dataframe: pd.DataFrame) -> List[str]:
        numeric_like: List[str] = []
        for col in dataframe.columns:
            series = pd.to_numeric(dataframe[col], errors="coerce")
            if int(series.notna().sum()) > 0:
                numeric_like.append(col)
        return numeric_like

    def _answer_with_structured_nlp(self, question: str, dataframe: pd.DataFrame) -> Optional[str]:
        question_lower = question.lower().strip()
        numeric_columns = self._get_numeric_like_columns(dataframe)

        agg_map = [
            ("mean", ["mean", "average", "avg"]),
            ("median", ["median"]),
            ("sum", ["sum", "total"]),
            ("min", ["minimum", "min", "lowest", "smallest"]),
            ("max", ["maximum", "max", "highest", "largest"]),
            ("std", ["std", "standard deviation", "deviation"]),
        ]

        for agg_name, keywords in agg_map:
            if any(keyword in question_lower for keyword in keywords):
                col = self._extract_column_from_question(question, dataframe)

                if not col:
                    if len(numeric_columns) == 1:
                        col = numeric_columns[0]
                    elif len(numeric_columns) > 1:
                        sample_cols = ", ".join(numeric_columns[:8])
                        return (
                            "Please specify a numeric column name. "
                            f"Available numeric columns include: {sample_cols}."
                        )
                    else:
                        return "This dataset has no numeric columns available for that calculation."

                if col not in dataframe.columns:
                    return None

                series = pd.to_numeric(dataframe[col], errors="coerce").dropna()
                if series.empty:
                    return f"Column '{col}' has no valid numeric values for {agg_name}."

                if agg_name == "mean":
                    value = float(series.mean())
                elif agg_name == "median":
                    value = float(series.median())
                elif agg_name == "sum":
                    value = float(series.sum())
                elif agg_name == "min":
                    value = float(series.min())
                elif agg_name == "max":
                    value = float(series.max())
                else:
                    value = float(series.std())

                return (
                    f"The {agg_name} value of '{col}' is {self._format_value(value)} "
                    f"(computed from {len(series):,} numeric rows)."
                )

        if any(token in question_lower for token in ["how many rows", "row count", "records", "entries"]):
            return f"This dataset has {len(dataframe):,} rows."

        if any(token in question_lower for token in ["how many columns", "column count", "number of columns"]):
            return f"This dataset has {len(dataframe.columns):,} columns."

        if any(token in question_lower for token in ["missing", "null", "nan", "empty"]):
            col = self._extract_column_from_question(question, dataframe)
            if col:
                missing_count = int(dataframe[col].isna().sum())
                return f"Column '{col}' has {missing_count:,} missing values."
            total_missing = int(dataframe.isna().sum().sum())
            return f"The dataset has {total_missing:,} missing values in total."

        if any(token in question_lower for token in ["unique", "distinct"]):
            col = self._extract_column_from_question(question, dataframe)
            if col:
                unique_count = int(dataframe[col].nunique(dropna=True))
                return f"Column '{col}' has {unique_count:,} unique non-null values."

        return None

    def _set_chat_history_from_frontend(self, history: List[Dict[str, str]]) -> None:
        """Rebuild local memory from frontend-provided history window."""
        self.chat_history = []
        for msg in history[-12:]:
            role = msg.get("role", "")
            content = (msg.get("content", "") or "").strip()
            if role in {"user", "assistant"} and content:
                self.chat_history.append({"role": role, "content": content})

    def _append_chat_message(self, role: str, content: str) -> None:
        """Append one chat message and keep a bounded in-memory history."""
        clean_content = (content or "").strip()
        if role not in {"user", "assistant"} or not clean_content:
            return
        self.chat_history.append({"role": role, "content": clean_content})
        self.chat_history = self.chat_history[-12:]

    def _format_chat_history(self) -> str:
        if not self.chat_history:
            return "No previous conversation."

        return "\n".join(
            [
                f"{'User' if msg['role'] == 'user' else 'Assistant'}: {msg['content']}"
                for msg in self.chat_history
            ]
        )

    def generate_insights(
        self,
        dataframe: pd.DataFrame,
        statistics: Dict[str, Any],
        structure: Dict[str, Any],
    ) -> str:
        context = self._build_dataset_context(dataframe, statistics, structure)
        fallback_prompt = (
            "Analyze this dataset and provide 3-4 key insights for a non-technical audience. "
            "Focus on what is interesting, unusual, or actionable.\n\n"
            f"Dataset context:\n{context}"
        )

        if self.mock_mode or self.langchain_llm is None:
            external = self._external_llm_fallback(
                system_prompt=(
                    "You are an expert data analyst. "
                    "The data has already been cleaned - no need to mention cleaning steps. "
                    "Be concise and professional."
                ),
                user_prompt=fallback_prompt,
            )
            if external:
                return external
            return self._generate_mock_insights(context)

        try:
            prompt = ChatPromptTemplate.from_messages(
                [
                    (
                        "system",
                        (
                            "You are an expert data analyst. "
                            "The data has already been cleaned - no need to mention cleaning steps. "
                            "Be concise and professional."
                        ),
                    ),
                    (
                        "human",
                        (
                            "Analyze this dataset and provide 3-4 key insights "
                            "for a non-technical audience. Focus on what is interesting, "
                            "unusual, or actionable.\n\n"
                            "Dataset context:\n{context}"
                        ),
                    ),
                ]
            )
            chain = prompt | self.langchain_llm | StrOutputParser()
            result = chain.invoke({"context": context})
            return result or self._generate_mock_insights(context)
        except Exception as e:
            print(f"LangChain insights error: {e}")
            external = self._external_llm_fallback(
                system_prompt=(
                    "You are an expert data analyst. "
                    "The data has already been cleaned - no need to mention cleaning steps. "
                    "Be concise and professional."
                ),
                user_prompt=fallback_prompt,
            )
            if external:
                return external
            return self._generate_mock_insights(context)

    def answer_question(
        self,
        question: str,
        dataframe: pd.DataFrame,
        history: list = None,
    ) -> str:
        # First pass: deterministic dataframe-backed NLP for factual/statistical questions.
        structured_answer = self._answer_with_structured_nlp(question, dataframe)
        if structured_answer:
            self._append_chat_message("user", question)
            self._append_chat_message("assistant", structured_answer)
            return self._normalize_chat_answer(structured_answer)

        if self.mock_mode or self.langchain_llm is None:
            summary = self._create_dataset_summary(dataframe)
            external = self._external_llm_fallback(
                system_prompt=(
                    "You are InsightFlow AI, a data analyst. "
                    "Start every answer with 'I am InsightFlow AI'. "
                    "Answer directly. Do not ask follow-up questions. "
                    "The data has already been cleaned."
                ),
                user_prompt=(
                    f"Dataset info:\n{summary}\n\n"
                    f"Question: {question}"
                ),
            )
            if external:
                return self._normalize_chat_answer(external)
            return self._normalize_chat_answer(self._generate_mock_answer(question, summary))

        # Sync frontend history into local rolling memory window.
        if history:
            self._set_chat_history_from_frontend(history)

        # Primary path: Pandas DataFrame Agent for exact dataframe-backed answers.
        try:
            agent = create_pandas_dataframe_agent(
                self.langchain_llm,
                dataframe,
                agent_type=AgentType.OPENAI_FUNCTIONS,
                verbose=False,
                allow_dangerous_code=True,
                handle_parsing_errors=True,
                prefix=(
                    "You are InsightFlow AI, a data analyst. "
                    "Start every answer with 'I am InsightFlow AI'. "
                    "Answer directly without asking follow-up questions. "
                    "The data is already cleaned."
                ),
            )
            result = agent.run(question)
            self._append_chat_message("user", question)
            self._append_chat_message("assistant", result)
            return self._normalize_chat_answer(result)
        except Exception as e:
            print(f"Pandas agent failed: {e}. Falling back to prompt chain.")

        # Fallback path: prompt chain with memory context.
        try:
            history_text = self._format_chat_history()

            summary = self._create_dataset_summary(dataframe)

            prompt = ChatPromptTemplate.from_messages(
                [
                    (
                        "system",
                        (
                            "You are InsightFlow AI, a data analyst. "
                            "Start every answer with 'I am InsightFlow AI'. "
                            "Answer directly. Do not ask follow-up questions. "
                            "The data has already been cleaned."
                        ),
                    ),
                    (
                        "human",
                        (
                            "Dataset info:\n{summary}\n\n"
                            "Conversation so far:\n{history}\n\n"
                            "Question: {question}"
                        ),
                    ),
                ]
            )
            chain = prompt | self.langchain_llm | StrOutputParser()
            result = chain.invoke(
                {
                    "summary": summary,
                    "history": history_text,
                    "question": question,
                }
            )
            self._append_chat_message("user", question)
            self._append_chat_message("assistant", result)
            return self._normalize_chat_answer(result)
        except Exception as e:
            print(f"LangChain fallback error: {e}")
            summary = self._create_dataset_summary(dataframe)
            external = self._external_llm_fallback(
                system_prompt=(
                    "You are InsightFlow AI, a data analyst. "
                    "Start every answer with 'I am InsightFlow AI'. "
                    "Answer directly. Do not ask follow-up questions. "
                    "The data has already been cleaned."
                ),
                user_prompt=(
                    f"Dataset info:\n{summary}\n\n"
                    f"Question: {question}"
                ),
            )
            if external:
                return self._normalize_chat_answer(external)
            return self._normalize_chat_answer(self._generate_mock_answer(question, summary))

    def _external_llm_fallback(self, system_prompt: str, user_prompt: str) -> Optional[str]:
        """Try non-Groq providers in priority order: Gemini, then Hugging Face."""
        gemini = self._gemini_generate(system_prompt, user_prompt)
        if gemini:
            return gemini

        hf = self._hf_generate(system_prompt, user_prompt)
        if hf:
            return hf

        return None

    def _gemini_generate(self, system_prompt: str, user_prompt: str) -> Optional[str]:
        if not self.gemini_api_key:
            return None

        try:
            url = (
                "https://generativelanguage.googleapis.com/v1beta/models/"
                f"{urllib_parse.quote(self.gemini_model)}:generateContent"
                f"?key={urllib_parse.quote(self.gemini_api_key)}"
            )
            payload = {
                "contents": [{"parts": [{"text": f"{system_prompt}\n\n{user_prompt}"}]}],
                "generationConfig": {
                    "temperature": 0.4,
                    "maxOutputTokens": 900,
                },
            }
            data = self._http_post_json(url, payload, headers={"Content-Type": "application/json"})
            if not isinstance(data, dict):
                return None

            candidates = data.get("candidates") or []
            if not candidates:
                return None

            parts = (((candidates[0] or {}).get("content") or {}).get("parts") or [])
            text = "\n".join(part.get("text", "") for part in parts if isinstance(part, dict)).strip()
            return text or None
        except Exception as exc:
            print(f"Gemini fallback failed: {exc}")
            return None

    def _hf_generate(self, system_prompt: str, user_prompt: str) -> Optional[str]:
        if not self.hf_api_key:
            return None

        try:
            url = f"https://api-inference.huggingface.co/models/{urllib_parse.quote(self.hf_model, safe='/')}"
            prompt = f"{system_prompt}\n\n{user_prompt}"
            payload = {
                "inputs": prompt,
                "parameters": {
                    "temperature": 0.4,
                    "max_new_tokens": 700,
                    "return_full_text": False,
                },
            }
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.hf_api_key}",
            }
            data = self._http_post_json(url, payload, headers=headers)

            if isinstance(data, list) and data and isinstance(data[0], dict):
                text = (data[0].get("generated_text") or "").strip()
                return text or None

            if isinstance(data, dict):
                generated = (data.get("generated_text") or "").strip()
                if generated:
                    return generated
                error_msg = data.get("error")
                if error_msg:
                    print(f"HF fallback error: {error_msg}")

            return None
        except Exception as exc:
            print(f"HF fallback failed: {exc}")
            return None

    def _http_post_json(self, url: str, payload: Dict[str, Any], headers: Dict[str, str]) -> Any:
        req = urllib_request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            with urllib_request.urlopen(req, timeout=30) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        except urllib_error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
            raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc
        except urllib_error.URLError as exc:
            raise RuntimeError(f"Network error: {exc}") from exc

        if not raw.strip():
            return None

        try:
            return json.loads(raw)
        except Exception:
            return raw

    def _chat(
        self,
        system_prompt: str,
        user_prompt: str,
        history: Optional[List[Dict[str, str]]] = None,
    ) -> str:
        """Send a chat completion request to Groq via KeyManager and return plain text."""
        if self.mock_mode or not self.key_manager.clients:
            raise RuntimeError("Groq client is not initialized")

        return self.key_manager.call_with_retry(system_prompt, user_prompt, history=history)

    def _build_dataset_context(
        self,
        dataframe: pd.DataFrame,
        statistics: Dict[str, Any],
        structure: Dict[str, Any],
    ) -> str:
        """Build comprehensive context about the dataset for LLM."""
        context = f"""
Dataset Overview:
- Total Rows: {len(dataframe)}
- Total Columns: {len(dataframe.columns)}
- Column Names: {', '.join(dataframe.columns.tolist())}

Data Types:
{json.dumps(structure['column_types'], indent=2)}

Numeric Columns Analysis:
"""

        if statistics.get("numeric_analysis"):
            for col, stats_info in statistics["numeric_analysis"].items():
                context += (
                    f"\n- {col}: Mean={stats_info['mean']:.2f}, "
                    f"Median={stats_info['median']:.2f}, Std={stats_info['std']:.2f}"
                )

        context += "\n\nCategorical Columns Analysis:\n"
        if statistics.get("categorical_analysis"):
            for col, stats_info in statistics["categorical_analysis"].items():
                top_vals = list(stats_info.get("top_values", {}).items())[:3]
                context += (
                    f"\n- {col}: {stats_info['unique_values']} unique values. Top: "
                    f"{', '.join([f'{k}({v})' for k, v in top_vals])}"
                )

        context += "\n\nKey Correlations:\n"
        if statistics.get("correlations", {}).get("strong_correlations"):
            for pair, corr_val in list(statistics["correlations"]["strong_correlations"].items())[:3]:
                context += f"\n- {pair}: {corr_val:.3f}"

        context += "\n\nAnomalies Detected:\n"
        if statistics.get("anomalies"):
            for col, anomaly_info in statistics["anomalies"].items():
                context += (
                    f"\n- {col}: {anomaly_info['outlier_count']} outliers "
                    f"({anomaly_info['outlier_percentage']:.1f}%)"
                )
        else:
            context += "\n- No significant anomalies detected"

        return context

    def _create_dataset_summary(self, dataframe: pd.DataFrame) -> str:
        """Create a summary of dataset for question answering."""
        numeric_columns = dataframe.select_dtypes(include=["number"]).columns.tolist()
        categorical_columns = dataframe.select_dtypes(exclude=["number"]).columns.tolist()

        numeric_summary_lines = []
        for col in numeric_columns[:12]:
            series = pd.to_numeric(dataframe[col], errors="coerce").dropna()
            if series.empty:
                continue
            numeric_summary_lines.append(
                f"- {col}: mean={series.mean():.2f}, median={series.median():.2f}, min={series.min():.2f}, max={series.max():.2f}"
            )

        categorical_summary_lines = []
        for col in categorical_columns[:8]:
            series = dataframe[col].dropna().astype(str)
            if series.empty:
                continue
            top_counts = series.value_counts().head(3)
            top_values = ", ".join([f"{idx}({val})" for idx, val in top_counts.items()])
            categorical_summary_lines.append(
                f"- {col}: unique={series.nunique()}, top={top_values}"
            )

        numeric_block = "\n".join(numeric_summary_lines) if numeric_summary_lines else "- None"
        categorical_block = "\n".join(categorical_summary_lines) if categorical_summary_lines else "- None"

        summary = f"""
Dataset Summary:
- Dimensions: {len(dataframe)} rows x {len(dataframe.columns)} columns
- Columns: {', '.join(dataframe.columns.tolist())}
- Data Types: {dataframe.dtypes.to_dict()}
- Memory Usage: {dataframe.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB

Numeric Stats (sample):
{numeric_block}

Categorical Stats (sample):
{categorical_block}

Data Preview (first 5 rows):
{dataframe.head().to_string()}
"""
        return summary

    def _generate_mock_insights(self, context: str) -> str:
        """Generate mock insights when LLM is unavailable."""
        return """
Dataset Insights:

1. Data Quality: Your dataset has been successfully cleaned and processed. Missing values were handled intelligently, and duplicate rows were removed.

2. Data Composition: The dataset contains a good mix of numeric, categorical, and possibly temporal data. This allows for rich analysis possibilities.

3. Key Patterns: We detected correlations between numeric variables and identified the most frequent categories in categorical columns.

4. Anomalies: The analysis flagged potential outliers in numeric columns. These may represent unusual cases worth investigating.

Recommendation: Use the charts and statistics above to explore your data and ask targeted questions in chat.
"""

    def _generate_mock_answer(self, question: str, dataset_summary: str) -> str:
        """Generate mock answer when LLM is unavailable."""
        question_lower = question.lower()

        if any(word in question_lower for word in ["distribution", "spread", "range"]):
            return (
                "Based on your data, distribution patterns were analyzed. Check histogram views "
                "for skewness or multiple peaks across numeric columns."
            )

        if any(word in question_lower for word in ["correlation", "relationship", "related"]):
            return (
                "Correlation analysis shows how numeric variables move together. Strong values "
                "indicate predictable relationships between columns."
            )

        if any(word in question_lower for word in ["outlier", "anomaly", "unusual"]):
            return (
                "Outliers were detected using the IQR method. These points fall outside the "
                "typical range and may be errors, rare events, or useful signals."
            )

        if any(word in question_lower for word in ["missing", "null", "empty"]):
            return (
                "Missing values were handled during preprocessing. Numeric columns used median "
                "imputation and categorical columns used mode imputation."
            )

        if any(word in question_lower for word in ["count", "how many", "total"]):
            return (
                "Dataset size and metadata are available in the summary. You can also filter "
                "and group columns to compute targeted counts."
            )

        return (
            "Based on the current dataset summary, I can provide direct analysis of "
            "distributions, relationships, anomalies, and key column patterns."
        )
