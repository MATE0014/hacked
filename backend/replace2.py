import re

with open('backend/llm_engine.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the init method to use KeyManager
init_replacement = """    def __init__(self):
        \"\"\"Initialize Groq LLM KeyManager\"\"\"
        self.key_manager = KeyManager()
        self.model_name = self.key_manager.model_name
        self.mock_mode = not bool(self.key_manager.clients)
        
        if self.mock_mode:
            print(\"\"\"
⚠️ Groq LLM KeyManager not initialized! No API keys found. Running in MOCK MODE.
\"\"\")
        else:
            print("✓ Groq LLM KeyManager initialized (FREE - No credit card needed!)")
            print(f"  Model: {self.model_name} | Free tier: ~30k req/month")
            print(f"  Found {len(self.key_manager.clients)} API keys for rotation")
"""

content = re.sub(r'    def __init__\(self\):[\s\S]*?(?=    def _enforce_chat_style)', init_replacement, content)

# Replace the _chat method to use KeyManager
chat_replacement = """    def _chat(self, system_prompt: str, user_prompt: str, history: Optional[List[Dict[str, str]]] = None) -> str:
        \"\"\"Send a chat completion request to Groq via KeyManager and return plain text.\"\"\"
        if self.mock_mode or not self.key_manager.clients:
            raise RuntimeError("Groq client is not initialized")

        return self.key_manager.call_with_retry(system_prompt, user_prompt, history=history)
"""

content = re.sub(r'    def _chat\(self, system_prompt: str, user_prompt: str\) -> str:[\s\S]*?(?=    def _build_dataset_context)', chat_replacement, content)

# Replace answer_question to support history
answer_question_repl = """    def answer_question(self, question: str, dataframe: pd.DataFrame, history: Optional[List[Dict[str, str]]] = None) -> str:
        \"\"\"
        Answer questions about the dataset using free APIs
        
        Args:
            question: User's question about the data
            dataframe: The DataFrame to analyze
            history: Optional list of previous chat messages
            
        Returns:
            AI-generated answer
        \"\"\"
        
        # Create context for the LLM
        dataset_summary = self._create_dataset_summary(dataframe)
        
        if self.mock_mode:
            return self._enforce_chat_style(self._generate_mock_answer(question, dataset_summary))
        
        try:
            user_prompt = (
                "You are a data analyst helping users understand their data. "
                "Answer the following question based on the dataset information provided. "
                "Be concise, clear, and provide actionable insights. "
                "Start every answer with exactly: 'I am InsightFlow AI'. "
                "Do not ask reconfirmation questions, clarification questions, or follow-up questions. "
                "Provide a direct answer only.\\n\\n"
                f"Dataset Summary:\\n{dataset_summary}\\n\\n"
                f"User Question: {question}\\n\\n"
                "Answer:"
            )
            raw_answer = self._chat(
                system_prompt="You are a helpful data analyst.",
                user_prompt=user_prompt,
                history=history[-6:] if history else None
            )
            return self._enforce_chat_style(raw_answer)
        except Exception as e:
            print(f"Error answering question: {e}")
            return self._enforce_chat_style(self._generate_mock_answer(question, dataset_summary))"""

content = re.sub(r'    def answer_question\(self, question: str, dataframe: pd\.DataFrame\) -> str:[\s\S]*?(?=    def _chat)', answer_question_repl + "\n\n", content)

with open('backend/llm_engine.py', 'w', encoding='utf-8') as f:
    f.write(content)
