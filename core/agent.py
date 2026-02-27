# ============================================================
# DBMA - Database Management Agent
# core/agent.py — AI Agent Orchestration with Ollama LLM
# ============================================================
#
# ⚠️  LLM INTEGRATION POINT:
#     This file contains all Ollama / LangChain LLM integration.
#     The model used is configured via OLLAMA_MODEL in .env
#     Make sure Ollama is running: `ollama serve`
#     Pull your model first: `ollama pull llama3.1:8b`
#
# ════════════════════════════════════════════════════════════
# NEW FEATURES IN THIS VERSION:
#
# 1. SELF-HEALING QUERY RETRY LOOP
#    When a SQL query fails, the agent automatically:
#    - Reads the MySQL error message
#    - Sends error + original SQL back to LLM for analysis
#    - LLM corrects the SQL (wrong column, syntax, etc.)
#    - Retries up to MAX_HEAL_ATTEMPTS times
#    - Reports each attempt to the user in real-time
#    - Gives up cleanly if all retries fail
#
# 2. MULTI-AGENT SQL OPTIMIZER PIPELINE
#    Three specialized sub-agents work in sequence:
#    - Agent 1 (Writer):    Generates the initial SQL from natural language
#    - Agent 2 (Optimizer): Rewrites the SQL for performance (indexes, JOINs)
#    - Agent 3 (Validator): Checks for safety, correctness, and risk
#    Each agent has its own focused system prompt.
#    The pipeline runs transparently and returns the best SQL.
# ════════════════════════════════════════════════════════════

import re
import json
from typing import Optional, List, Dict, Any, Generator, Tuple
from dataclasses import dataclass, field
from enum import Enum
from loguru import logger

# ── LLM INTEGRATION: Ollama + LangChain + LangSmith ─────────
from langchain_community.llms import Ollama
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.output_parsers import StrOutputParser

# ── LANGSMITH TRACING ─────────────────────────────────────────
import os
import uuid
import datetime
from langsmith import Client as LangSmithClient
from langchain_core.tracers.langchain import LangChainTracer
# ── END LLM INTEGRATION ───────────────────────────────────────

from config import ollama_config, app_config
from core.persistence import PersistenceManager, ChatMessage
from core.mysql_manager import MySQLManager, QueryResult


# ════════════════════════════════════════════════════════════
# CONSTANTS
# ════════════════════════════════════════════════════════════

MAX_HEAL_ATTEMPTS = 3   # Self-healing: max auto-retry attempts before giving up
OPTIMIZER_ENABLED = False  # Disabled — was adding 2 extra LLM calls per query (3-4x slower)

# ── Rolling Summary (ChatGPT-style Memory) ────────────────────
# How it works — same technique as ChatGPT:
#   1. Keep last RECENT_MESSAGES_COUNT messages as full text
#   2. Summarize everything OLDER than that into a compact paragraph
#   3. LLM receives: [SUMMARY of old messages] + [last N full messages]
#   4. Every SUMMARY_UPDATE_EVERY new messages, re-summarize to stay current
#
# This gives the LLM access to the FULL conversation history
# (100, 500, 1000+ messages) without overloading its context window.
RECENT_MESSAGES_COUNT = 40    # Always send last 40 messages as full text
SUMMARY_UPDATE_EVERY  = 20    # Re-summarize every 20 new messages


# ════════════════════════════════════════════════════════════
# ENUMS & DATACLASSES
# ════════════════════════════════════════════════════════════

class AgentIntent(Enum):
    """Classification of what the user is asking for."""
    SHOW_DATABASES = "show_databases"
    SWITCH_DATABASE = "switch_database"
    SELECT_QUERY = "select_query"
    INSERT_DATA = "insert_data"
    UPDATE_DATA = "update_data"
    DELETE_DATA = "delete_data"
    CREATE_TABLE = "create_table"
    DROP_TABLE = "drop_table"
    ALTER_TABLE = "alter_table"
    DESCRIBE_TABLE = "describe_table"
    SHOW_TABLES = "show_tables"
    HELP = "help"
    EXPLAIN_SCHEMA = "explain_schema"
    GENERAL_QUESTION = "general_question"
    EXECUTE_QUERY = "execute_query"
    UNKNOWN = "unknown"


@dataclass
class HealAttempt:
    """Record of one self-healing retry attempt."""
    attempt_no: int
    original_sql: str
    error_message: str
    corrected_sql: str
    success: bool
    execution_ms: int = 0


@dataclass
class OptimizerReport:
    """
    Report produced by the Multi-Agent SQL Optimizer Pipeline.
    Contains output from all 3 sub-agents.
    """
    original_sql: str           # From Agent 1 (Writer)
    optimized_sql: str          # From Agent 2 (Optimizer)
    final_sql: str              # From Agent 3 (Validator) — the SQL to actually run
    optimizer_notes: str        # What Agent 2 changed and why
    validator_notes: str        # Agent 3 safety/correctness notes
    risk_level: str             # "LOW" | "MEDIUM" | "HIGH"
    was_modified: bool          # True if optimizer or validator changed the SQL


@dataclass
class AgentResponse:
    """Structured response from the DBMA Agent."""
    natural_text: str
    sql_query: Optional[str] = None
    intent: AgentIntent = AgentIntent.UNKNOWN
    confidence: float = 1.0
    auto_execute: bool = False
    requires_confirmation: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    # New fields for self-healing and optimizer
    heal_attempts: List[HealAttempt] = field(default_factory=list)
    optimizer_report: Optional[OptimizerReport] = None

    def has_sql(self) -> bool:
        return self.sql_query is not None and self.sql_query.strip() != ""


# ════════════════════════════════════════════════════════════
# SYSTEM PROMPTS
# ════════════════════════════════════════════════════════════

# ── Main Agent Prompt (unchanged) ────────────────────────────
DBMA_SYSTEM_PROMPT = """
You are **DBMA (Database Management Assistant)** — an advanced AI system that combines:

• A General Knowledge Assistant (like ChatGPT)
• A MySQL Database Expert
• A Query Generator & Analyzer
• A Data Explainer

You can answer:

1. General questions (technology, coding, science, theory, etc.)
2. Database design questions
3. SQL query generation requests
4. SQL debugging & optimization
5. Schema analysis questions
6. Data interpretation questions

═══════════════════════════════════════════════
CURRENT DATABASE CONTEXT  ← ALWAYS USE THIS FOR SQL
═══════════════════════════════════════════════

Active Database: {database_name}

Database Schema (THIS IS THE ONLY SOURCE OF TRUTH FOR SQL):
{schema_context}

Recent Query History:
{query_history}

CRITICAL RULES FOR SQL:
• Use ONLY tables and columns listed in the schema above
• The active database is {database_name} — never reference any other database
• If conversation memory mentions other databases (hospital_database etc.), IGNORE those for SQL
• Never hallucinate table or column names not in the schema above

═══════════════════════════════════════════════
INTENT DETECTION LAYER (VERY IMPORTANT)
═══════════════════════════════════════════════

First classify the user request into ONE category:

1️⃣ GENERAL QUESTION  
Examples:
• "What is DBMS?"
• "Explain normalization"
• "What is machine learning?"

→ Respond like ChatGPT (No SQL)

2️⃣ SQL GENERATION REQUEST  
Examples:
• "Show all patients"
• "List doctors with salary > 50000"
• "Top 5 rides by fare"

→ Generate SQL using rules below

3️⃣ DATABASE METADATA REQUEST  
Examples:
• "Show tables"
• "Describe patients"
• "Database structure"

→ Generate MySQL system queries

4️⃣ HYBRID REQUEST (Explanation + SQL)  
Examples:
• "Explain and show patients older than 60"
• "How to get highest salary? Show query"

→ Provide explanation + SQL

5️⃣ NON-DATABASE CODING / TECH QUESTIONS  
Examples:
• "Explain Python loops"
• "What is API?"

→ Answer normally (No SQL)

═══════════════════════════════════════════════
STRICT SQL GENERATION RULES — FOLLOW EXACTLY
═══════════════════════════════════════════════

RULE 1 — OUTPUT FORMAT (MANDATORY)
Always output SQL inside a ```sql code block.

RULE 2 — USE REAL SCHEMA ONLY
• Use only tables/columns from provided schema
• Never hallucinate names
• If unsure → use SELECT *

RULE 3 — SHOW ALL DATA BY DEFAULT
If user says:
"show", "list", "get", "display"

→ Use:
SELECT * FROM `table`;

No LIMIT unless requested

RULE 4 — LIMIT CONDITIONS
Add LIMIT only if user says:
• top
• first
• limit
• highest N

RULE 5 — BACKTICK FORMATTING
Wrap all identifiers:

✅ `patients`
❌ patients

RULE 6 — MYSQL 8.x SYNTAX ONLY

RULE 7 — ONE QUERY PER RESPONSE

RULE 8 — NO OVER-ENGINEERING
Avoid unnecessary:
• JOINs
• Subqueries
• CTEs

Unless explicitly required

RULE 9 — SAFE FALLBACK
If table exists but columns unclear:

SELECT * FROM `table`;

RULE 10 — NEVER REFUSE SQL
Always produce best possible query

═══════════════════════════════════════════════
RESPONSE FORMAT LOGIC
═══════════════════════════════════════════════

🟢 FOR GENERAL QUESTIONS:

Provide clear, structured explanations like ChatGPT.

Format:
• Definition
• Explanation
• Example (if needed)

NO SQL block.

───────────────────────────────────────────────

🟡 FOR SQL REQUESTS:

Format strictly:

1. One-line explanation
2. SQL code block
3. Optional result note

Example:

I'll retrieve all records from the patients table.

```sql
SELECT * FROM `patients`;

```
"""

# ── Self-Healing Sub-Agent Prompt ────────────────────────────
HEALER_PROMPT = """You are the DBMA Self-Healing Agent. Your ONLY job is to fix broken SQL.

Database: {database_name}
Schema:
{schema_context}

The following SQL query failed with a MySQL error:

FAILED SQL:
```sql
{failed_sql}
```

MYSQL ERROR:
{error_message}

COMMON ERROR TYPES AND FIXES:
• "Unknown column 'X'" → X is misspelled or doesn't exist. Find correct column in schema.
• "Table 'X' doesn't exist" → Table name is wrong. Use correct table from schema.
• "You have an error in your SQL syntax" → Fix syntax near the error position.
• "Column 'X' in field list is ambiguous" → Prefix column with table name: `table`.`column`
• "Unknown table 'X' in MULTI DELETE" → Fix table alias in DELETE statement.

YOUR TASK:
1. Read the error carefully
2. Check the schema for correct names
3. Output the CORRECTED SQL inside a ```sql block
4. Add one line before the SQL explaining what you fixed

RULES:
• Output ONLY the corrected SQL in a ```sql block
• Do NOT repeat the broken SQL
• Do NOT explain at length — one fix-line then the SQL block
• Keep the query logic identical — only fix the error

CORRECTED SQL:"""

# ── Optimizer Sub-Agent Prompt (Agent 2) ─────────────────────
OPTIMIZER_PROMPT = """You are the DBMA SQL Performance Optimizer Agent.

Database: {database_name}
Schema:
{schema_context}

You receive a working SQL query and must optimize it for performance.

ORIGINAL SQL:
```sql
{original_sql}
```

YOUR TASK — Optimize for performance:
• Add WHERE clauses to reduce data scan if beneficial
• Use indexed columns in WHERE/ORDER BY when schema shows indexes
• Avoid SELECT * only if specific columns are clearly more efficient
• Suggest LIMIT for very large tables (>1M rows implied)
• Use EXISTS instead of IN for subqueries where applicable
• Prefer JOINs over correlated subqueries

RULES:
• If the query is already optimal, output it UNCHANGED
• Do NOT change the query's intent or result set
• Output the (possibly optimized) SQL in a ```sql block
• After the SQL block, write "OPTIMIZER_NOTES:" then one line of what changed (or "No changes needed")

Example output:
```sql
SELECT `patient_id`, `name` FROM `patients` WHERE `city` = 'Mumbai' LIMIT 1000;
```
OPTIMIZER_NOTES: Added LIMIT 1000 as safety cap for large table scan."""

# ── Validator Sub-Agent Prompt (Agent 3) ─────────────────────
VALIDATOR_PROMPT = """You are the DBMA SQL Safety Validator Agent.

Database: {database_name}
Schema:
{schema_context}

You receive a SQL query and must validate it for safety and correctness.

SQL TO VALIDATE:
```sql
{sql_to_validate}
```

YOUR TASK:
1. Check for dangerous operations without WHERE clause (DELETE/UPDATE/DROP without WHERE)
2. Check column and table names exist in schema
3. Check SQL syntax is valid MySQL 8.x
4. Check for potential data loss operations
5. Confirm the query does what it logically should

RISK LEVELS:
• LOW    — Safe SELECT/SHOW/DESCRIBE queries
• MEDIUM — INSERT/UPDATE with WHERE clause, CREATE TABLE
• HIGH   — DELETE/UPDATE without WHERE, DROP TABLE, TRUNCATE

OUTPUT FORMAT (follow exactly):
```sql
<the validated SQL — unchanged if correct, or corrected if small issue found>
```
VALIDATOR_NOTES: <one line: what you checked and confirmed or changed>
RISK_LEVEL: <LOW | MEDIUM | HIGH>"""


# ── Summarizer Sub-Agent Prompt ──────────────────────────────
# Used to compress old messages into a memory block.
# Same concept as ChatGPT's internal memory summarization.
SUMMARIZER_PROMPT = """You are the DBMA Memory Summarizer. Your job is to compress a conversation history into a dense, factual summary that preserves ALL important information.

You will receive a list of messages between a user and a database assistant (DBMA).

YOUR TASK:
Create a comprehensive summary that captures:
1. User's personal information (name, role, preferences) — ALWAYS include if mentioned
2. Which databases were discussed and what operations were performed
3. What tables, columns, data were explored
4. Key facts, results, and conclusions from the conversation
5. Any important context that would help answer future questions
6. SQL queries that were generated or executed and their results

RULES:
• Be DENSE and FACTUAL — no fluff, no filler
• Use bullet points for clarity
• ALWAYS preserve: names, numbers, table names, column names, important results
• This summary replaces the original messages — it must be self-contained
• Maximum 400 words

OUTPUT FORMAT:
## User Context
[user's name, role, preferences if mentioned]

## Database Work
[databases used, tables explored, queries run, results found]

## Key Facts & Decisions
[important things discussed, problems solved, conclusions reached]

CONVERSATION TO SUMMARIZE:
{messages_text}

SUMMARY:"""


# ════════════════════════════════════════════════════════════
# MAIN AGENT CLASS
# ════════════════════════════════════════════════════════════

class DBMAAgent:
    """
    The core AI Agent for DBMA.

    Responsibilities:
    - Convert natural language to SQL queries
    - Manage conversation context per database thread
    - Coordinate with PersistenceManager for history
    - Coordinate with MySQLManager for execution
    - Classify user intent
    - Stream LLM responses for real-time output
    - Self-heal failed SQL queries (auto-retry with LLM correction)
    - Multi-agent SQL optimization pipeline (Writer → Optimizer → Validator)
    """

    def __init__(
            self,
            mysql_manager: MySQLManager,
            persistence: PersistenceManager,
    ):
        self.mysql = mysql_manager
        self.persistence = persistence
        self._current_thread_id: Optional[str] = None
        self._current_database: Optional[str] = None
        self._schema_context: str = ""
        self._schema_cache: Optional[Dict] = None

        # ── LLM INTEGRATION: Initialize Ollama LLM ────────────
        # ⚠️  LLM INTEGRATION REQUIRED
        # This initializes the local Ollama model
        # Make sure `ollama serve` is running and your model is pulled
        # ── LangSmith setup ───────────────────────────────────────
        self._ls_client:  LangSmithClient = None
        self._ls_project: str = os.getenv("LANGCHAIN_PROJECT", "DBMA")
        self._ls_active:  bool = False

        api_key  = os.getenv("LANGCHAIN_API_KEY", "")
        tracing  = os.getenv("LANGCHAIN_TRACING_V2", "false").lower() == "true"
        endpoint = os.getenv("LANGCHAIN_ENDPOINT", "https://api.smith.langchain.com")

        if api_key and tracing:
            try:
                self._ls_client = LangSmithClient(
                    api_url=endpoint,
                    api_key=api_key,
                )
                # Quick connectivity test
                list(self._ls_client.list_projects())
                self._ls_active = True
                logger.info(f"LangSmith connected — project: {self._ls_project}")
            except Exception as e:
                logger.warning(f"LangSmith init failed: {e} — tracing disabled")
                self._ls_client = None

        # Build LangChain callback tracer for LLM calls
        self._ls_tracer = None
        if self._ls_active:
            try:
                self._ls_tracer = LangChainTracer(
                    project_name=self._ls_project,
                    client=self._ls_client,
                )
            except Exception as e:
                logger.warning(f"LangChain tracer failed: {e}")

        self._llm = Ollama(
            base_url=ollama_config.base_url,
            model=ollama_config.model,
            temperature=ollama_config.temperature,
            timeout=ollama_config.timeout,
        )
        # ── END LLM INTEGRATION ───────────────────────────────

        logger.info(f"DBMAAgent initialized with model: {ollama_config.model}")
        logger.info(f"LangSmith active: {self._ls_active}")
        logger.info(f"Self-healing: enabled (max {MAX_HEAL_ATTEMPTS} retries)")
        logger.info(f"Multi-agent optimizer: {'enabled' if OPTIMIZER_ENABLED else 'disabled'}")

    # ════════════════════════════════════════════════════════
    # DATABASE CONTEXT
    # ════════════════════════════════════════════════════════

    def set_database_context(self, database_name: str) -> str:
        """
        Switch the agent to a specific database context.
        - Creates or retrieves the thread_id for this database
        - Loads previous chat history from PostgreSQL
        - Refreshes the database schema cache
        Returns the thread_id.
        """
        logger.info(f"Setting database context: {database_name}")
        self._current_database = database_name

        # Get or create the persistent thread for this database
        thread_id = self.persistence.get_or_create_session(database_name)
        self._current_thread_id = thread_id

        # Switch MySQL connection to this database
        self.mysql.use_database(database_name)

        # Load and cache the schema
        self._refresh_schema(database_name, thread_id)

        msg_count = self.persistence.get_message_count(thread_id)
        logger.info(f"Loaded thread {thread_id} with {msg_count} historical messages")

        # Run summarization lazily on DB switch — never during active chat turns.
        # This ensures the summary is always fresh for next session
        # without adding ANY latency to user queries.
        try:
            self.update_summary_if_needed(thread_id)
        except Exception as e:
            logger.debug(f"Lazy summary update skipped: {e}")

        return thread_id

    def _refresh_schema(self, database_name: str, thread_id: str):
        """
        Refresh database schema from MySQL on every database switch.

        ROOT CAUSE FIX:
        The old logic loaded the cached schema from PostgreSQL if ANY cache
        existed — and never re-fetched from MySQL. This meant if the database
        had 0 tables when first connected, the LLM would always see 0 tables
        even after new tables were created (stale cache forever).

        NEW LOGIC:
        1. Always fetch the LIVE schema directly from MySQL first
        2. If MySQL fetch succeeds → update the PostgreSQL cache with fresh data
        3. If MySQL fetch fails (connection issue) → fall back to stale cache
        4. If no cache either → mark schema as unavailable
        """
        # Step 1: Always try live MySQL first — never trust stale cache
        schema = self.mysql.get_full_database_schema(database_name)

        if schema:
            # Got live schema — update cache so it stays fresh
            self._schema_cache   = schema
            self._schema_context = self.mysql.format_schema_for_llm(schema)
            self.persistence.save_schema_cache(thread_id, database_name, schema)
            table_count = len(schema.get("tables", {}))
            logger.info(f"Schema refreshed from MySQL: {table_count} tables in `{database_name}`")
            return

        # Step 2: MySQL fetch failed — fall back to cached schema
        logger.warning(f"Live schema fetch failed for `{database_name}` — trying cache")
        cached = self.persistence.load_schema_cache(thread_id)
        if cached:
            self._schema_cache   = cached.get("schema_json", {})
            self._schema_context = self.mysql.format_schema_for_llm(self._schema_cache)
            logger.warning("Using stale schema cache — may be outdated")
            return

        # Step 3: No live schema, no cache — mark unavailable
        self._schema_cache   = {}
        self._schema_context = f"Database: {database_name} (schema not available — run /refresh)"
        logger.error(f"No schema available for `{database_name}`")

    def refresh_schema_force(self):
        """Force re-extract schema from MySQL (bypasses cache)."""
        if self._current_database and self._current_thread_id:
            with self.persistence._conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM dbma_schema_cache WHERE thread_id = %s",
                    (self._current_thread_id,),
                )
            self._refresh_schema(self._current_database, self._current_thread_id)

    # ════════════════════════════════════════════════════════
    # INTENT CLASSIFICATION
    # ════════════════════════════════════════════════════════

    def classify_intent(self, user_input: str) -> AgentIntent:
        """
        Classify user intent from natural language.
        Uses pattern matching for common intents.
        """
        inp = user_input.lower().strip()

        if re.search(r"\bshow\s+(all\s+)?databases?\b|\blist\s+databases?\b|\bwhat\s+databases?\b", inp):
            return AgentIntent.SHOW_DATABASES
        if re.search(r"\bshow\s+(all\s+)?tables?\b|\blist\s+tables?\b", inp):
            return AgentIntent.SHOW_TABLES
        if re.search(r"\buse\s+\w+\b|\bswitch\s+to\b|\bgo\s+to\s+database\b|\bchange\s+(database\s+)?to\b", inp):
            return AgentIntent.SWITCH_DATABASE
        if re.search(r"\bdescribe\b|\bdesc\b|\bshow\s+columns?\b|\bstructure\s+of\b", inp):
            return AgentIntent.DESCRIBE_TABLE
        if re.search(r"\brun\s+this\b|\bexecute\s+this\b|\byes.*run\b|\bconfirm\b", inp):
            return AgentIntent.EXECUTE_QUERY
        if re.search(r"\bhelp\b|\bwhat\s+can\s+you\b|\bcommands?\b", inp):
            return AgentIntent.HELP
        if re.search(r"\bselect\b|\bget\b|\bfetch\b|\bshow\s+me\b|\bfind\b|\blist\b", inp):
            return AgentIntent.SELECT_QUERY
        if re.search(r"\binsert\b|\badd\b|\bcreate\s+record\b|\bnew\s+row\b", inp):
            return AgentIntent.INSERT_DATA
        if re.search(r"\bupdate\b|\bmodify\b|\bchange\b|\bedit\b", inp):
            return AgentIntent.UPDATE_DATA
        if re.search(r"\bdelete\b|\bremove\b|\bdrop\s+row\b", inp):
            return AgentIntent.DELETE_DATA
        if re.search(r"\bcreate\s+table\b|\bnew\s+table\b", inp):
            return AgentIntent.CREATE_TABLE
        if re.search(r"\bdrop\s+table\b|\bdelete\s+table\b", inp):
            return AgentIntent.DROP_TABLE
        if re.search(r"\bexplain\b|\bwhat\s+is\b|\bhow\s+does\b|\bwhat.*schema\b", inp):
            return AgentIntent.EXPLAIN_SCHEMA

        return AgentIntent.GENERAL_QUESTION

    # ════════════════════════════════════════════════════════
    # MAIN CHAT ENTRY POINT
    # ════════════════════════════════════════════════════════

    def chat(self, user_input: str) -> AgentResponse:
        """
        Main entry point. Creates a LangSmith run manually so it works
        correctly from background threads (where @traceable loses context).
        """
        user_input = user_input.strip()

        # ── LangSmith: open a run for this full chat turn ─────────
        run_id     = str(uuid.uuid4())
        start_time = datetime.datetime.utcnow()
        if self._ls_active and self._ls_client:
            try:
                self._ls_client.create_run(
                    id=run_id,
                    name="DBMA-Chat-Turn",
                    run_type="chain",
                    project_name=self._ls_project,
                    inputs={"user_input": user_input, "database": self._current_database},
                    start_time=start_time,
                )
            except Exception as e:
                logger.debug(f"LangSmith create_run failed: {e}")

        response = self._chat_inner(user_input)

        # ── LangSmith: close the run with output ──────────────────
        if self._ls_active and self._ls_client:
            try:
                self._ls_client.update_run(
                    run_id,
                    outputs={
                        "intent":       response.intent.value,
                        "natural_text": response.natural_text[:500],
                        "sql_query":    response.sql_query,
                        "has_sql":      response.has_sql(),
                        "error":        response.error,
                        "heal_attempts": len(response.heal_attempts),
                        "optimizer_used": response.optimizer_report is not None,
                    },
                    end_time=datetime.datetime.utcnow(),
                    error=response.error,
                )
            except Exception as e:
                logger.debug(f"LangSmith update_run failed: {e}")

        return response

    def _chat_inner(self, user_input: str) -> AgentResponse:
        """Internal chat logic — called by chat() which handles LangSmith wrapping."""

        # ── STEP 1: Classify intent first (before any DB check) ──
        intent = self.classify_intent(user_input)
        logger.debug(f"Intent classified: {intent.value} for input: '{user_input}'")

        # ── STEP 2: Handle quick intents that need no DB ──────────
        quick_response = self._handle_quick_intents(user_input, intent)
        if quick_response is not None:
            self._save_interaction(user_input, quick_response)
            return quick_response

        # ── STEP 3: For complex queries, DB must be selected ──────
        if not self._current_thread_id:
            available_dbs = self.mysql.list_databases()
            db_list = ", ".join(available_dbs) if available_dbs else "none found"
            hint    = f"'use {available_dbs[0]}'" if available_dbs else "create a database first"
            return AgentResponse(
                natural_text=(
                    f"Please select a database first!\n\n"
                    f"Available databases: {db_list}\n\n"
                    f"Just say: {hint}"
                ),
                intent=AgentIntent.UNKNOWN,
                error="No database context",
            )

        # ── STEP 4: Build full context using Rolling Summary ─────────
        #
        # This is the SAME technique ChatGPT uses internally:
        #
        #  ┌─────────────────────────────────────────────────────┐
        #  │  SYSTEM PROMPT                                      │
        #  │  + CONVERSATION SUMMARY (compressed old messages)  │ ← covers ALL old history
        #  │  + RECENT MESSAGES (last 40, full text)            │ ← covers latest context
        #  │  + CURRENT USER MESSAGE                            │
        #  └─────────────────────────────────────────────────────┘
        #
        # Result: LLM effectively has access to the ENTIRE conversation
        # regardless of how many messages exist (100, 500, 1000+)
        # without any token limit issues.

        context_block = self._build_rolling_context(self._current_thread_id)

        query_hist     = self.persistence.get_query_history(self._current_thread_id, limit=5)
        query_hist_str = "\n".join(
            [f"- {q['sql_query'][:80]}... ({'OK' if q['success'] else 'FAILED'})"
             for q in query_hist]
        ) if query_hist else "No previous queries"

        # ── STEP 5: Build system prompt and call LLM ──────────────
        system_prompt = DBMA_SYSTEM_PROMPT.format(
            database_name=self._current_database or "None",
            schema_context=self._schema_context[:3000],
            query_history=query_hist_str,
        )

        # Inject the summary block into system prompt so LLM sees full history.
        # CRITICAL: summary may mention other databases (e.g. hospital_database) from
        # previous sessions. The LLM must NEVER use those table/column names for the
        # CURRENT database — always use the schema_context above as sole truth.
        if context_block["summary"]:
            system_prompt += (
                f"\n\n══════════════════════════════════════════════════════"
                f"\nCONVERSATION MEMORY — PERSONAL HISTORY ONLY"
                f"\n══════════════════════════════════════════════════════"
                f"\nIMPORTANT: This is a memory of past conversations for context."
                f"\nDO NOT use any table names, column names, or database names from"
                f"\nthis memory for SQL generation. For SQL always use ONLY the"
                f"\n'Database Schema' section above (current active database: {self._current_database})."
                f"\n\n{context_block['summary']}"
            )

        messages = [{"role": "system", "content": system_prompt}]
        messages.extend(context_block["recent_formatted"])   # last 40 full messages
        messages.append({"role": "user", "content": user_input})

        try:
            llm_response_text = self._invoke_llm(messages)
        except Exception as e:
            logger.error(f"LLM invocation failed: {e}")
            return AgentResponse(
                natural_text=(
                    f"⚠️ LLM Error: {str(e)}\n\n"
                    f"Make sure Ollama is running: ollama serve\n"
                    f"And model is pulled: ollama pull {ollama_config.model}"
                ),
                intent=intent,
                error=str(e),
            )

        raw_sql      = self._extract_sql(llm_response_text)
        natural_text = self._clean_response_text(llm_response_text)

        # ── STEP 6: Multi-Agent Optimizer Pipeline ────────────────
        # Only runs for SQL queries (not general chat / metadata queries)
        optimizer_report: Optional[OptimizerReport] = None
        final_sql = raw_sql

        # Optimizer only runs for real data queries — never for metadata/simple intents.
        # SHOW TABLES, SHOW DATABASES, DESCRIBE etc. need zero optimization
        # and skipping saves 2 full LLM round-trips per simple question.
        _optimizer_intents = {
            AgentIntent.SELECT_QUERY,
            AgentIntent.INSERT_DATA,
            AgentIntent.UPDATE_DATA,
            AgentIntent.DELETE_DATA,
            AgentIntent.CREATE_TABLE,
        }
        if raw_sql and OPTIMIZER_ENABLED and intent in _optimizer_intents:
            try:
                optimizer_report = self._run_optimizer_pipeline(raw_sql)
                final_sql = optimizer_report.final_sql

                # Append optimizer summary to the chat response
                if optimizer_report.was_modified:
                    natural_text += (
                        f"\n\n🔧 **Optimizer Pipeline Report:**\n"
                        f"• Optimizer : {optimizer_report.optimizer_notes}\n"
                        f"• Validator : {optimizer_report.validator_notes}\n"
                        f"• Risk Level: {optimizer_report.risk_level}"
                    )
                else:
                    natural_text += (
                        f"\n\n✅ **Optimizer:** Query verified — no changes needed. "
                        f"Risk: {optimizer_report.risk_level}"
                    )
            except Exception as e:
                logger.warning(f"Optimizer pipeline failed (using original SQL): {e}")
                # Don't crash — fall back to original SQL

        requires_confirmation = self._is_destructive(final_sql)
        auto_execute          = intent in (AgentIntent.SHOW_DATABASES, AgentIntent.SHOW_TABLES)

        response = AgentResponse(
            natural_text=natural_text,
            sql_query=final_sql,
            intent=intent,
            requires_confirmation=requires_confirmation,
            auto_execute=auto_execute,
            optimizer_report=optimizer_report,
        )

        self._save_interaction(user_input, response)
        return response

    # ════════════════════════════════════════════════════════
    # FEATURE 1 — SELF-HEALING QUERY RETRY LOOP
    # ════════════════════════════════════════════════════════

    def execute_with_healing(
        self,
        sql: str,
        on_attempt_callback=None,
    ) -> Tuple[QueryResult, List[HealAttempt]]:
        """
        Execute SQL with automatic self-healing on failure.

        Flow:
        1. Execute the SQL query
        2. If it succeeds → return result immediately
        3. If it fails → send error + SQL to the Healer LLM sub-agent
        4. Healer analyzes error, corrects SQL
        5. Execute corrected SQL
        6. Repeat up to MAX_HEAL_ATTEMPTS times
        7. If all retries fail → return final error

        Args:
            sql:                 The SQL to execute
            on_attempt_callback: Optional callable(attempt_no, status_msg)
                                 called after each attempt for UI updates

        Returns:
            Tuple of (QueryResult, list of HealAttempt records)
        """
        if not self._current_database:
            result = QueryResult(success=False, query=sql,
                                 error="No database selected")
            return result, []

        heal_log: List[HealAttempt] = []
        current_sql = sql

        for attempt in range(MAX_HEAL_ATTEMPTS + 1):  # +1 for initial attempt

            # ── Execute current SQL ───────────────────────────
            result = self.mysql.execute_query(current_sql)

            if result.success:
                # ✅ Success — log if this was a healed attempt
                if attempt > 0:
                    heal_log[-1].success     = True
                    heal_log[-1].execution_ms = result.execution_ms
                    logger.info(f"Self-healing succeeded on attempt {attempt}")
                    if on_attempt_callback:
                        on_attempt_callback(attempt, f"✅ Healed on attempt {attempt} — query succeeded")
                return result, heal_log

            # ── Query failed ──────────────────────────────────
            error_msg = result.error or "Unknown error"
            logger.warning(f"Query failed (attempt {attempt}): {error_msg}")

            if attempt == MAX_HEAL_ATTEMPTS:
                # Hit retry limit — give up
                logger.error(f"Self-healing exhausted after {MAX_HEAL_ATTEMPTS} attempts")
                if on_attempt_callback:
                    on_attempt_callback(
                        attempt,
                        f"❌ Auto-heal failed after {MAX_HEAL_ATTEMPTS} attempts.\n"
                        f"Last error: {error_msg}"
                    )
                return result, heal_log

            # ── Attempt self-healing ──────────────────────────
            if on_attempt_callback:
                on_attempt_callback(
                    attempt,
                    f"⚠️ Query failed: {error_msg}\n"
                    f"🔄 Self-healing attempt {attempt + 1}/{MAX_HEAL_ATTEMPTS}..."
                )

            corrected_sql = self._heal_sql(current_sql, error_msg)

            if not corrected_sql or corrected_sql.strip() == current_sql.strip():
                # LLM couldn't suggest a different fix — stop trying
                logger.warning("Healer returned same/empty SQL — stopping retry loop")
                if on_attempt_callback:
                    on_attempt_callback(
                        attempt,
                        f"❌ Healer could not find a correction. Manual fix required."
                    )
                return result, heal_log

            # Record this healing attempt
            heal_log.append(HealAttempt(
                attempt_no=attempt + 1,
                original_sql=current_sql,
                error_message=error_msg,
                corrected_sql=corrected_sql,
                success=False,  # will be updated above if next attempt succeeds
            ))

            logger.info(f"Healer produced corrected SQL: {corrected_sql[:80]}...")
            if on_attempt_callback:
                on_attempt_callback(
                    attempt,
                    f"🧠 Healer correction: {corrected_sql[:100]}..."
                )

            current_sql = corrected_sql  # try the corrected SQL next iteration

        # Should never reach here
        return result, heal_log

    def _heal_sql(self, failed_sql: str, error_message: str) -> Optional[str]:
        """
        Call the Healer sub-agent LLM to correct a broken SQL query.
        Returns the corrected SQL string, or None if healing failed.
        """
        logger.info(f"Invoking Healer for error: {error_message[:80]}")

        healer_prompt = HEALER_PROMPT.format(
            database_name=self._current_database or "unknown",
            schema_context=self._schema_context[:2000],
            failed_sql=failed_sql,
            error_message=error_message,
        )

        try:
            # Direct LLM call — no conversation history needed for healing
            # /no_think suppresses qwen3 chain-of-thought
            full_prompt = f"[SYSTEM]\n{healer_prompt}\n\n[ASSISTANT]\n/no_think\n"
            healer_response = self._llm.invoke(full_prompt)

            # Log to LangSmith as a healer sub-run
            if self._ls_active and self._ls_client:
                try:
                    run_id = str(uuid.uuid4())
                    self._ls_client.create_run(
                        id=run_id, name="DBMA-Healer", run_type="llm",
                        project_name=self._ls_project,
                        inputs={"failed_sql": failed_sql, "error": error_message},
                        start_time=datetime.datetime.utcnow(),
                    )
                    self._ls_client.update_run(
                        run_id,
                        outputs={"healer_response": healer_response[:500]},
                        end_time=datetime.datetime.utcnow(),
                    )
                except Exception:
                    pass

            corrected = self._extract_sql(healer_response)
            logger.debug(f"Healer returned: {corrected}")
            return corrected

        except Exception as e:
            logger.error(f"Healer LLM call failed: {e}")
            return None

    def format_heal_report(self, heal_attempts: List[HealAttempt]) -> str:
        """
        Format self-healing attempts into a human-readable report
        for display in the chat panel.
        """
        if not heal_attempts:
            return ""

        lines = ["\n🔬 **Self-Healing Report:**"]
        for a in heal_attempts:
            status = "✅ Fixed" if a.success else "❌ Failed"
            lines.append(
                f"  Attempt {a.attempt_no}: {status}\n"
                f"  Error   : {a.error_message[:80]}\n"
                f"  Fix     : {a.corrected_sql[:80]}..."
            )
        return "\n".join(lines)

    # ════════════════════════════════════════════════════════
    # FEATURE 2 — MULTI-AGENT SQL OPTIMIZER PIPELINE
    # ════════════════════════════════════════════════════════

    def _run_optimizer_pipeline(self, original_sql: str) -> OptimizerReport:
        """
        Run the 3-agent SQL optimization pipeline:

        Agent 1 (Writer)    → Already done — original_sql is the input
        Agent 2 (Optimizer) → Rewrites for performance
        Agent 3 (Validator) → Validates safety and correctness

        Returns an OptimizerReport with all outputs.
        """
        db   = self._current_database or "unknown"
        sch  = self._schema_context[:1500]

        # ── Agent 2: Optimizer ────────────────────────────────
        logger.info("Optimizer Pipeline: Agent 2 (Optimizer) running...")
        optimizer_response = self._invoke_sub_agent(
            agent_name="DBMA-Optimizer",
            prompt=OPTIMIZER_PROMPT.format(
                database_name=db,
                schema_context=sch,
                original_sql=original_sql,
            ),
        )

        optimized_sql    = self._extract_sql(optimizer_response) or original_sql
        optimizer_notes  = self._extract_tagged_line(optimizer_response, "OPTIMIZER_NOTES") \
                           or "No changes needed"

        # ── Agent 3: Validator ────────────────────────────────
        logger.info("Optimizer Pipeline: Agent 3 (Validator) running...")
        validator_response = self._invoke_sub_agent(
            agent_name="DBMA-Validator",
            prompt=VALIDATOR_PROMPT.format(
                database_name=db,
                schema_context=sch,
                sql_to_validate=optimized_sql,
            ),
        )

        final_sql        = self._extract_sql(validator_response) or optimized_sql
        validator_notes  = self._extract_tagged_line(validator_response, "VALIDATOR_NOTES") \
                           or "Validated — no issues found"
        risk_level       = self._extract_tagged_line(validator_response, "RISK_LEVEL") \
                           or self._infer_risk_level(final_sql)

        # Normalize risk level
        risk_level = risk_level.strip().upper()
        if risk_level not in ("LOW", "MEDIUM", "HIGH"):
            risk_level = self._infer_risk_level(final_sql)

        was_modified = (
            final_sql.strip().lower() != original_sql.strip().lower()
        )

        logger.info(
            f"Optimizer Pipeline complete | "
            f"Modified: {was_modified} | Risk: {risk_level}"
        )

        return OptimizerReport(
            original_sql=original_sql,
            optimized_sql=optimized_sql,
            final_sql=final_sql,
            optimizer_notes=optimizer_notes,
            validator_notes=validator_notes,
            risk_level=risk_level,
            was_modified=was_modified,
        )

    def _invoke_sub_agent(self, agent_name: str, prompt: str) -> str:
        """
        Invoke a focused sub-agent with a specific prompt.
        Logs to LangSmith as a child run under the current session.
        """
        full_prompt = f"[SYSTEM]\n{prompt}\n\n[ASSISTANT]\n/no_think\n"

        run_id = str(uuid.uuid4())
        if self._ls_active and self._ls_client:
            try:
                self._ls_client.create_run(
                    id=run_id, name=agent_name, run_type="llm",
                    project_name=self._ls_project,
                    inputs={"prompt": full_prompt[-1500:]},
                    start_time=datetime.datetime.utcnow(),
                )
            except Exception:
                pass

        response  = ""
        error_msg = None
        try:
            response = self._llm.invoke(full_prompt)
        except Exception as e:
            error_msg = str(e)
            logger.error(f"{agent_name} failed: {e}")
            raise
        finally:
            if self._ls_active and self._ls_client:
                try:
                    self._ls_client.update_run(
                        run_id,
                        outputs={"response": response[:1000]},
                        end_time=datetime.datetime.utcnow(),
                        error=error_msg,
                    )
                except Exception:
                    pass

        return response

    def _extract_tagged_line(self, text: str, tag: str) -> Optional[str]:
        """
        Extract a line like 'TAG: value' from LLM response.
        Used to parse OPTIMIZER_NOTES, VALIDATOR_NOTES, RISK_LEVEL.
        """
        m = re.search(rf"{tag}\s*:\s*(.+)", text, re.IGNORECASE)
        return m.group(1).strip() if m else None

    def _infer_risk_level(self, sql: str) -> str:
        """Infer risk level from SQL type when Validator doesn't specify."""
        if not sql:
            return "LOW"
        first = sql.strip().split()[0].upper()
        if first in ("DELETE", "DROP", "TRUNCATE"):
            return "HIGH"
        if first in ("UPDATE", "INSERT", "ALTER", "CREATE"):
            return "MEDIUM"
        return "LOW"

    # ════════════════════════════════════════════════════════
    # STREAMING VERSION
    # ════════════════════════════════════════════════════════

    def chat_stream(self, user_input: str) -> Generator[str, None, AgentResponse]:
        """
        Streaming version of chat() — yields text chunks as LLM generates them.
        ⚠️  LLM INTEGRATION REQUIRED — Streaming via Ollama
        """
        if not self._current_thread_id:
            yield "⚠️ No database selected. Please say 'use <database_name>' first."
            return

        recent_msgs       = self.persistence.get_recent_messages(self._current_thread_id, n=20)
        history_formatted = self.persistence.format_history_for_llm(recent_msgs)
        query_hist        = self.persistence.get_query_history(self._current_thread_id, limit=5)
        query_hist_str    = "\n".join(
            [f"- {q['sql_query'][:80]}..." for q in query_hist]
        ) if query_hist else "No previous queries"

        system_prompt = DBMA_SYSTEM_PROMPT.format(
            database_name=self._current_database or "None",
            schema_context=self._schema_context[:3000],
            query_history=query_hist_str,
        )

        messages = [{"role": "system", "content": system_prompt}]
        messages.extend(history_formatted[-16:])
        messages.append({"role": "user", "content": user_input})

        full_response = ""
        try:
            for chunk in self._stream_llm(messages):
                full_response += chunk
                yield chunk
        except Exception as e:
            yield f"\n⚠️ Stream error: {e}"
            return

        sql_query    = self._extract_sql(full_response)
        intent       = self.classify_intent(user_input)
        natural_text = self._clean_response_text(full_response)

        response = AgentResponse(
            natural_text=natural_text,
            sql_query=sql_query,
            intent=intent,
            requires_confirmation=self._is_destructive(sql_query),
        )
        self._save_interaction(user_input, response)
        return response

    # ════════════════════════════════════════════════════════
    # LLM INTEGRATION METHODS
    # ════════════════════════════════════════════════════════

    def _invoke_llm(self, messages: List[Dict[str, str]], parent_run_id: str = None) -> str:
        """
        Invoke Ollama with full LangSmith tracing.
        Uses direct LangSmith Client API (thread-safe, no @traceable needed).

        FIX 1: role "human" mapped to "user" so history is NOT silently dropped.
        FIX 2: /no_think appended to suppress qwen3 chain-of-thought tokens.
        """
        prompt_parts = []
        for msg in messages:
            role    = msg["role"]
            content = msg["content"]
            if role == "system":
                prompt_parts.append(f"[SYSTEM]\n{content}\n")
            elif role in ("user", "human"):   # FIX 1: accept both role names
                prompt_parts.append(f"[USER]\n{content}\n")
            elif role == "assistant":
                prompt_parts.append(f"[ASSISTANT]\n{content}\n")

        # FIX 2: /no_think suppresses <think>...</think> output from qwen3/deepseek
        full_prompt = "\n".join(prompt_parts) + "\n[ASSISTANT]\n/no_think\n"

        # ── Open LangSmith LLM run ────────────────────────────────
        llm_run_id  = str(uuid.uuid4())
        llm_start   = datetime.datetime.utcnow()
        if self._ls_active and self._ls_client:
            try:
                self._ls_client.create_run(
                    id=llm_run_id,
                    name="Ollama-LLM",
                    run_type="llm",
                    project_name=self._ls_project,
                    parent_run_id=parent_run_id,
                    inputs={"prompt": full_prompt[-2000:], "model": ollama_config.model},
                    start_time=llm_start,
                    extra={"model": ollama_config.model, "temperature": ollama_config.temperature},
                )
            except Exception as e:
                logger.debug(f"LangSmith LLM run open failed: {e}")

        # ── Call Ollama ───────────────────────────────────────────
        response  = ""
        error_msg = None
        try:
            callbacks = [self._ls_tracer] if self._ls_tracer else None
            response  = self._llm.invoke(
                full_prompt,
                config={"callbacks": callbacks} if callbacks else {},
            )
        except Exception as e:
            error_msg = str(e)
            raise

        # ── Close LangSmith LLM run ───────────────────────────────
        if self._ls_active and self._ls_client:
            try:
                self._ls_client.update_run(
                    llm_run_id,
                    outputs={"response": response[:2000]},
                    end_time=datetime.datetime.utcnow(),
                    error=error_msg,
                )
            except Exception as e:
                logger.debug(f"LangSmith LLM run close failed: {e}")

        return response

    def _stream_llm(self, messages: List[Dict[str, str]]) -> Generator[str, None, None]:
        """
        ⚠️  LLM INTEGRATION REQUIRED
        Stream tokens from Ollama LLM.

        FIX 1: role "human" mapped to "user" so history is NOT silently dropped.
        FIX 2: /no_think appended to suppress qwen3 chain-of-thought tokens.
        """
        prompt_parts = []
        for msg in messages:
            role    = msg["role"]
            content = msg["content"]
            if role == "system":
                prompt_parts.append(f"[SYSTEM]\n{content}\n")
            elif role in ("user", "human"):   # FIX 1: accept both role names
                prompt_parts.append(f"[USER]\n{content}\n")
            elif role == "assistant":
                prompt_parts.append(f"[ASSISTANT]\n{content}\n")

        # FIX 2: /no_think suppresses <think>...</think> output from qwen3/deepseek
        full_prompt = "\n".join(prompt_parts) + "\n[ASSISTANT]\n/no_think\n"

        # ⚠️  LLM INTEGRATION REQUIRED — Stream from Ollama
        for chunk in self._llm.stream(full_prompt):
            yield chunk

    # ════════════════════════════════════════════════════════
    # QUICK INTENT HANDLERS (No LLM, No DB needed)
    # ════════════════════════════════════════════════════════

    def _handle_quick_intents(
            self,
            user_input: str,
            intent: AgentIntent,
    ) -> Optional[AgentResponse]:
        """
        Handle intents that work WITHOUT a database selected.
        Returns None if this intent needs DB context.

        FIX 1: SHOW DATABASES — always works, no DB needed
        FIX 2: SHOW TABLES    — shows helpful message if no DB selected
        FIX 3: HELP           — always works, no DB needed
        FIX 4: USE <db>       — fixed regex so "change database to X" works correctly
        """

        # ── SHOW DATABASES (no DB required) ──────────────────
        if intent == AgentIntent.SHOW_DATABASES:
            return AgentResponse(
                natural_text="Showing all available MySQL databases:",
                sql_query="SHOW DATABASES",
                intent=intent,
                auto_execute=True,
                requires_confirmation=False,
            )

        # ── SHOW TABLES (needs DB, gives helpful msg if none) ─
        if intent == AgentIntent.SHOW_TABLES:
            if not self._current_database:
                available = self.mysql.list_databases()
                db_list   = ", ".join(available) if available else "none"
                return AgentResponse(
                    natural_text=(
                        f"Please select a database first to show its tables.\n\n"
                        f"Available databases: {db_list}\n"
                        f"Say: 'use <database_name>'"
                    ),
                    intent=intent,
                    requires_confirmation=False,
                )
            return AgentResponse(
                natural_text=f"Showing all tables in `{self._current_database}`:",
                sql_query=f"SHOW TABLES FROM `{self._current_database}`",
                intent=intent,
                auto_execute=True,
                requires_confirmation=False,
            )

        # ── HELP (no DB required) ─────────────────────────────
        if intent == AgentIntent.HELP:
            return AgentResponse(
                natural_text=self._get_help_text(),
                intent=intent,
                requires_confirmation=False,
            )

        # ── USE DATABASE (no DB required) ─────────────────────
        # FIX 4: improved regex — skips the word "DATABASE" being captured
        # as the db name (e.g. "change database to dbma_db" now works correctly)
        use_match = re.search(
            r"(?:use|switch\s+to|go\s+to|connect\s+to|change(?:\s+database)?\s+to)"
            r"\s+(?:(?:database|db)\s+)?[`'\"]?((?!database\b|db\b)\w+)[`'\"]?",
            user_input,
            re.IGNORECASE,
        )
        if use_match:
            db_name  = use_match.group(1)
            reserved = {"database", "db", "table", "to", "the", "a", "an"}
            if db_name.lower() not in reserved:
                return AgentResponse(
                    natural_text=f"Switching to database `{db_name}`...",
                    sql_query=f"USE `{db_name}`",
                    intent=AgentIntent.SWITCH_DATABASE,
                    auto_execute=True,
                    requires_confirmation=False,
                    metadata={"target_database": db_name},
                )

        # Everything else needs LLM — return None
        return None

    # ════════════════════════════════════════════════════════
    # UTILITY METHODS
    # ════════════════════════════════════════════════════════

    def _build_rolling_context(self, thread_id: str) -> dict:
        """
        Build conversation context using ONLY fast PostgreSQL queries.
        NO LLM calls here — zero extra latency per chat turn.

        PERFORMANCE FIX:
        The old version called _summarize_messages() inline (an LLM call)
        every time unsummarized messages exceeded RECENT_MESSAGES_COUNT.
        With 100+ messages this triggered on EVERY chat turn = 2x slower.

        NEW APPROACH:
        - Just load: existing summary (from DB) + last 40 messages (from DB)
        - Both are pure SQL queries < 5ms each
        - Summarization only happens in update_summary_if_needed() which is
          called lazily on database switch, never during active chat

        The LLM still gets full context:
          [summary of old messages] + [last 40 full messages]
        """
        try:
            existing = self.persistence.load_conversation_summary(thread_id)

            if existing:
                summary_text  = existing["summary_text"]
                last_seq      = existing["summarized_up_to_seq"]
                # Load recent messages that are NOT yet in the summary
                unsummarized  = self.persistence.get_messages_after_seq(thread_id, last_seq)
            else:
                summary_text  = ""
                unsummarized  = self.persistence.get_recent_messages(thread_id, n=RECENT_MESSAGES_COUNT)

            # Always cap at RECENT_MESSAGES_COUNT for token budget
            to_keep          = unsummarized[-RECENT_MESSAGES_COUNT:] if len(unsummarized) > RECENT_MESSAGES_COUNT else unsummarized
            recent_formatted = self.persistence.format_history_for_llm(to_keep)

            return {"summary": summary_text, "recent_formatted": recent_formatted}

        except Exception as e:
            logger.error(f"_build_rolling_context failed: {e}")
            msgs = self.persistence.get_recent_messages(thread_id, n=RECENT_MESSAGES_COUNT)
            return {
                "summary": "",
                "recent_formatted": self.persistence.format_history_for_llm(msgs),
            }

    def update_summary_if_needed(self, thread_id: str) -> None:
        """
        Run summarization lazily — called only on database switch,
        NEVER during active chat turns (zero impact on response time).

        This updates the summary in the background so next chat session
        starts with a fresh compressed memory of old messages.
        """
        try:
            existing     = self.persistence.load_conversation_summary(thread_id)
            last_seq     = existing["summarized_up_to_seq"] if existing else 0
            unsummarized = self.persistence.get_messages_after_seq(thread_id, last_seq)

            # Only summarize if enough new messages have piled up
            if len(unsummarized) <= RECENT_MESSAGES_COUNT:
                return  # nothing to summarize yet

            to_fold      = unsummarized[:-RECENT_MESSAGES_COUNT]
            summary_text = self._summarize_messages(
                existing_summary=existing["summary_text"] if existing else "",
                new_messages=to_fold,
            )
            self.persistence.save_conversation_summary(
                thread_id=thread_id,
                summary_text=summary_text,
                summarized_up_to_seq=to_fold[-1].sequence_no,
                message_count_summarized=(
                    (existing["message_count_summarized"] if existing else 0) + len(to_fold)
                ),
            )
            logger.info(f"Summary updated: {len(to_fold)} messages folded for thread {thread_id}")
        except Exception as e:
            logger.error(f"update_summary_if_needed failed: {e}")

    def _summarize_messages(
        self,
        existing_summary: str,
        new_messages: list,
    ) -> str:
        """
        Call LLM to compress messages into a dense summary.
        If an existing summary is provided, the new messages are
        folded INTO it (incremental update, same as ChatGPT).
        """
        try:
            # Build plain-text transcript of messages to summarize
            lines = []
            if existing_summary:
                lines.append("=== EXISTING SUMMARY (keep all facts from here) ===")
                lines.append(existing_summary)
                lines.append("\n=== NEW MESSAGES TO ADD TO SUMMARY ===")

            for msg in new_messages:
                role = "User" if msg.role in ("human", "user") else "DBMA"
                text = (msg.content or "").strip()
                if msg.sql_query:
                    text += f"\n[SQL: {msg.sql_query.strip()}]"
                if text:
                    lines.append(f"{role}: {text}")

            messages_text = "\n".join(lines)

            prompt = SUMMARIZER_PROMPT.format(messages_text=messages_text)
            result = self._invoke_llm([
                {"role": "system", "content": "You are a precise conversation summarizer. Output only the summary, no preamble."},
                {"role": "user",   "content": prompt},
            ])
            # Strip think blocks if any
            result = re.sub(r"<think>.*?</think>", "", result, flags=re.DOTALL).strip()
            return result if result else existing_summary

        except Exception as e:
            logger.error(f"_summarize_messages failed: {e}")
            return existing_summary  # keep old summary on failure

    def _maybe_update_summary(self, thread_id: str) -> None:
        """
        Check if enough new messages have arrived since last summary
        to trigger a background re-summarization.
        Runs non-blocking — never delays user response.
        """
        try:
            existing = self.persistence.load_conversation_summary(thread_id)
            if not existing:
                return  # will be built on next _build_rolling_context call

            # Count messages since last summarization
            new_since = self.persistence.get_messages_after_seq(
                thread_id, existing["summarized_up_to_seq"]
            )
            # Only trigger if we have more unsummarized messages than RECENT_MESSAGES_COUNT
            # (i.e. the oldest ones are about to fall off the recent window)
            if len(new_since) > RECENT_MESSAGES_COUNT + SUMMARY_UPDATE_EVERY:
                logger.debug(f"Summary update triggered: {len(new_since)} unsummarized msgs")
                # _build_rolling_context will handle the update on next chat call
        except Exception as e:
            logger.debug(f"_maybe_update_summary failed: {e}")

    def _extract_personal_context(self, thread_id: str) -> str:
        """
        Scan the FULL conversation history for personal facts the user has shared:
        name, preferences, role, location, etc.

        WHY THIS EXISTS:
        get_recent_messages(n=40) only gives the last 40 messages.
        If the user said "my name is Vikas" 100 messages ago, it's outside
        the context window — LLM forgets it completely.

        This method scans ALL messages (no limit), finds personal statements,
        and injects them into the system prompt so the LLM always remembers.

        Returns a formatted string of facts, or empty string if none found.
        """
        try:
            # Load complete history — no limit, scan everything
            all_messages = self.persistence.load_chat_history(thread_id, limit=None)
            if not all_messages:
                return ""

            facts = []
            # Patterns that indicate personal information being shared
            personal_patterns = [
                # Name patterns
                (r"my name is ([\w\s]+)", "User's name is {}"),
                (r"i am ([\w\s]+)", "User said they are {}"),
                (r"i'm ([\w\s]+)", "User said they are {}"),
                (r"call me ([\w\s]+)", "User wants to be called {}"),
                (r"people call me ([\w\s]+)", "User goes by {}"),
                # Role / profession
                (r"i(?:'m| am) a(?:n)? ([\w\s]+(?:developer|engineer|student|doctor|manager|analyst|designer|researcher|teacher|professor|scientist))", "User's role: {}"),
                (r"i work as (?:a(?:n)? )?([\w\s]+)", "User works as {}"),
                (r"i(?:'m| am) studying ([\w\s]+)", "User is studying {}"),
                # Preferences
                (r"i prefer ([\w\s]+)", "User prefers {}"),
                (r"i like ([\w\s]+)", "User likes {}"),
                (r"i(?:'m| am) from ([\w\s]+)", "User is from {}"),
            ]

            seen_facts = set()  # deduplicate
            for msg in all_messages:
                if msg.role not in ("human", "user"):
                    continue
                text = (msg.content or "").lower().strip()
                for pattern, template in personal_patterns:
                    m = re.search(pattern, text, re.IGNORECASE)
                    if m:
                        value = m.group(1).strip().title()
                        # Filter out very short or generic matches
                        if len(value) >= 2 and value.lower() not in (
                            "a", "an", "the", "not", "no", "yes", "ok", "here", "there"
                        ):
                            fact = template.format(value)
                            if fact not in seen_facts:
                                seen_facts.add(fact)
                                facts.append(fact)

            if not facts:
                return ""

            return "Known facts about this user:\n" + "\n".join(f"• {f}" for f in facts[:10])

        except Exception as e:
            logger.debug(f"_extract_personal_context failed: {e}")
            return ""

    def _extract_sql(self, llm_response: str) -> Optional[str]:
        """
        Robustly extract SQL from LLM response.
        Tries multiple patterns in order of reliability.

        FIX 3: strips <think>...</think> blocks FIRST so qwen3/deepseek
        reasoning tokens never interfere with SQL extraction.
        """
        # FIX 3: strip <think> tokens before any parsing
        text = re.sub(r"<think>[\s\S]*?</think>", "", llm_response, flags=re.IGNORECASE)
        text = re.sub(r"<think>[\s\S]*$",          "", text,         flags=re.IGNORECASE)
        text = re.sub(r"</?think>",                 "", text,         flags=re.IGNORECASE)

        SQL_KEYWORDS = (
            r"SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|SHOW|"
            r"DESCRIBE|DESC|USE|TRUNCATE|CALL|GRANT|REVOKE|EXPLAIN"
        )

        # 1. ```sql ... ``` block (most reliable — what we ask LLM to use)
        m = re.search(r"```sql\s*(.*?)\s*```", text, re.DOTALL | re.IGNORECASE)
        if m:
            sql = m.group(1).strip()
            if sql:
                return sql

        # 2. ``` ... ``` block without language tag
        m = re.search(r"```\s*(.*?)\s*```", text, re.DOTALL)
        if m:
            sql = m.group(1).strip()
            if re.match(rf"^({SQL_KEYWORDS})\b", sql, re.IGNORECASE):
                return sql

        # 3. Multi-line SQL block — find first SQL keyword line and grab
        #    everything until the next blank line or end of text
        m = re.search(
            rf"(?:^|\n)((?:{SQL_KEYWORDS})\b.*?)(?:\n\n|\n[A-Z*]|$)",
            text,
            re.DOTALL | re.IGNORECASE,
        )
        if m:
            candidate = m.group(1).strip()
            if len(candidate.split()) >= 2:
                if not candidate.endswith(";"):
                    candidate += ";"
                return candidate

        # 4. Last resort — any line starting with SQL keyword
        for line in text.splitlines():
            line = line.strip()
            if re.match(rf"^({SQL_KEYWORDS})\b", line, re.IGNORECASE):
                if len(line.split()) >= 2:
                    if not line.endswith(";"):
                        line += ";"
                    return line

        return None

    def _clean_response_text(self, llm_response: str) -> str:
        """
        Remove SQL code blocks from response for display in chat panel.

        FIX 3: also strips <think>...</think> blocks from qwen3/deepseek
        so reasoning tokens never appear in the chat UI.
        """
        # FIX 3: strip <think> reasoning tokens (qwen3, deepseek-r1)
        cleaned = re.sub(r"<think>[\s\S]*?</think>", "", llm_response, flags=re.IGNORECASE)
        cleaned = re.sub(r"<think>[\s\S]*$",          "", cleaned,      flags=re.IGNORECASE)
        cleaned = re.sub(r"</?think>",                 "", cleaned,      flags=re.IGNORECASE)

        # Strip SQL code blocks (shown separately in query input box)
        cleaned = re.sub(r"```sql.*?```", "", cleaned, flags=re.DOTALL | re.IGNORECASE)
        cleaned = re.sub(r"```.*?```",    "", cleaned, flags=re.DOTALL)

        # Collapse excessive blank lines left behind
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)

        return cleaned.strip()

    def _is_destructive(self, sql: Optional[str]) -> bool:
        """Check if a SQL query is potentially destructive."""
        if not sql:
            return False
        first_word = sql.strip().split()[0].upper()
        return first_word in {"DELETE", "DROP", "TRUNCATE", "UPDATE"}

    def _save_interaction(self, user_input: str, response: AgentResponse):
        """
        Save both sides of a conversation to PostgreSQL.
        FIX: Silently skips if no thread_id yet — allows SHOW DATABASES
        and HELP to work before any DB is selected without crashing.
        """
        if not self._current_thread_id:
            return  # No thread yet — safe skip, no crash

        try:
            self.persistence.save_message(
                thread_id=self._current_thread_id,
                role="human",
                content=user_input,
            )
            self.persistence.save_message(
                thread_id=self._current_thread_id,
                role="assistant",
                content=response.natural_text,
                sql_query=response.sql_query,
                metadata={"intent": response.intent.value},
            )
        except Exception as e:
            logger.warning(f"Could not save interaction: {e}")

    def _get_help_text(self) -> str:
        return """
╔══════════════════════════════════════════════════════╗
║              DBMA — Help & Commands                  ║
╠══════════════════════════════════════════════════════╣
║  Natural Language Examples:                          ║
║  • "show me all databases"                           ║
║  • "use my_database"                                 ║
║  • "show all tables"                                 ║
║  • "get all users where age > 25"                    ║
║  • "add a new product named Widget priced at 9.99"   ║
║  • "how many orders were placed today?"              ║
║  • "describe the customers table"                    ║
║  • "show schema of the orders table"                 ║
║  • "create a table for employee records"             ║
║                                                      ║
║  Direct Commands:                                    ║
║  • /refresh  — Refresh database schema cache         ║
║  • /history  — Show recent query history             ║
║  • /clear    — Clear chat history for this DB        ║
║  • /sessions — List all database sessions            ║
║  • /exit     — Exit DBMA                             ║
╚══════════════════════════════════════════════════════╝
""".strip()

    # ════════════════════════════════════════════════════════
    # STATE PROPERTIES
    # ════════════════════════════════════════════════════════

    @property
    def current_thread_id(self) -> Optional[str]:
        return self._current_thread_id

    @property
    def current_database(self) -> Optional[str]:
        return self._current_database

    @property
    def schema_summary(self) -> str:
        """Returns a brief schema summary for UI display."""
        if not self._schema_cache:
            return "No schema loaded"
        tables = list(self._schema_cache.get("tables", {}).keys())
        if not tables:
            return "No tables found"
        return f"{len(tables)} tables: {', '.join(tables[:5])}{'...' if len(tables) > 5 else ''}"

    @property
    def is_llm_ready(self) -> bool:
        return self._llm is not None










