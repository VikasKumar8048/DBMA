# ============================================================
# DBMA - Database Management Agent
# core/persistence.py — PostgreSQL Chat Persistence Manager
# ============================================================

import json
import hashlib
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
import psycopg2
import psycopg2.extras
from loguru import logger

from config import postgres_config, mysql_config


class ChatMessage:
    """Represents a single chat message (human or assistant)."""

    def __init__(
            self,
            role: str,
            content: str,
            thread_id: str,
            message_id: Optional[str] = None,
            sql_query: Optional[str] = None,
            query_result: Optional[Dict] = None,
            created_at: Optional[datetime] = None,
            sequence_no: int = 0,
            metadata: Optional[Dict] = None,
    ):
        self.role = role
        self.content = content
        self.thread_id = thread_id
        self.message_id = message_id
        self.sql_query = sql_query
        self.query_result = query_result
        self.created_at = created_at or datetime.utcnow()
        self.sequence_no = sequence_no
        self.metadata = metadata or {}

    def to_langchain_format(self) -> Dict[str, str]:
        """Convert to LangChain message format for LLM context."""
        return {"role": self.role, "content": self.content}

    def __repr__(self):
        return f"<ChatMessage [{self.role}] seq={self.sequence_no}>"


class PersistenceManager:
    """
    Manages all long-term memory / chat persistence using PostgreSQL.

    Key Concepts:
    - thread_id: Unique ID per MySQL database (deterministic hash of host+user+dbname)
    - Each MySQL database gets its own isolated chat thread
    - Full chat history is loaded when switching to a database
    - Agent state checkpoints are stored for resumability
    """

    def __init__(self):
        self._conn: Optional[psycopg2.extensions.connection] = None
        self._initialized: bool = False

    # ── Connection ────────────────────────────────────────────

    def connect(self) -> bool:
        """Connect to PostgreSQL persistence database."""
        try:
            self._conn = psycopg2.connect(**postgres_config.get_connection_params())
            self._conn.autocommit = True
            psycopg2.extras.register_uuid()
            self._initialized = True
            logger.info(f"Connected to PostgreSQL persistence DB: {postgres_config.db}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            self._initialized = False
            return False

    def disconnect(self):
        """Close PostgreSQL connection."""
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("Disconnected from PostgreSQL")

    def is_connected(self) -> bool:
        return self._conn is not None and not self._conn.closed

    def ensure_connected(self):
        """Reconnect if needed."""
        if not self.is_connected():
            self.connect()

    def initialize_schema(self) -> bool:
        """
        Run the schema.sql file to create all required tables.
        Safe to run multiple times (uses IF NOT EXISTS).
        Also ensures the conversation_summary table exists for rolling memory.
        """
        try:
            schema_path = "database/schema.sql"
            with open(schema_path, "r") as f:
                schema_sql = f.read()
            self.ensure_connected()
            with self._conn.cursor() as cursor:
                cursor.execute(schema_sql)
            logger.info("PostgreSQL schema initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Schema initialization failed: {e}")
            # Even if schema.sql fails, try to create summary table directly
            try:
                with self._conn.cursor() as cursor:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS dbma_conversation_summary (
                            summary_id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                            thread_id                VARCHAR(255) NOT NULL UNIQUE
                                                         REFERENCES dbma_sessions(thread_id)
                                                         ON DELETE CASCADE,
                            summary_text             TEXT NOT NULL,
                            summarized_up_to_seq     INTEGER NOT NULL,
                            message_count_summarized INTEGER NOT NULL,
                            created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
                        )
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_summary_thread
                        ON dbma_conversation_summary(thread_id)
                    """)
                logger.info("Conversation summary table created as fallback")
            except Exception as e2:
                logger.error(f"Fallback summary table creation failed: {e2}")
            return False

    # ── Thread ID Generation ──────────────────────────────────

    @staticmethod
    def generate_thread_id(host: str, user: str, database: str) -> str:
        """
        Generate a deterministic, unique thread_id for a specific
        MySQL host + user + database combination.

        This ensures every MySQL database always gets the exact same
        thread_id regardless of when or how the app is started.
        """
        raw = f"{host}::{user}::{database}"
        hash_val = hashlib.sha256(raw.encode()).hexdigest()[:32]
        return f"thread_{hash_val}"

    # ── Session Management ────────────────────────────────────

    def get_or_create_session(
            self,
            database_name: str,
            host: Optional[str] = None,
            user: Optional[str] = None,
    ) -> str:
        """
        Get existing session thread_id for a database, or create a new one.
        Returns the thread_id.
        """
        self.ensure_connected()
        h = host or mysql_config.host
        u = user or mysql_config.user
        thread_id = self.generate_thread_id(h, u, database_name)

        try:
            with self._conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO dbma_sessions (thread_id, mysql_db_name, mysql_host, mysql_user)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (thread_id) DO UPDATE
                        SET last_active_at = NOW()
                    RETURNING thread_id
                    """,
                    (thread_id, database_name, h, u),
                )
                row = cursor.fetchone()
                logger.debug(f"Session upserted: thread_id={thread_id} db={database_name}")
                return thread_id
        except Exception as e:
            logger.error(f"Failed to upsert session: {e}")
            return thread_id  # return even on error — thread_id is deterministic

    def get_session_info(self, thread_id: str) -> Optional[Dict[str, Any]]:
        """Get session metadata for a thread_id."""
        self.ensure_connected()
        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT * FROM dbma_sessions WHERE thread_id = %s",
                    (thread_id,)
                )
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"get_session_info error: {e}")
            return None

    def list_sessions(self) -> List[Dict[str, Any]]:
        """List all existing chat sessions (one per MySQL database)."""
        self.ensure_connected()
        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT s.*, 
                           COUNT(m.message_id) as message_count,
                           MAX(m.created_at) as last_message_at
                    FROM dbma_sessions s
                    LEFT JOIN dbma_messages m ON m.thread_id = s.thread_id
                    GROUP BY s.session_id
                    ORDER BY s.last_active_at DESC
                    """
                )
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"list_sessions error: {e}")
            return []

    # ── Message Persistence ───────────────────────────────────

    def save_message(
            self,
            thread_id: str,
            role: str,
            content: str,
            sql_query: Optional[str] = None,
            query_result: Optional[Dict] = None,
            metadata: Optional[Dict] = None,
    ) -> Optional[str]:
        """
        Persist a chat message to PostgreSQL.
        Returns the message_id (UUID) of the saved message.
        """
        self.ensure_connected()
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO dbma_messages 
                        (thread_id, role, content, sql_query, query_result, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING message_id::text
                    """,
                    (
                        thread_id,
                        role,
                        content,
                        sql_query,
                        json.dumps(query_result) if query_result else None,
                        json.dumps(metadata or {}),
                    ),
                )
                row = cursor.fetchone()
                msg_id = row[0] if row else None
                logger.debug(f"Message saved: thread={thread_id} role={role} id={msg_id}")
                return msg_id
        except Exception as e:
            logger.error(f"save_message error: {e}")
            return None

    def load_chat_history(
            self,
            thread_id: str,
            limit: Optional[int] = None,
    ) -> List[ChatMessage]:
        """
        Load chat history for a database thread in chronological order.

        BUG FIX: The old query used ORDER BY ASC + LIMIT which returned the
        OLDEST N messages, so new messages were silently cut off.

        The correct approach:
        - If no limit → return ALL messages in ASC order
        - If limit N  → return the NEWEST N messages, but display them in
                         ASC order (oldest→newest) so the chat reads naturally.

        SQL pattern used when limit is set:
            SELECT * FROM (
                SELECT * FROM dbma_messages
                WHERE thread_id = X
                ORDER BY sequence_no DESC   ← grab newest first
                LIMIT N
            ) sub
            ORDER BY sequence_no ASC        ← re-sort for display order
        """
        self.ensure_connected()
        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                if limit:
                    # Fetch newest N, then re-sort chronologically for display
                    cursor.execute(
                        """
                        SELECT * FROM (
                            SELECT * FROM dbma_messages
                            WHERE thread_id = %s
                            ORDER BY sequence_no DESC
                            LIMIT %s
                        ) sub
                        ORDER BY sequence_no ASC
                        """,
                        (thread_id, limit),
                    )
                else:
                    # No limit — load every message in chronological order
                    cursor.execute(
                        """
                        SELECT * FROM dbma_messages
                        WHERE thread_id = %s
                        ORDER BY sequence_no ASC
                        """,
                        (thread_id,),
                    )

                rows = cursor.fetchall()
                messages = []
                for row in rows:
                    msg = ChatMessage(
                        role=row["role"],
                        content=row["content"],
                        thread_id=thread_id,
                        message_id=str(row["message_id"]),
                        sql_query=row["sql_query"],
                        query_result=row["query_result"],
                        created_at=row["created_at"],
                        sequence_no=row["sequence_no"],
                        metadata=row["metadata"] or {},
                    )
                    messages.append(msg)
                logger.info(f"Loaded {len(messages)} messages for thread: {thread_id}")
                return messages
        except Exception as e:
            logger.error(f"load_chat_history error: {e}")
            return []

    def get_recent_messages(
            self,
            thread_id: str,
            n: int = 20,
    ) -> List[ChatMessage]:
        """Get the N most recent messages for an LLM context window."""
        self.ensure_connected()
        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT * FROM (
                        SELECT * FROM dbma_messages
                        WHERE thread_id = %s
                        ORDER BY sequence_no DESC
                        LIMIT %s
                    ) sub
                    ORDER BY sequence_no ASC
                    """,
                    (thread_id, n),
                )
                rows = cursor.fetchall()
                return [
                    ChatMessage(
                        role=row["role"],
                        content=row["content"],
                        thread_id=thread_id,
                        message_id=str(row["message_id"]),
                        sql_query=row["sql_query"],
                        created_at=row["created_at"],
                        sequence_no=row["sequence_no"],
                    )
                    for row in rows
                ]
        except Exception as e:
            logger.error(f"get_recent_messages error: {e}")
            return []

    def get_message_count(self, thread_id: str) -> int:
        """Get total number of messages for a thread."""
        self.ensure_connected()
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM dbma_messages WHERE thread_id = %s",
                    (thread_id,),
                )
                return cursor.fetchone()[0]
        except Exception:
            return 0

    def format_history_for_llm(
            self,
            messages: List[ChatMessage],
            include_sql: bool = True,
    ) -> List[Dict[str, str]]:
        """
        Format chat history into LLM-compatible message dicts.
        Maps "human" → "user" so Ollama/OpenAI prompt builder sees the messages.
        Without this fix, history is silently dropped and LLM has no memory.
        """
        # Role mapping: LangChain/DB uses "human", Ollama prompt expects "user"
        ROLE_MAP = {"human": "user", "user": "user", "assistant": "assistant"}
        formatted = []
        for msg in messages:
            role = ROLE_MAP.get(msg.role, "user")  # default to "user" if unknown
            content = msg.content or ""
            # Append SQL to assistant messages so LLM knows what was executed
            if include_sql and msg.sql_query and role == "assistant":
                content = f"{content}\n\nGenerated SQL:\n```sql\n{msg.sql_query}\n```"
            if content.strip():  # skip empty messages
                formatted.append({"role": role, "content": content})
        return formatted

    # ── Schema Cache ──────────────────────────────────────────

    def save_schema_cache(
            self,
            thread_id: str,
            database_name: str,
            schema_json: Dict[str, Any],
    ) -> bool:
        """Cache the database schema for fast LLM context loading."""
        self.ensure_connected()
        try:
            table_count = len(schema_json.get("tables", {}))
            with self._conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO dbma_schema_cache 
                        (thread_id, mysql_db_name, schema_json, table_count, refreshed_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (thread_id) DO UPDATE
                        SET schema_json = EXCLUDED.schema_json,
                            table_count = EXCLUDED.table_count,
                            refreshed_at = NOW()
                    """,
                    (thread_id, database_name, json.dumps(schema_json), table_count),
                )
            logger.info(f"Schema cached for thread={thread_id} tables={table_count}")
            return True
        except Exception as e:
            logger.error(f"save_schema_cache error: {e}")
            return False

    def load_schema_cache(self, thread_id: str) -> Optional[Dict[str, Any]]:
        """Load cached schema for a thread."""
        self.ensure_connected()
        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT * FROM dbma_schema_cache WHERE thread_id = %s",
                    (thread_id,)
                )
                row = cursor.fetchone()
                if row:
                    return dict(row)
                return None
        except Exception as e:
            logger.error(f"load_schema_cache error: {e}")
            return None

    # ── Query History ─────────────────────────────────────────

    def save_query_history(
            self,
            thread_id: str,
            sql_query: str,
            success: bool,
            execution_ms: int = 0,
            rows_affected: int = 0,
            error_message: Optional[str] = None,
            message_id: Optional[str] = None,
    ) -> bool:
        """Save executed query to audit history."""
        self.ensure_connected()
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO dbma_query_history
                        (thread_id, message_id, sql_query, execution_ms, 
                         rows_affected, success, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        thread_id, message_id, sql_query,
                        execution_ms, rows_affected, success, error_message
                    ),
                )
            return True
        except Exception as e:
            logger.error(f"save_query_history error: {e}")
            return False

    def get_query_history(
            self,
            thread_id: str,
            limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get recent query execution history for a database."""
        self.ensure_connected()
        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT sql_query, success, execution_ms, rows_affected,
                           error_message, executed_at
                    FROM dbma_query_history
                    WHERE thread_id = %s
                    ORDER BY executed_at DESC
                    LIMIT %s
                    """,
                    (thread_id, limit),
                )
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"get_query_history error: {e}")
            return []

    # ── Checkpoint (Agent State) ──────────────────────────────

    def save_checkpoint(
            self,
            thread_id: str,
            state_data: Dict[str, Any],
            checkpoint_ns: str = "",
            checkpoint_key: str = "default",
    ) -> bool:
        """Save LangGraph agent state checkpoint."""
        self.ensure_connected()
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO dbma_checkpoints
                        (thread_id, checkpoint_ns, checkpoint_key, state_data)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (thread_id, checkpoint_ns, checkpoint_key) DO UPDATE
                        SET state_data = EXCLUDED.state_data,
                            created_at = NOW()
                    """,
                    (thread_id, checkpoint_ns, checkpoint_key, json.dumps(state_data)),
                )
            return True
        except Exception as e:
            logger.error(f"save_checkpoint error: {e}")
            return False

    def load_checkpoint(
            self,
            thread_id: str,
            checkpoint_ns: str = "",
            checkpoint_key: str = "default",
    ) -> Optional[Dict[str, Any]]:
        """Load the latest agent state checkpoint."""
        self.ensure_connected()
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT state_data FROM dbma_checkpoints
                    WHERE thread_id = %s 
                      AND checkpoint_ns = %s 
                      AND checkpoint_key = %s
                    """,
                    (thread_id, checkpoint_ns, checkpoint_key),
                )
                row = cursor.fetchone()
                if row:
                    return row[0]  # psycopg2 auto-parses JSONB
                return None
        except Exception as e:
            logger.error(f"load_checkpoint error: {e}")
            return None

    def clear_thread(self, thread_id: str) -> bool:
        """Delete all messages and state for a thread (fresh start)."""
        self.ensure_connected()
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM dbma_messages WHERE thread_id = %s", (thread_id,)
                )
                cursor.execute(
                    "DELETE FROM dbma_checkpoints WHERE thread_id = %s", (thread_id,)
                )
                cursor.execute(
                    "DELETE FROM dbma_schema_cache WHERE thread_id = %s", (thread_id,)
                )
            logger.info(f"Cleared thread: {thread_id}")
            return True
        except Exception as e:
            logger.error(f"clear_thread error: {e}")
            return False

    # ── Conversation Summary (ChatGPT-style Rolling Memory) ───────

    def save_conversation_summary(
            self,
            thread_id: str,
            summary_text: str,
            summarized_up_to_seq: int,
            message_count_summarized: int,
    ) -> bool:
        """
        Save or update the rolling conversation summary for a thread.
        This is the exact technique ChatGPT uses to give LLMs access
        to full conversation history without exceeding token limits.
        """
        self.ensure_connected()
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO dbma_conversation_summary
                        (thread_id, summary_text, summarized_up_to_seq,
                         message_count_summarized, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (thread_id) DO UPDATE
                        SET summary_text              = EXCLUDED.summary_text,
                            summarized_up_to_seq      = EXCLUDED.summarized_up_to_seq,
                            message_count_summarized  = EXCLUDED.message_count_summarized,
                            updated_at                = NOW()
                    """,
                    (thread_id, summary_text, summarized_up_to_seq, message_count_summarized),
                )
            logger.info(
                f"Summary saved: thread={thread_id} "
                f"covers {message_count_summarized} msgs up to seq={summarized_up_to_seq}"
            )
            return True
        except Exception as e:
            logger.error(f"save_conversation_summary error: {e}")
            return False

    def load_conversation_summary(
            self,
            thread_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Load the existing conversation summary for a thread.
        Returns dict with keys: summary_text, summarized_up_to_seq,
        message_count_summarized — or None if no summary exists yet.
        """
        self.ensure_connected()
        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT summary_text, summarized_up_to_seq,
                           message_count_summarized, updated_at
                    FROM dbma_conversation_summary
                    WHERE thread_id = %s
                    """,
                    (thread_id,),
                )
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"load_conversation_summary error: {e}")
            return None

    def get_messages_after_seq(
            self,
            thread_id: str,
            after_seq: int,
    ) -> List[ChatMessage]:
        """
        Load all messages with sequence_no > after_seq.
        Used to get messages that are NOT yet included in the summary.
        """
        self.ensure_connected()
        try:
            with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT * FROM dbma_messages
                    WHERE thread_id = %s
                      AND sequence_no > %s
                    ORDER BY sequence_no ASC
                    """,
                    (thread_id, after_seq),
                )
                rows = cursor.fetchall()
                return [
                    ChatMessage(
                        role=row["role"],
                        content=row["content"],
                        thread_id=thread_id,
                        message_id=str(row["message_id"]),
                        sql_query=row["sql_query"],
                        created_at=row["created_at"],
                        sequence_no=row["sequence_no"],
                    )
                    for row in rows
                ]
        except Exception as e:
            logger.error(f"get_messages_after_seq error: {e}")
            return []




























