-- ============================================================
-- DBMA - Database Management Agent
-- PostgreSQL Persistence Schema
-- Run this file once to initialize the persistence database
-- ============================================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ── Sessions Table ────────────────────────────────────────────
-- Each row = one unique database context (one chat thread per MySQL database)
CREATE TABLE IF NOT EXISTS dbma_sessions (
    session_id      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    thread_id       VARCHAR(255) UNIQUE NOT NULL,     -- unique per mysql_db_name
    mysql_db_name   VARCHAR(255) NOT NULL,             -- which MySQL database this chat is for
    mysql_host      VARCHAR(255) NOT NULL DEFAULT 'localhost',
    mysql_user      VARCHAR(255) NOT NULL DEFAULT 'root',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_active_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata        JSONB DEFAULT '{}'::JSONB
);

CREATE INDEX IF NOT EXISTS idx_sessions_thread_id ON dbma_sessions(thread_id);
CREATE INDEX IF NOT EXISTS idx_sessions_mysql_db ON dbma_sessions(mysql_db_name);

-- ── Chat Messages Table ───────────────────────────────────────
-- Stores every human and AI message per thread
CREATE TABLE IF NOT EXISTS dbma_messages (
    message_id      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    thread_id       VARCHAR(255) NOT NULL REFERENCES dbma_sessions(thread_id) ON DELETE CASCADE,
    sequence_no     SERIAL,
    role            VARCHAR(20) NOT NULL CHECK (role IN ('human', 'assistant', 'system', 'tool')),
    content         TEXT NOT NULL,
    sql_query       TEXT,                              -- extracted SQL if any
    query_result    JSONB,                             -- result of SQL execution if any
    tokens_used     INTEGER DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata        JSONB DEFAULT '{}'::JSONB
);

CREATE INDEX IF NOT EXISTS idx_messages_thread_id ON dbma_messages(thread_id);
CREATE INDEX IF NOT EXISTS idx_messages_created_at ON dbma_messages(created_at);
CREATE INDEX IF NOT EXISTS idx_messages_sequence ON dbma_messages(thread_id, sequence_no);

-- ── Agent State Checkpoints ───────────────────────────────────
-- Stores LangGraph/agent state snapshots for resumability
CREATE TABLE IF NOT EXISTS dbma_checkpoints (
    checkpoint_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    thread_id       VARCHAR(255) NOT NULL,
    checkpoint_ns   VARCHAR(255) NOT NULL DEFAULT '',
    checkpoint_key  VARCHAR(255) NOT NULL DEFAULT 'default',
    state_data      JSONB NOT NULL,                    -- serialized agent state
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(thread_id, checkpoint_ns, checkpoint_key)
);

CREATE INDEX IF NOT EXISTS idx_checkpoints_thread ON dbma_checkpoints(thread_id);

-- ── Database Schema Cache ─────────────────────────────────────
-- Caches MySQL database schemas for quick agent context loading
CREATE TABLE IF NOT EXISTS dbma_schema_cache (
    cache_id        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    thread_id       VARCHAR(255) NOT NULL UNIQUE REFERENCES dbma_sessions(thread_id) ON DELETE CASCADE,
    mysql_db_name   VARCHAR(255) NOT NULL,
    schema_json     JSONB NOT NULL,                    -- full schema: tables, columns, keys, etc.
    table_count     INTEGER DEFAULT 0,
    refreshed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_schema_cache_thread ON dbma_schema_cache(thread_id);

-- ── Query History ─────────────────────────────────────────────
-- Separately tracks every executed SQL for audit/replay
CREATE TABLE IF NOT EXISTS dbma_query_history (
    query_id        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    thread_id       VARCHAR(255) NOT NULL,
    message_id      UUID REFERENCES dbma_messages(message_id) ON DELETE SET NULL,
    sql_query       TEXT NOT NULL,
    execution_ms    INTEGER,
    rows_affected   INTEGER,
    success         BOOLEAN DEFAULT TRUE,
    error_message   TEXT,
    executed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_query_history_thread ON dbma_query_history(thread_id);
CREATE INDEX IF NOT EXISTS idx_query_history_time ON dbma_query_history(executed_at);

-- ── Helper Functions ──────────────────────────────────────────

-- Generate a deterministic thread_id from mysql_host + mysql_user + db_name
CREATE OR REPLACE FUNCTION generate_thread_id(
    p_host VARCHAR,
    p_user VARCHAR,
    p_db   VARCHAR
) RETURNS VARCHAR AS $$
BEGIN
    RETURN 'thread_' || encode(
        digest(p_host || '::' || p_user || '::' || p_db, 'sha256'),
        'hex'
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Upsert session and return thread_id
CREATE OR REPLACE FUNCTION upsert_session(
    p_host VARCHAR,
    p_user VARCHAR,
    p_db   VARCHAR
) RETURNS VARCHAR AS $$
DECLARE
    v_thread_id VARCHAR;
BEGIN
    v_thread_id := generate_thread_id(p_host, p_user, p_db);

    INSERT INTO dbma_sessions (thread_id, mysql_db_name, mysql_host, mysql_user)
    VALUES (v_thread_id, p_db, p_host, p_user)
    ON CONFLICT (thread_id) DO UPDATE
        SET last_active_at = NOW();

    RETURN v_thread_id;
END;
$$ LANGUAGE plpgsql;

-- ── Conversation Summary Cache ────────────────────────────────
-- Stores compressed summaries of old messages so the LLM can
-- access the full conversation context without token overflow.
-- Exact same approach ChatGPT uses internally.
CREATE TABLE IF NOT EXISTS dbma_conversation_summary (
    summary_id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    thread_id           VARCHAR(255) NOT NULL UNIQUE REFERENCES dbma_sessions(thread_id) ON DELETE CASCADE,
    summary_text        TEXT NOT NULL,          -- compressed summary of old messages
    summarized_up_to_seq INTEGER NOT NULL,      -- sequence_no of last message included in summary
    message_count_summarized INTEGER NOT NULL,  -- how many messages are summarized
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_summary_thread ON dbma_conversation_summary(thread_id);






