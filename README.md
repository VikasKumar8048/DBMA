# DBMA — Database Management Agent
### *Database Management Agent: A local-first agentic AI Database Operating System with persistent per-database memory, self-healing SQL execution, and natural language query generation*

<div align="center">

![Python](https://img.shields.io/badge/Python-3.10+-blue?style=for-the-badge&logo=python)
![MySQL](https://img.shields.io/badge/MySQL-8.0+-orange?style=for-the-badge&logo=mysql)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-blue?style=for-the-badge&logo=postgresql)
![Ollama](https://img.shields.io/badge/Ollama-Local_LLM-black?style=for-the-badge)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)

**Author: Vikas Kumar**
**Created: February 2026**

*No cloud. No API keys. No subscription. Runs entirely on your machine.*

</div>

---

## What is DBMA?

DBMA (Database Management Agent) is a **local-first agentic AI system** that acts as an intelligent operating layer on top of MySQL. Instead of writing complex SQL queries manually, you describe what you want in plain English — DBMA generates, executes, self-heals, and remembers everything.

It is designed for **data engineers**, **database administrators**, and **students** who work with multiple databases simultaneously and need an AI assistant that understands the full context of every database they manage — without sending any data to the cloud.

```
┌─────────────────────────┬─────────────────────────┐
│   📊 Query Output       │   💬 DBMA Chat          │
│                         │                         │
│  mysql > SHOW TABLES;   │  You: show all tables   │
│  +-----------------+    │                         │
│  | Tables_in_db    |    │  DBMA: I'll show all    │
│  | student         |    │  tables in hospital_db  │
│  | teacher         |    │                         │
│  +-----------------+    │  Generated SQL:         │
│  3 rows (0.001 sec)     │  SHOW TABLES;           │
├─────────────────────────┼─────────────────────────┤
│   SQL Query Input       │   Plain English Input   │
│                         │                         │
│  SQL here (auto-filled) │  Ask anything...        │
└─────────────────────────┴─────────────────────────┘
```

---

## Key Features

### 🧠 Persistent Per-Database Memory
Every database gets its own isolated conversation thread stored in PostgreSQL. Switch between 10 databases — DBMA remembers every query, every table, every decision made in each one. Conversations persist across sessions indefinitely.

### 🔄 Self-Healing SQL Execution
When a query fails, DBMA automatically reads the MySQL error message, corrects the SQL, and retries — up to 3 times. No manual debugging required.

### 📝 Rolling Conversation Summary
Inspired by how ChatGPT manages long conversations, DBMA compresses old messages into a dense memory block using LLM summarization. The AI effectively has access to your entire conversation history regardless of length — 100 messages, 500 messages, 1000 messages.

### 🏠 Fully Local — Zero Cloud Dependency
Runs on Ollama with local models (qwen3:8b, llama3, etc.). Your database credentials, schema, and query results never leave your machine.

### 🔍 Intent Classification Engine
Automatically classifies every input into 8+ intent types (SELECT, INSERT, UPDATE, DELETE, SHOW_TABLES, EXPLAIN_SCHEMA, GENERAL_QUESTION, etc.) and routes accordingly.

### 📊 LangSmith Observability
Full tracing of every LLM call — latency, token usage, error rates — via LangSmith integration.

### 🖥️ Terminal-Native TUI
Built with Textual — runs directly in any terminal. Works over SSH on remote servers. No browser required.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         DBMA ARCHITECTURE                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   User Input (Natural Language)                                 │
│          │                                                      │
│          ▼                                                      │
│   ┌─────────────────┐                                           │
│   │ Intent Classifier│  ← Classifies into 8+ intent types       │
│   └────────┬────────┘                                           │
│            │                                                    │
│            ▼                                                    │
│   ┌─────────────────────────────────┐                           │
│   │     Rolling Context Builder     │                           │
│   │  [Summary of old messages]      │  ← PostgreSQL query only  │
│   │  + [Last 40 full messages]      │  ← No LLM call here       │
│   └────────┬────────────────────────┘                           │
│            │                                                    │
│            ▼                                                    │
│   ┌─────────────────┐                                           │
│   │   Main LLM      │  ← Single Ollama call (qwen3:8b)          │
│   │   (Ollama)      │                                           │
│   └────────┬────────┘                                           │
│            │                                                    │
│            ▼                                                    │
│   ┌─────────────────┐     ┌──────────────────┐                  │
│   │  SQL Extractor  │────▶│ Self-Healing Loop │                 │ 
│   └─────────────────┘     │ (auto-retry x3)  │                  │
│                            └────────┬─────────┘                 │
│                                     │                           │
│                                     ▼                           │
│                            ┌──────────────────┐                 │
│                            │   MySQL Execute  │                 │
│                            └────────┬─────────┘                 │
│                                     │                           │
│                                     ▼                           │
│                            ┌──────────────────┐                 │
│                            │ Result → TUI     │                 │
│                            │ Save to          │                 │
│                            │ PostgreSQL       │                 │
│                            └──────────────────┘                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Comparison With Existing Tools

| Feature | DBMA | DataGrip AI | Vanna.ai | Text2SQL | DBeaver AI |
|---------|------|-------------|----------|----------|------------|
| Local / No Cloud | ✅ | ❌ | ❌ | ❌ | ❌ |
| Per-DB Memory | ✅ | ❌ | ❌ | ❌ | ❌ |
| Self-Healing SQL | ✅ | ❌ | ❌ | ❌ | ❌ |
| Rolling Summary | ✅ | ❌ | ❌ | ❌ | ❌ |
| Terminal Native | ✅ | ❌ | ❌ | ❌ | ❌ |
| Free | ✅ | ❌ | ❌ | ❌ | ❌ |
| Works Over SSH | ✅ | ❌ | ❌ | ❌ | ❌ |

---

## Installation

### Prerequisites
- Python 3.10+
- MySQL 8.0+
- PostgreSQL 15+
- [Ollama](https://ollama.ai) installed and running

### 1. Clone the repository
```bash
git clone https://github.com/VikasKumar8048/DBMA.git
cd DBMA
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Pull the LLM model
```bash
ollama pull qwen3:8b
```

### 4. Configure environment
```bash
cp .env.example .env
# Edit .env with your MySQL and PostgreSQL credentials
```

### 5. Initialize PostgreSQL schema
```bash
psql -U your_user -d your_dbma_db -f database/schema.sql
```

### 6. Run DBMA
```bash
python main.py
```

---

## Configuration

Edit `.env` file:

```env
# MySQL Connection
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=your_password

# PostgreSQL Persistence
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=dbma_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password

# Ollama LLM
OLLAMA_MODEL=qwen3:8b
OLLAMA_BASE_URL=http://localhost:11434

# Optional: LangSmith Observability
LANGCHAIN_API_KEY=your_key
LANGCHAIN_TRACING_V2=true
```

---

## Usage

### Natural Language Queries
```
You: show me all tables in this database
DBMA: → Generates and executes SHOW TABLES automatically

You: find all patients older than 60
DBMA: → SELECT * FROM patients WHERE age > 60;

You: create a table for storing employee records with id, name, salary
DBMA: → Generates complete CREATE TABLE statement

You: what was the last query I ran on this database?
DBMA: → Retrieves from conversation memory
```

### Commands
```
/clear     — Clear screen (history preserved in memory)
/clearall  — Permanently delete all history for current database
/tables    — Show all tables in current database
/schema    — Show full database schema
/databases — List all available databases
/refresh   — Force refresh schema cache
/help      — Show all commands
```

### Switching Databases
```
You: use hospital_database
DBMA: Switched to hospital_database. Loaded 47 previous messages.
```

---

## Project Structure

```
DBMA/
├── main.py                 ← Entry point (click CLI)
├── simple_cli.py           ← Fallback non-TUI CLI
├── config.py               ← Centralized configuration
├── requirements.txt        ← Python dependencies
├── .env.example            ← Environment template
│
├── core/                   ← Business Logic
│   ├── __init__.py
│   ├── mysql_manager.py    ← MySQL connection, introspection, query execution
│   ├── persistence.py      ← PostgreSQL chat history manager (thread-per-db)
│   ├── agent.py            ← ⚠️ AI Agent (Ollama LLM integration here)
│   └── query_executor.py  ← SQL execution + MySQL-CLI-style output formatter
│
├── ui/                     ← User Interface
│   ├── __init__.py
│   ├── tui.py              ← Textual split-panel TUI application
│   └── dbma.tcss           ← Textual CSS styling
│
├── utils/                  ← Utilities
│   ├── __init__.py
│   ├── logger.py           ← Loguru logging setup
│   └── helpers.py          ← General utility functions
│
└── database/               ← Database files
    ├── schema.sql          ← PostgreSQL persistence schema (run once)
    └── migrations/         ← Future migration files
        └── 001_initial.sql
```
```

---

## Novel Contributions

This project introduces the following original system designs:

1. **Per-Database Conversation Isolation** — Each MySQL database maintains its own independent AI conversation thread with full history, stored in PostgreSQL with deterministic thread IDs.

2. **Self-Healing SQL Execution Loop** — Failed queries are automatically diagnosed using MySQL error messages and corrected by the LLM, with up to 3 retry attempts before surfacing the error.

3. **Rolling Conversational Memory for Database Contexts** — Adaptation of large language model conversation compression techniques to database management workflows, enabling unlimited history retention without token overflow.

4. **Local-First Agentic Database OS** — Complete agentic pipeline (classify → generate → execute → heal → remember) running entirely on local infrastructure with no external API dependencies.

---

## Author

**Vikas Kumar**
GitHub: [@VikasKumar8048](https://github.com/VikasKumar8048)

*This project was conceptualized, designed, and built entirely by Vikas Kumar.*
*First committed: February 28, 2026*

---

## License

MIT License — Copyright (c) 2026 Vikas Kumar

See [LICENSE](LICENSE) for full text.

---

## Citation

If you use DBMA in your research or project, please cite:

```bibtex
@software{kumar2026dbma,
  author    = {Vikas Kumar},
  title     = {DBMA: A Local-First Agentic AI Database Operating System},
  year      = {2026},
  month     = {February},
  url       = {https://github.com/VikasKumar8048/DBMA},
  note      = {Database Management Agent with persistent per-database memory 
               and self-healing SQL execution}
}
```