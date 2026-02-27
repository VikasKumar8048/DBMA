#!/usr/bin/env python3
# ============================================================
# DBMA - Database Management Agent
# main.py — Application Entry Point
# ============================================================
#
# Usage:
#   python main.py              → Launch full TUI (split-panel)
#   python main.py --simple     → Launch simple CLI (no TUI)
#   python main.py --setup      → Run database setup only
#   python main.py --version    → Show version info
#
# Prerequisites:
#   1. MySQL server running and accessible
#   2. PostgreSQL server running (for persistence)
#   3. Ollama running with your model:
#      → ollama serve
#      → ollama pull llama3.1:8b   (or your chosen model)
#   4. .env file configured (copy from .env.example)
# ============================================================

import sys
import os
import click
from loguru import logger

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

from utils.logger import setup_logger
from config import app_config, mysql_config, postgres_config, ollama_config


@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx):
    """DBMA — Database Management Agent CLI"""
    if ctx.invoked_subcommand is None:
        launch_tui()


@cli.command()
def tui():
    """Launch the full split-panel TUI interface (default)."""
    launch_tui()


@cli.command()
def simple():
    """Launch the simple single-panel CLI interface."""
    launch_simple_cli()


@cli.command()
def setup():
    """Initialize the PostgreSQL persistence database schema."""
    run_setup()


@cli.command()
def version():
    """Display DBMA version information."""
    show_version()


@cli.command()
@click.argument("database")
def inspect(database: str):
    """Inspect a MySQL database schema and print it."""
    run_inspect(database)


# ── Launch Functions ──────────────────────────────────────────

def launch_tui():
    """Start the full Textual TUI application."""
    setup_logger(app_config.log_file, app_config.log_level)
    logger.info(f"Starting DBMA v{app_config.version} (TUI mode)")

    # Pre-flight checks
    if not _check_environment():
        sys.exit(1)

    from ui.tui import DBMAApp
    app = DBMAApp()
    app.run()


def launch_simple_cli():
    """
    Simple CLI mode — no Textual TUI, just a readline-based shell.
    Useful for environments where TUI doesn't work or for debugging.
    """
    setup_logger(app_config.log_file, app_config.log_level)

    from simple_cli import SimpleCLI
    cli_app = SimpleCLI()
    cli_app.run()


def run_setup():
    """Initialize PostgreSQL schema."""
    print(f"DBMA Setup — PostgreSQL Schema Initialization")
    print(f"Host: {postgres_config.host}:{postgres_config.port}")
    print(f"Database: {postgres_config.db}")
    print()

    from core.persistence import PersistenceManager
    pm = PersistenceManager()

    if not pm.connect():
        print("❌ Failed to connect to PostgreSQL!")
        print("   Check your POSTGRES_* settings in .env")
        sys.exit(1)

    if pm.initialize_schema():
        print("✅ Schema initialized successfully!")
        print("   Tables created:")
        print("   - dbma_sessions      (thread management)")
        print("   - dbma_messages      (chat history)")
        print("   - dbma_checkpoints   (agent state)")
        print("   - dbma_schema_cache  (MySQL schema cache)")
        print("   - dbma_query_history (SQL audit log)")
    else:
        print("❌ Schema initialization failed!")
        sys.exit(1)

    pm.disconnect()


def show_version():
    """Display version and configuration info."""
    print(f"""
╔══════════════════════════════════════════════════════╗
║          DBMA — Database Management Agent            ║
╠══════════════════════════════════════════════════════╣
║  Version    : {app_config.version:<38}║
║                                                      ║
║  MySQL      : {mysql_config.host}:{mysql_config.port} (user: {mysql_config.user}){' ' * max(0, 20 - len(mysql_config.host) - len(str(mysql_config.port)) - len(mysql_config.user))}║
║  PostgreSQL : {postgres_config.host}:{postgres_config.port}/{postgres_config.db:<22}║
║  Ollama     : {ollama_config.base_url:<38}║
║  LLM Model  : {ollama_config.model:<38}║
╚══════════════════════════════════════════════════════╝
""")


def run_inspect(database: str):
    """Inspect and print a database schema."""
    # setup_logger(log_level="WARNING")
    setup_logger(app_config.log_file, "WARNING")

    from core.mysql_manager import MySQLManager
    mysql = MySQLManager()

    if not mysql.connect(database):
        print(f"❌ Failed to connect to MySQL database: {database}")
        sys.exit(1)

    print(f"\nInspecting database: {database}\n")
    schema = mysql.get_full_database_schema(database)
    print(mysql.format_schema_for_llm(schema))
    mysql.disconnect()


# ── Pre-flight Checks ─────────────────────────────────────────

def _check_environment() -> bool:
    """Run environment checks before launching."""
    issues = []

    # Check .env exists
    if not os.path.exists(".env"):
        if os.path.exists(".env.example"):
            print("⚠️  No .env file found! Copying .env.example → .env")
            import shutil
            shutil.copy(".env.example", ".env")
            print("   Please edit .env with your actual credentials and restart.")
        else:
            issues.append(".env file not found")

    # Check Ollama is reachable
    try:
        import httpx
        resp = httpx.get(f"{ollama_config.base_url}/api/tags", timeout=5)
        if resp.status_code != 200:
            issues.append(f"Ollama not responding at {ollama_config.base_url}")
        else:
            models = resp.json().get("models", [])
            model_names = [m["name"] for m in models]
            if ollama_config.model not in model_names and not any(
                ollama_config.model.split(":")[0] in m for m in model_names
            ):
                print(
                    f"⚠️  Model '{ollama_config.model}' not found in Ollama.\n"
                    f"   Available: {', '.join(model_names[:5]) if model_names else 'none'}\n"
                    f"   Run: ollama pull {ollama_config.model}"
                )
    except Exception as e:
        issues.append(f"Ollama unreachable ({ollama_config.base_url}): {e}")
        print(
            f"⚠️  Ollama not reachable at {ollama_config.base_url}\n"
            f"   Start it with: ollama serve\n"
            f"   Then pull your model: ollama pull {ollama_config.model}\n"
            f"   DBMA will start but LLM features will be unavailable."
        )

    if issues:
        for issue in issues:
            print(f"❌ {issue}")
        return False

    return True


# ── Entry Point ───────────────────────────────────────────────

if __name__ == "__main__":
    cli()
