# ============================================================
# DBMA - Database Management Agent
# simple_cli.py — Fallback Simple CLI (no Textual TUI)
# ============================================================
#
# This provides a readline-based CLI when Textual TUI is not
# available or not desired. Same AI agent, simpler interface.
# ============================================================

import sys
import re
import os
from typing import Optional
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.styles import Style
from prompt_toolkit.formatted_text import HTML
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.table import Table
from rich import box

from core.mysql_manager import MySQLManager
from core.persistence import PersistenceManager
from core.agent import DBMAAgent, AgentIntent
from core.query_executor import QueryExecutor
from utils.helpers import is_safe_query, get_timestamp
from config import mysql_config, postgres_config, ollama_config, app_config


PROMPT_STYLE = Style.from_dict({
    "prompt": "ansigreen bold",
    "db": "ansicyan bold",
})


class SimpleCLI:
    """
    Simple single-window CLI interface for DBMA.
    Two modes of input:
    1. SQL mode: Direct SQL is executed immediately
    2. Chat mode: Natural language processed by AI agent
    Current mode is indicated in the prompt.
    """

    def __init__(self):
        self.console = Console()
        self.mysql = MySQLManager()
        self.persistence = PersistenceManager()
        self.agent: Optional[DBMAAgent] = None
        self.executor: Optional[QueryExecutor] = None

        self._current_db: Optional[str] = None
        self._current_thread_id: Optional[str] = None
        self._mode: str = "chat"  # "chat" or "sql"
        self._running: bool = True

        # Prompt toolkit session with history
        history_file = os.path.expanduser("~/.dbma_history")
        self.session = PromptSession(
            history=FileHistory(history_file),
            auto_suggest=AutoSuggestFromHistory(),
        )

    def run(self):
        """Main loop."""
        self._print_banner()
        self._initialize()

        while self._running:
            try:
                user_input = self._get_input()
                if user_input is None:
                    break
                user_input = user_input.strip()
                if not user_input:
                    continue
                self._handle_input(user_input)
            except KeyboardInterrupt:
                self.console.print("\n[dim]Use /exit to quit[/dim]")
            except EOFError:
                break

        self._shutdown()

    def _initialize(self):
        """Connect to databases and initialize agent."""
        self.console.print(f"[dim]Connecting to MySQL at {mysql_config.host}:{mysql_config.port}...[/dim]")

        if not self.mysql.connect():
            self.console.print("[red]Failed to connect to MySQL! Check your .env[/red]")
            sys.exit(1)
        self.console.print("[green]✓ MySQL connected[/green]")

        self.console.print("[dim]Connecting to PostgreSQL persistence...[/dim]")
        if not self.persistence.connect():
            self.console.print("[yellow]⚠ PostgreSQL not connected — history won't persist[/yellow]")
        else:
            self.persistence.initialize_schema()
            self.console.print("[green]✓ PostgreSQL persistence connected[/green]")

        # ⚠️  LLM INTEGRATION REQUIRED
        self.agent = DBMAAgent(self.mysql, self.persistence)
        self.executor = QueryExecutor(self.mysql, self.persistence, self.console)

        self.console.print(f"[green]✓ DBMA Agent ready[/green] [dim](model: {ollama_config.model})[/dim]")
        self.console.print()
        self.console.print("[dim]Type [bold]/help[/bold] for commands, or start talking to the agent.[/dim]")
        self.console.print()

        # List available databases
        dbs = self.mysql.list_databases()
        if dbs:
            self.console.print(f"[dim]Databases: {', '.join(dbs)}[/dim]")
            self.console.print("[dim]Say 'use <database_name>' to start working.[/dim]\n")

    def _get_input(self) -> Optional[str]:
        """Get input from user with context-aware prompt."""
        db_part = f"[{self._current_db}]" if self._current_db else ""
        mode_indicator = "💬" if self._mode == "chat" else "SQL"

        try:
            result = self.session.prompt(
                HTML(
                    f"<ansigreen><b>dbma{db_part}</b></ansigreen>"
                    f"<ansiyellow> {mode_indicator} </ansiyellow>"
                    f"<ansicyan>▶ </ansicyan>"
                )
            )
            return result
        except KeyboardInterrupt:
            return ""
        except EOFError:
            return None

    def _handle_input(self, user_input: str):
        """Route input to appropriate handler."""
        stripped = user_input.strip()

        # Slash commands
        if stripped.startswith("/"):
            self._handle_command(stripped)
            return

        # Direct SQL in SQL mode
        if self._mode == "sql" or self._looks_like_sql(stripped):
            self._execute_sql(stripped)
            return

        # Natural language → Agent
        self._handle_chat(stripped)

    def _handle_chat(self, user_input: str):
        """Send natural language to AI agent."""
        if not self.agent:
            self.console.print("[red]Agent not initialized[/red]")
            return

        if not self._current_db and not re.search(r"\buse\b|\bswitch\b|\bshow\s+database", user_input, re.I):
            self.console.print("[yellow]⚠ No database selected. Say 'use <database_name>' first.[/yellow]")

        self.console.print(f"[dim]Thinking...[/dim]")

        try:
            # ⚠️  LLM INTEGRATION REQUIRED
            response = self.agent.chat(user_input)
        except Exception as e:
            self.console.print(f"[red]Agent error: {e}[/red]")
            return

        # Print agent's natural language response
        self.console.print()
        self.console.print(Panel(
            response.natural_text,
            title="[bold green]DBMA[/bold green]",
            border_style="green",
        ))

        # Handle database switch
        if response.intent == AgentIntent.SWITCH_DATABASE and response.metadata.get("target_database"):
            self._switch_database(response.metadata["target_database"])
            return

        # Print and optionally execute SQL
        if response.has_sql():
            self.console.print()
            self.console.print(f"[dim]Generated SQL:[/dim]")
            self.console.print(f"[bold #79c0ff]{response.sql_query}[/bold #79c0ff]")

            # Auto-execute safe queries
            if response.auto_execute and not response.requires_confirmation:
                self.console.print()
                self._execute_sql(response.sql_query)
            else:
                # Ask for confirmation
                if response.requires_confirmation:
                    self.console.print(f"\n[yellow]⚠ This is a destructive query![/yellow]")

                try:
                    confirm = self.session.prompt(
                        HTML("<ansiyellow>Execute this query? (y/n/e to edit): </ansiyellow>")
                    ).strip().lower()
                except (KeyboardInterrupt, EOFError):
                    confirm = "n"

                if confirm == "y":
                    self._execute_sql(response.sql_query)
                elif confirm == "e":
                    try:
                        edited = self.session.prompt(
                            HTML("<ansicyan>Edit SQL: </ansicyan>"),
                            default=response.sql_query,
                        ).strip()
                        if edited:
                            self._execute_sql(edited)
                    except (KeyboardInterrupt, EOFError):
                        self.console.print("[dim]Cancelled[/dim]")
                else:
                    self.console.print("[dim]Query not executed.[/dim]")

        self.console.print()

    def _execute_sql(self, sql: str):
        """Execute SQL and display MySQL-CLI-style output."""
        if not self.executor:
            return

        if self._current_db:
            self.console.print(
                f"[dim #58a6ff]mysql [{self._current_db}]>[/dim #58a6ff] [bold]{sql}[/bold]"
            )
        else:
            self.console.print(f"[dim]mysql>[/dim] [bold]{sql}[/bold]")

        result = self.executor.execute_and_format(sql, print_output=True)

        # Handle USE command
        if sql.strip().upper().startswith("USE") and result.success:
            db_match = re.search(r"USE\s+`?(\w+)`?", sql.strip(), re.IGNORECASE)
            if db_match:
                self._switch_database(db_match.group(1))

    def _switch_database(self, db_name: str):
        """Switch to a different database."""
        result = self.mysql.use_database(db_name)
        if result.success:
            self._current_db = db_name

            if self.agent:
                thread_id = self.agent.set_database_context(db_name)
                self._current_thread_id = thread_id
                if self.executor:
                    self.executor.set_thread(thread_id)

                # Show chat history count
                msg_count = self.persistence.get_message_count(thread_id)
                if msg_count > 0:
                    self.console.print(
                        f"[dim]Loaded {msg_count} previous messages for `{db_name}`[/dim]"
                    )

            self.console.print(
                f"[green]Database changed to[/green] [bold #58a6ff]{db_name}[/bold #58a6ff]"
            )
        else:
            self.console.print(f"[red]Failed: {result.error}[/red]")

    def _handle_command(self, command: str):
        """Handle /slash commands."""
        parts = command.strip().split(maxsplit=1)
        cmd = parts[0].lower()
        arg = parts[1] if len(parts) > 1 else ""

        if cmd == "/exit" or cmd == "/quit":
            self._running = False

        elif cmd == "/help":
            if self.agent:
                self.console.print(self.agent._get_help_text())

        elif cmd == "/mode":
            self._mode = "sql" if self._mode == "chat" else "chat"
            self.console.print(f"[dim]Switched to {self._mode.upper()} mode[/dim]")

        elif cmd == "/use" and arg:
            self._switch_database(arg)

        elif cmd == "/databases" or cmd == "/dbs":
            self._execute_sql("SHOW DATABASES")

        elif cmd == "/tables":
            if self._current_db:
                self._execute_sql(f"SHOW TABLES FROM `{self._current_db}`")

        elif cmd == "/schema":
            if self.agent and self.agent.schema_summary:
                self.console.print(self.agent.schema_summary)

        elif cmd == "/refresh":
            if self.agent:
                self.agent.refresh_schema_force()
                self.console.print("[green]Schema refreshed[/green]")

        elif cmd == "/history":
            if self._current_thread_id:
                history = self.persistence.get_query_history(self._current_thread_id, limit=20)
                if history:
                    for i, q in enumerate(history, 1):
                        status = "[green]✓[/green]" if q["success"] else "[red]✗[/red]"
                        self.console.print(f"  {i}. {status} {q['sql_query'][:80]}")

        elif cmd == "/sessions":
            sessions = self.persistence.list_sessions()
            for s in sessions:
                self.console.print(
                    f"  • {s['mysql_db_name']} ({s.get('message_count', 0)} messages)"
                )

        elif cmd == "/clear":
            os.system("clear" if os.name != "nt" else "cls")

        elif cmd == "/version":
            self.console.print(f"DBMA v{app_config.version}")

        else:
            self.console.print(f"[yellow]Unknown command: {command}. Type /help[/yellow]")

    def _looks_like_sql(self, text: str) -> bool:
        """Quick check if input looks like direct SQL."""
        sql_keywords = {
            "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
            "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "TRUNCATE", "USE",
            "BEGIN", "COMMIT", "ROLLBACK", "CALL", "GRANT", "REVOKE",
        }
        first_word = text.split()[0].upper() if text.split() else ""
        return first_word in sql_keywords

    def _print_banner(self):
        """Print the ASCII art banner."""
        banner = """
[bold #58a6ff]
  ██████╗ ██████╗ ███╗   ███╗ █████╗ 
  ██╔══██╗██╔══██╗████╗ ████║██╔══██╗
  ██║  ██║██████╔╝██╔████╔██║███████║
  ██║  ██║██╔══██╗██║╚██╔╝██║██╔══██║
  ██████╔╝██████╔╝██║ ╚═╝ ██║██║  ██║
  ╚═════╝ ╚═════╝ ╚═╝     ╚═╝╚═╝  ╚═╝
[/bold #58a6ff][bold]  Database Management Agent v{version}[/bold]
[dim]  Natural Language ↔ MySQL • Persistent Memory • Multi-DB[/dim]
""".format(version=app_config.version)
        self.console.print(banner)

    def _shutdown(self):
        """Clean up on exit."""
        self.console.print("\n[dim]Shutting down DBMA...[/dim]")
        self.mysql.disconnect()
        self.persistence.disconnect()
        self.console.print("[green]Goodbye![/green]")
