# ============================================================
# DBMA - Database Management Agent
# ui/tui.py — Main Textual TUI Application (Split-Panel Shell)
# ============================================================
#
# FIX SUMMARY (original structure kept 100%):
#
#   FIX 1 — _run_agent:       @work(thread=True) def (not async def)
#                              UI never freezes, LLM runs in real thread
#
#   FIX 2 — _execute_sql:     @work(thread=True) def (not async def)
#                              Removed broken 'async' that caused hangs
#
#   FIX 3 — _switch_database: @work(thread=True) def (not async coroutine)
#                              run_worker() removed — just call directly
#
#   FIX 4 — SQL → query input: SQL is placed in input for user review
#                              auto_execute ONLY for SHOW/DESCRIBE (read-only)
#                              NEVER auto-executes CREATE/INSERT/UPDATE/DELETE
#
#   FIX 5 — ChatBubble:       Content built once in __init__, never re-built
#                              Prevents text duplication in chat panel
#
#   FIX 6 — query-input-area: CSS height fixed to 5 — never expands/breaks
#
#   FIX 7 — _initialize:      Moved to @work(thread=True) def — not async
# ============================================================

import re
from pathlib import Path
from typing import Optional
from datetime import datetime

from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical, ScrollableContainer
from textual.widgets import Input, Label, Static, Button, RichLog
from textual.reactive import reactive
from textual.message import Message
from textual.screen import ModalScreen
from textual import work
from loguru import logger

from core.mysql_manager import MySQLManager
from core.persistence import PersistenceManager
from core.agent import DBMAAgent, AgentResponse, AgentIntent
from core.query_executor import QueryExecutor
from config import mysql_config, app_config


# ── Confirmation Modal ────────────────────────────────────────
class DestructiveConfirmModal(ModalScreen):
    """
    Modal for confirming destructive SQL operations.
    Y or Enter = confirm. N or Escape = cancel.
    """

    BINDINGS = [
        ("escape", "cancel",  "Cancel"),
        ("y",      "execute", "Yes Execute"),
        ("n",      "cancel",  "No Cancel"),
    ]

    def __init__(self, sql: str, callback):
        self._sql = sql
        self._callback = callback
        super().__init__()

    def compose(self) -> ComposeResult:
        with Container(id="confirm-modal-container"):
            yield Label("DESTRUCTIVE OPERATION", id="modal-title")
            yield Label("SQL to execute:", id="modal-subtitle")
            yield Static(
                f"[bold #f0883e]{self._sql[:300]}[/bold #f0883e]",
                id="modal-query",
            )
            yield Label("This CANNOT be undone!", id="modal-warning")
            yield Label(
                "Press Y or click Execute to confirm  |  N or Escape to cancel",
                id="modal-hint",
            )
            with Horizontal(id="modal-buttons"):
                yield Button("No, Cancel", id="btn-cancel")
                yield Button("Yes, Execute It", id="btn-execute")

    def on_mount(self) -> None:
        try:
            self.query_one("#btn-cancel", Button).focus()
        except Exception:
            pass

    def on_button_pressed(self, event: Button.Pressed) -> None:
        confirmed = (event.button.id == "btn-execute")
        self.dismiss()
        self._callback(confirmed)

    def action_execute(self) -> None:
        self.dismiss()
        self._callback(True)

    def action_cancel(self) -> None:
        self.dismiss()
        self._callback(False)


# ── Chat Bubble ───────────────────────────────────────────────
class ChatBubble(Static):
    """
    A single chat message bubble.
    FIX: Content built ONCE in __init__ and never modified.
    This prevents the text duplication bug from the previous version.
    """

    def __init__(self, role: str, content: str, sql: Optional[str] = None, **kwargs):
        self._role = role

        # Build display text once — immutable after this
        if role == "human":
            label = "[bold #58a6ff]You ▶[/bold #58a6ff]"
        elif role == "system":
            label = "[bold #f0883e]System ℹ[/bold #f0883e]"
        elif role == "error":
            label = "[bold #f85149]Error ✗[/bold #f85149]"
        else:
            label = "[bold #3fb950]DBMA ◆[/bold #3fb950]"

        # Escape user content to prevent markup rendering glitches
        safe = content.replace("[", "\\[") if role == "human" else content

        display = f"{label}\n{safe}"

        if sql:
            safe_sql = sql.replace("[", "\\[")
            display += f"\n\n[dim]Generated SQL:[/dim]\n[bold #79c0ff]{safe_sql}[/bold #79c0ff]"

        # Pass fully built string to Static — never call update() after this
        super().__init__(display, **kwargs)

        # CSS class
        css_map = {
            "human":     "chat-bubble-human",
            "assistant": "chat-bubble-agent",
            "system":    "chat-bubble-system",
            "error":     "chat-bubble-error",
        }
        self.add_class(css_map.get(role, "chat-bubble-agent"))


# ── Main DBMA TUI Application ─────────────────────────────────
class DBMAApp(App):
    """
    Main Textual application for DBMA.
    Split-panel: Query Output (left) + Chat (right).
    """

    CSS_PATH = str(Path(__file__).parent / "dbma.tcss")
    TITLE = "DBMA — Database Management Agent"

    BINDINGS = [
        ("ctrl+c", "quit",               "Quit"),
        ("ctrl+r", "refresh_schema",     "Refresh Schema"),
        ("ctrl+h", "toggle_help",        "Help"),
        ("ctrl+l", "clear_query_output", "Clear Output"),
        ("tab",    "focus_chat",         "Focus Chat"),
        ("escape", "focus_query",        "Focus Query"),
    ]

    # Capture ALL mouse events at app level so terminal never gets them
    # This prevents the terminal from painting its own blue selection highlight
    ENABLE_COMMAND_PALETTE = False

    # Reactive state
    current_db       = reactive("None")
    is_connected     = reactive(False)
    query_count      = reactive(0)
    is_agent_thinking = reactive(False)

    def __init__(self):
        super().__init__()
        self.mysql_manager  = MySQLManager()
        self.persistence    = PersistenceManager()
        self.query_executor: Optional[QueryExecutor] = None
        self.agent:          Optional[DBMAAgent]     = None
        self._current_thread_id: Optional[str] = None

    # ── App Lifecycle ─────────────────────────────────────────

    def on_mount(self) -> None:
        """Called when app starts."""
        self._initialize()

    def on_mouse_down(self, event) -> None:
        """
        Capture ALL mouse down events at the app level.
        This tells Textual to handle the mouse, which prevents the terminal
        (VS Code / Windows Terminal) from entering its own text selection mode
        and painting the blue highlight stripe across the full screen width.
        """
        event.stop()   # stop propagation — app owns this event, not the terminal

    @work(thread=True, exclusive=True)
    def _initialize(self):
        """
        FIX 7: Background thread initialization — UI stays responsive.
        Was: async def _initialize() run via run_worker(coroutine)
        Now: @work(thread=True) def — true background thread
        """
        self._sys("Initializing DBMA...")

        # Connect MySQL
        self._sys(f"Connecting to MySQL at {mysql_config.host}:{mysql_config.port}...")
        if self.mysql_manager.connect():
            self.call_from_thread(setattr, self, "is_connected", True)
            self._sys("✓ MySQL connected successfully", "success")
        else:
            self._sys("✗ MySQL connection failed! Check your .env", "error")
            return

        # Connect PostgreSQL
        self._sys("Connecting to PostgreSQL persistence...")
        if self.persistence.connect():
            self.persistence.initialize_schema()
            self._sys("✓ PostgreSQL persistence connected", "success")
        else:
            self._sys("✗ PostgreSQL failed! Chat history won't persist.", "warning")

        # Init Agent + Executor
        self.agent          = DBMAAgent(self.mysql_manager, self.persistence)
        self.query_executor = QueryExecutor(self.mysql_manager, self.persistence)

        self._sys(f"✓ DBMA Agent ready (Model: {self.agent._llm.model if self.agent._llm else 'N/A'})", "success")

        # List databases
        dbs = self.mysql_manager.list_databases()
        if dbs:
            self.call_from_thread(
                self._print_to_query_output,
                f"[dim]Available databases: {', '.join(dbs)}[/dim]"
            )

        # Welcome chat bubble
        self.call_from_thread(
            self._add_chat_bubble,
            "system",
            self._get_welcome_message(),
        )

        self.call_from_thread(self._update_status_bar)
        self.call_from_thread(lambda: self.query_one("#chat-input").focus())

    def compose(self) -> ComposeResult:
        """Build the UI layout."""
        yield Container(
            # ── Header ──────────────────────────────────────
            Horizontal(
                Label(f"◆ DBMA v{app_config.version}", id="header-title"),
                Label("", id="header-db-badge"),
                Label("", id="header-status"),
                id="header",
            ),

            # ── Main Split ───────────────────────────────────
            Horizontal(

                # LEFT — Query Output + SQL Input
                Vertical(
                    Label(" 📊 Query Output", id="query-panel-header"),
                    RichLog(
                        highlight=True,
                        markup=True,
                        id="query-output",
                        wrap=True,
                        auto_scroll=True,
                    ),
                    Vertical(
                        Horizontal(
                            Label("mysql",  id="query-prompt-label"),
                            Label("",       id="query-prompt-db"),
                            Label("> ",     id="query-prompt-arrow"),
                            id="query-prompt-line",
                        ),
                        Input(
                            placeholder="SQL here (auto-filled by agent) — press Enter to execute",
                            id="query-input",
                        ),
                        id="query-input-area",
                    ),
                    id="query-panel",
                ),

                # RIGHT — Chat Panel
                Vertical(
                    Label(" 💬 DBMA Chat", id="chat-panel-header"),
                    ScrollableContainer(id="chat-messages"),
                    Vertical(
                        Label(" ⌨ Ask DBMA ▶", id="chat-input-label"),
                        Input(
                            placeholder="Ask anything in plain English... (e.g., 'show me all orders from today')",
                            id="chat-input",
                        ),
                        id="chat-input-area",
                    ),
                    id="chat-panel",
                ),
                id="main-container",
            ),

            # ── Status Bar ───────────────────────────────────
            Horizontal(
                Label("", id="status-left"),
                Label("", id="status-right"),
                id="status-bar",
            ),
        )

    # ── Input Handlers ────────────────────────────────────────

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Handle Enter in any input field."""
        value = event.value.strip()
        event.input.value = ""     # clear immediately — prevents stale content

        if not value:
            return

        if event.input.id == "chat-input":
            self._handle_chat_input(value)

        elif event.input.id == "query-input":
            if value.startswith("/"):
                self._handle_slash_command(value)
            else:
                self._handle_query_execution(value)

    # ── Chat Flow ─────────────────────────────────────────────

    def _handle_chat_input(self, user_input: str) -> None:
        """Process natural language from chat panel."""
        if user_input.startswith("/"):
            self._handle_slash_command(user_input)
            return

        # Show human bubble immediately
        self._add_chat_bubble("human", user_input)
        self._update_status_bar()

        if not self.agent:
            self._add_chat_bubble("error", "⚠️ Agent not initialized — please wait...")
            return

        # Show thinking
        self.is_agent_thinking = True
        self._update_loading_state(True)

        # FIX 1: true background thread — UI never freezes
        self._run_agent(user_input)

    @work(thread=True, exclusive=False)
    def _run_agent(self, user_input: str):
        """
        FIX 1: @work(thread=True) def — Ollama call in real background thread.
        Was: async def called via run_worker (still blocked event loop).
        Now: Plain def decorated with @work(thread=True) — truly non-blocking.
        The LLM (Ollama) can take seconds — this keeps UI alive during that time.
        """
        try:
            # This blocking call lives entirely in its own thread
            response: AgentResponse = self.agent.chat(user_input)
            # Return to main UI thread to update display
            self.call_from_thread(self._handle_agent_response, response)
        except Exception as e:
            logger.error(f"Agent error: {e}")
            self.call_from_thread(
                self._add_chat_bubble,
                "error",
                f"⚠️ Agent error: {str(e)}\n\nMake sure Ollama is running: ollama serve\n"
                f"And model is pulled: ollama pull llama3.1:8b",
            )
        finally:
            # Always restore UI state
            self.call_from_thread(self._update_loading_state, False)
            self.is_agent_thinking = False

    def _handle_agent_response(self, response: AgentResponse) -> None:
        """
        Called on the MAIN thread after LLM responds.
        Updates chat panel and populates query input.
        """
        # Add agent reply to chat (with SQL shown inside bubble)
        self._add_chat_bubble("assistant", response.natural_text, sql=response.sql_query)

        # Handle database switch
        if (response.intent == AgentIntent.SWITCH_DATABASE
                and response.metadata.get("target_database")):
            self._switch_database(response.metadata["target_database"])
            return

        # FIX 4: SQL goes INTO query input for user to review/edit/run
        # NEVER auto-execute — except truly safe read-only queries (SHOW, DESCRIBE)
        if response.has_sql():
            # ── CRITICAL FIX ──────────────────────────────────────────────────────
            # The Input widget CANNOT handle multi-line strings (newlines cause the
            # widget to overflow and destroy the entire layout — the "blue lines" bug).
            # Solution: compress the SQL to a single line before placing it in Input.
            # The full formatted SQL is already shown in the chat bubble above,
            # so the user can read it there. The Input just needs it on one line
            # to execute correctly — MySQL accepts single-line SQL perfectly.
            # ─────────────────────────────────────────────────────────────────────
            raw_sql = response.sql_query or ""
            single_line_sql = " ".join(raw_sql.split())   # collapses ALL whitespace/newlines

            try:
                qi = self.query_one("#query-input", Input)
                qi.value = single_line_sql                 # ← single line, never breaks layout
                qi.focus()
            except Exception as e:
                logger.debug(f"Could not set query input: {e}")

            # Show hint in chat
            self._add_chat_bubble(
                "system",
                "👆 SQL placed in query input. Press [Enter] to execute, or edit first.",
            )

            # Auto-execute ONLY for truly read-only: SHOW DATABASES / SHOW TABLES
            # Everything else (CREATE, INSERT, UPDATE, DELETE) waits for user Enter
            if response.auto_execute and not response.requires_confirmation:
                first = single_line_sql.strip().split()[0].upper() if single_line_sql.strip() else ""
                if first in ("SHOW", "DESCRIBE", "DESC", "EXPLAIN"):
                    self._execute_sql(single_line_sql)

    # ── Query Execution ───────────────────────────────────────

    def _handle_query_execution(self, sql: str) -> None:
        """Execute SQL typed or confirmed in the query input."""
        first = sql.strip().split()[0].upper() if sql.strip() else ""
        if first in ("DELETE", "DROP", "TRUNCATE"):
            self.push_screen(
                DestructiveConfirmModal(
                    sql,
                    callback=lambda ok: (
                        self._execute_sql(sql) if ok
                        else self._print_to_query_output("[dim]Query cancelled.[/dim]")
                    ),
                )
            )
        else:
            self._execute_sql(sql)

    @work(thread=True, exclusive=False)
    def _execute_sql(self, sql: str, from_agent: bool = False):
        """
        FIX 2: @work(thread=True) def — SQL runs in background thread.
        Was: @work(thread=True) async def — 'async' on a thread worker is wrong
             and causes the event loop to block on network I/O.
        Now: Plain def — background thread handles MySQL I/O correctly.
        """
        if not self.query_executor:
            return

        # Echo query to output panel
        self.call_from_thread(
            self._print_to_query_output,
            f"\n[dim #58a6ff]mysql [{self.current_db}]>[/dim #58a6ff] [bold]{sql}[/bold]",
        )

        # Execute via executor
        result = self.query_executor.execute_and_format(sql, print_output=False)

        # Handle USE <db> — switch context
        if sql.strip().upper().startswith("USE") and result.success:
            m = re.search(r"USE\s+`?(\w+)`?", sql.strip(), re.IGNORECASE)
            if m:
                self.call_from_thread(self._switch_to_database_context, m.group(1))

        # Print result
        formatted = self.query_executor.format_result_as_text(result)
        self.call_from_thread(self._print_to_query_output, formatted)

        # Increment query counter
        self.query_count += 1
        self.call_from_thread(self._update_status_bar)

        # Save to query history
        if self._current_thread_id:
            self.persistence.save_query_history(
                thread_id=self._current_thread_id,
                sql_query=sql,
                success=result.success,
                execution_ms=result.execution_ms,
                rows_affected=result.affected_rows,
                error_message=result.error,
            )

    # ── Database Switching ────────────────────────────────────

    @work(thread=True, exclusive=True)
    def _switch_database(self, db_name: str):
        """
        FIX 3: @work(thread=True) def — database switch in background thread.
        Was: async def _switch_database() passed to run_worker(..., thread=True)
             which is incorrect — async coroutines passed to thread workers don't
             actually run in a thread, they run on the event loop.
        Now: @work(thread=True) def — called directly, truly non-blocking.
        """
        self.call_from_thread(
            self._print_to_query_output,
            f"\n[dim]Switching to database [bold #58a6ff]{db_name}[/bold #58a6ff]...[/dim]",
        )

        result = self.mysql_manager.use_database(db_name)
        if not result.success:
            self.call_from_thread(
                self._add_chat_bubble,
                "error",
                f"⚠️ Failed to switch to `{db_name}`: {result.error}",
            )
            return

        # Load agent context (schema + history) for this database
        if self.agent:
            thread_id = self.agent.set_database_context(db_name)
            self._current_thread_id = thread_id
            if self.query_executor:
                self.query_executor.set_thread(thread_id)

        # Update UI on main thread
        self.call_from_thread(self._switch_to_database_context, db_name)

    def _switch_to_database_context(self, db_name: str) -> None:
        """Update all UI elements after a database switch (main thread)."""
        self.current_db = db_name

        # Update header badge
        try:
            self.query_one("#header-db-badge", Label).update(f" ◆ {db_name} ")
        except Exception:
            pass

        # Update query prompt
        try:
            self.query_one("#query-prompt-db", Label).update(f" [{db_name}]")
        except Exception:
            pass

        # Reload chat history for this database
        self._load_chat_history_to_panel()
        self._update_status_bar()

        self._print_to_query_output(
            f"[green]Database changed to[/green] [bold #58a6ff]{db_name}[/bold #58a6ff]"
        )

    def _load_chat_history_to_panel(self) -> None:
        """Load and display full chat history for the current database."""
        if not self._current_thread_id:
            return

        try:
            container = self.query_one("#chat-messages", ScrollableContainer)
            container.remove_children()
        except Exception:
            return

        # Show which database we switched to
        try:
            container.mount(
                ChatBubble(
                    "system",
                    f"✓ Switched to database: [bold]{self.current_db}[/bold]\n"
                    f"Thread: {self._current_thread_id[:16]}...",
                )
            )
        except Exception:
            pass

        # Load saved messages from PostgreSQL
        messages = self.persistence.load_chat_history(
            self._current_thread_id,
            limit=app_config.max_chat_history,
        )

        if messages:
            for msg in messages:
                try:
                    container.mount(
                        ChatBubble(
                            role=msg.role,
                            content=msg.content,
                            sql=msg.sql_query,
                        )
                    )
                except Exception:
                    pass
            try:
                container.scroll_end(animate=False)
            except Exception:
                pass
            self._print_to_query_output(
                f"[dim]Loaded {len(messages)} previous messages for `{self.current_db}`[/dim]"
            )
        else:
            try:
                container.mount(
                    ChatBubble(
                        "system",
                        f"New chat started for [bold]{self.current_db}[/bold]. "
                        f"Ask me anything about this database!",
                    )
                )
            except Exception:
                pass

    # ── Slash Commands ────────────────────────────────────────

    def _handle_slash_command(self, command: str) -> None:
        """Handle /commands typed in either input."""
        cmd = command.strip().lower().split()[0]

        if cmd in ("/exit", "/quit"):
            self.action_quit()

        elif cmd == "/refresh":
            if self.agent and self.current_db != "None":
                self._refresh_schema_worker()
            else:
                self._add_chat_bubble("system", "Select a database first to refresh schema.")

        elif cmd == "/history":
            if self._current_thread_id:
                history = self.persistence.get_query_history(
                    self._current_thread_id, limit=20
                )
                if history:
                    lines = ["Recent Query History:\n"]
                    for i, q in enumerate(history, 1):
                        status = "✓" if q["success"] else "✗"
                        lines.append(f"  {i}. [{status}] {q['sql_query'][:60]}...")
                    self._add_chat_bubble("system", "\n".join(lines))
                else:
                    self._add_chat_bubble("system", "No query history yet.")
            else:
                self._add_chat_bubble("system", "No database selected.")

        elif cmd == "/sessions":
            sessions = self.persistence.list_sessions()
            if sessions:
                lines = [f"Active Sessions ({len(sessions)}):\n"]
                for s in sessions:
                    lines.append(
                        f"  • {s['mysql_db_name']} "
                        f"({s.get('message_count', 0)} messages, "
                        f"last: {str(s['last_active_at'])[:16]})"
                    )
                self._add_chat_bubble("system", "\n".join(lines))
            else:
                self._add_chat_bubble("system", "No sessions found.")

        elif cmd == "/clear":
            if self._current_thread_id:
                self.persistence.clear_thread(self._current_thread_id)
                try:
                    self.query_one("#chat-messages", ScrollableContainer).remove_children()
                except Exception:
                    pass
                self._add_chat_bubble("system", "✓ Chat history cleared for this database.")

        elif cmd in ("/databases", "/dbs"):
            self._execute_sql("SHOW DATABASES")

        elif cmd == "/tables":
            if self.current_db != "None":
                self._execute_sql(f"SHOW TABLES FROM `{self.current_db}`")
            else:
                self._add_chat_bubble("system", "Select a database first.")

        elif cmd == "/schema":
            if self.agent:
                self._add_chat_bubble("system", f"Schema:\n{self.agent.schema_summary}")

        elif cmd == "/help":
            if self.agent:
                self._add_chat_bubble("system", self.agent._get_help_text())

        else:
            self._add_chat_bubble(
                "system",
                f"Unknown command: {command}\nType /help for available commands.",
            )

    @work(thread=True)
    def _refresh_schema_worker(self):
        """Refresh schema in background thread."""
        if self.agent:
            self.agent.refresh_schema_force()
            self.call_from_thread(
                self._add_chat_bubble,
                "system",
                "✓ Database schema refreshed from MySQL.",
            )

    # ── UI Helpers ────────────────────────────────────────────

    def _print_to_query_output(self, text: str) -> None:
        """Write text to the left query output panel."""
        try:
            self.query_one("#query-output", RichLog).write(text)
        except Exception as e:
            logger.debug(f"_print_to_query_output: {e}")

    def _add_chat_bubble(
        self,
        role: str,
        content: str,
        sql: Optional[str] = None,
        error: bool = False,
    ) -> None:
        """
        Add a chat bubble to the right panel.
        FIX 5: Creates ChatBubble once and mounts — never modifies after mount.
        """
        try:
            container = self.query_one("#chat-messages", ScrollableContainer)
            actual_role = "error" if error else role
            bubble = ChatBubble(role=actual_role, content=content, sql=sql)
            container.mount(bubble)
            container.scroll_end(animate=False)
        except Exception as e:
            logger.debug(f"_add_chat_bubble: {e}")

    def _sys(self, msg: str, level: str = "info") -> None:
        """Write a [System] message to the query output panel."""
        colors = {
            "info":    "dim",
            "success": "green",
            "error":   "bold red",
            "warning": "yellow",
        }
        c = colors.get(level, "dim")
        self.call_from_thread(
            self._print_to_query_output,
            f"[{c}][System] {msg}[/{c}]",
        )

    def _show_system_message(self, msg: str, style: str = "info") -> None:
        """Alias for _sys — keeps compatibility with any external callers."""
        self._sys(msg, style)

    def _update_status_bar(self) -> None:
        """Update the bottom status bar."""
        try:
            conn = (
                "[green]● Connected[/green]"
                if self.is_connected
                else "[red]● Disconnected[/red]"
            )
            db   = f"DB: [bold #58a6ff]{self.current_db}[/bold #58a6ff]"
            qc   = f"Queries: {self.query_count}"
            ts   = datetime.now().strftime("%H:%M:%S")
            self.query_one("#status-left",  Label).update(f"{conn}  │  {db}  │  {qc}")
            self.query_one("#status-right", Label).update(f"mysql@{mysql_config.host}  │  {ts}")
        except Exception:
            pass

    def _update_loading_state(self, is_loading: bool) -> None:
        """Show/hide the thinking indicator in the chat label."""
        try:
            label = self.query_one("#chat-input-label", Label)
            label.update(
                " ⏳ DBMA is thinking..." if is_loading else " ⌨ Ask DBMA ▶"
            )
        except Exception:
            pass

    def _get_welcome_message(self) -> str:
        return (
            f"Welcome to DBMA v{app_config.version}!\n\n"
            "I'm your AI-powered MySQL assistant. I can:\n"
            "• Generate SQL from natural language\n"
            "• Switch between databases (just say 'use mydb')\n"
            "• Show tables, describe schemas, analyze data\n"
            "• Remember all our conversations per database\n\n"
            "Start by saying: 'show me all databases' or 'use <your_db_name>'"
        )

    # ── Action Handlers (keyboard shortcuts) ─────────────────

    def action_refresh_schema(self) -> None:
        """Ctrl+R"""
        if self.agent and self.current_db != "None":
            self._refresh_schema_worker()

    def action_clear_query_output(self) -> None:
        """Ctrl+L"""
        try:
            self.query_one("#query-output", RichLog).clear()
        except Exception:
            pass

    def action_focus_chat(self) -> None:
        """Tab"""
        try:
            self.query_one("#chat-input").focus()
        except Exception:
            pass

    def action_focus_query(self) -> None:
        """Escape"""
        try:
            self.query_one("#query-input").focus()
        except Exception:
            pass

    def action_toggle_help(self) -> None:
        """Ctrl+H"""
        if self.agent:
            self._add_chat_bubble("system", self.agent._get_help_text())

    def action_quit(self) -> None:
        """Ctrl+C"""
        try:
            self.mysql_manager.disconnect()
            self.persistence.disconnect()
        except Exception:
            pass
        self.exit()

    # ── Watch Reactive State ──────────────────────────────────

    def watch_current_db(self, _: str) -> None:
        self._update_status_bar()

    def watch_is_connected(self, _: bool) -> None:
        self._update_status_bar()

    def watch_query_count(self, _: int) -> None:
        self._update_status_bar()

















