# ============================================================
# DBMA - Database Management Agent
# core/query_executor.py — SQL Execution & MySQL-style Output Formatter
# ============================================================

from typing import Optional, List, Callable
from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich.panel import Panel
from rich.syntax import Syntax
from rich import box
from loguru import logger

from core.mysql_manager import MySQLManager, QueryResult
from core.persistence import PersistenceManager
from config import app_config


class QueryExecutor:
    """
    Executes SQL queries via MySQLManager and formats output
    exactly like the MySQL CLI — tables, row counts, timing, errors.

    Bridges the agent's generated SQL to actual database execution
    and updates persistence with the results.
    """

    def __init__(
            self,
            mysql_manager: MySQLManager,
            persistence: PersistenceManager,
            console: Optional[Console] = None,
    ):
        self.mysql = mysql_manager
        self.persistence = persistence
        self.console = console or Console()
        self._current_thread_id: Optional[str] = None

    def set_thread(self, thread_id: str):
        """Set the current persistence thread for query logging."""
        self._current_thread_id = thread_id

    def execute_and_format(
            self,
            sql: str,
            print_output: bool = True,
            output_callback: Optional[Callable[[str], None]] = None,
    ) -> QueryResult:
        """
        Execute a SQL query and return formatted, MySQL-CLI-style output.

        Args:
            sql: The SQL query to execute
            print_output: Whether to print to console directly
            output_callback: Optional callback to send formatted output line by line

        Returns:
            QueryResult with full execution details
        """
        result = self.mysql.execute_query(sql)

        # Build formatted output
        formatted_lines = self._format_result(result, sql)

        if print_output:
            for line in formatted_lines:
                self.console.print(line)

        if output_callback:
            for line in formatted_lines:
                output_callback(line)

        # Persist query to history
        if self._current_thread_id:
            self.persistence.save_query_history(
                thread_id=self._current_thread_id,
                sql_query=sql,
                success=result.success,
                execution_ms=result.execution_ms,
                rows_affected=result.affected_rows,
                error_message=result.error,
            )

        return result

    def _format_result(self, result: QueryResult, sql: str) -> List:
        """
        Format QueryResult into Rich renderables that mimic MySQL CLI output.
        Returns a list of Rich renderables.
        """
        output = []

        if not result.success:
            # Error output — red, like MySQL error format
            error_text = Text()
            error_text.append("ERROR", style="bold red")
            error_text.append(f": {result.error}", style="red")
            output.append(error_text)
            return output

        query_type = result.query_type

        if query_type in ("SELECT", "SHOW", "DESCRIBE", "EXPLAIN"):
            if result.rows:
                # Build Rich table (MySQL-style)
                table = self._build_mysql_table(result)
                output.append(table)

                # Row count line: "5 rows in set (0.002 sec)"
                row_word = "row" if len(result.rows) == 1 else "rows"
                timing = result.execution_ms / 1000
                count_text = Text()
                count_text.append(f"{len(result.rows)} {row_word} in set ", style="dim")
                count_text.append(f"({timing:.3f} sec)", style="dim italic")
                output.append(count_text)
            else:
                # Empty set
                empty_text = Text()
                empty_text.append("Empty set ", style="dim")
                empty_text.append(f"({result.execution_ms / 1000:.3f} sec)", style="dim italic")
                output.append(empty_text)

        elif query_type == "USE":
            ok_text = Text()
            ok_text.append("Database changed", style="green")
            output.append(ok_text)

        elif query_type in ("INSERT", "UPDATE", "DELETE"):
            ok_text = Text()
            ok_text.append("Query OK", style="bold green")
            row_word = "row" if result.affected_rows == 1 else "rows"
            ok_text.append(f", {result.affected_rows} {row_word} affected ", style="green")
            ok_text.append(f"({result.execution_ms / 1000:.3f} sec)", style="dim italic")
            if result.last_insert_id and query_type == "INSERT":
                output.append(ok_text)
                id_text = Text(f"  Last INSERT ID: {result.last_insert_id}", style="dim cyan")
                output.append(id_text)
            else:
                output.append(ok_text)

        elif query_type in ("CREATE", "DROP", "ALTER", "TRUNCATE"):
            ok_text = Text()
            ok_text.append("Query OK", style="bold green")
            ok_text.append(f", 0 rows affected ", style="green")
            ok_text.append(f"({result.execution_ms / 1000:.3f} sec)", style="dim italic")
            output.append(ok_text)

        elif query_type == "TRANSACTION":
            ok_text = Text("Query OK", style="bold green")
            output.append(ok_text)

        else:
            # Generic success
            ok_text = Text()
            ok_text.append("Query OK ", style="bold green")
            ok_text.append(f"({result.execution_ms / 1000:.3f} sec)", style="dim italic")
            output.append(ok_text)

        return output

    def _build_mysql_table(self, result: QueryResult) -> Table:
        """
        Build a Rich Table that mimics MySQL CLI table output.
        Example:
        +----+----------+-------+
        | id | name     | age   |
        +----+----------+-------+
        | 1  | Alice    | 25    |
        +----+----------+-------+
        """
        table = Table(
            box=box.SIMPLE_HEAVY,
            show_header=True,
            header_style="bold cyan",
            border_style="dim white",
            show_lines=False,
            pad_edge=False,
            collapse_padding=False,
        )

        # Add columns
        for col_name in result.columns:
            table.add_column(str(col_name), style="white", no_wrap=False)

        # Add rows
        for row in result.rows:
            str_row = []
            for cell in row:
                if cell is None:
                    str_row.append(Text("NULL", style="dim italic yellow"))
                else:
                    str_row.append(str(cell))
            table.add_row(*str_row)

        return table

    def format_sql_syntax(self, sql: str) -> Syntax:
        """Return a Rich Syntax object for SQL highlighting."""
        return Syntax(
            sql,
            "sql",
            theme="monokai",
            line_numbers=False,
            word_wrap=True,
        )

    def format_result_as_text(self, result: QueryResult) -> str:
        """
        Format result as plain text string (for chat panel display).
        Used when we need string output rather than Rich renderables.
        """
        if not result.success:
            return f"ERROR: {result.error}"

        if result.query_type in ("SELECT", "SHOW", "DESCRIBE", "EXPLAIN"):
            if not result.rows:
                return "Empty set"

            # Simple text table
            if not result.columns:
                return str(result.rows)

            lines = []
            col_widths = [len(str(c)) for c in result.columns]
            for row in result.rows:
                for i, cell in enumerate(row):
                    col_widths[i] = max(col_widths[i], len(str(cell) if cell is not None else "NULL"))

            # Header
            sep = "+" + "+".join("-" * (w + 2) for w in col_widths) + "+"
            header = "|" + "|".join(f" {str(c):<{w}} " for c, w in zip(result.columns, col_widths)) + "|"
            lines.append(sep)
            lines.append(header)
            lines.append(sep)

            # Rows
            for row in result.rows:
                cells = []
                for cell, w in zip(row, col_widths):
                    val = "NULL" if cell is None else str(cell)
                    cells.append(f" {val:<{w}} ")
                lines.append("|" + "|".join(cells) + "|")
            lines.append(sep)

            row_word = "row" if len(result.rows) == 1 else "rows"
            lines.append(f"{len(result.rows)} {row_word} in set ({result.execution_ms / 1000:.3f} sec)")
            return "\n".join(lines)

        elif result.query_type in ("INSERT", "UPDATE", "DELETE"):
            return f"Query OK, {result.affected_rows} row(s) affected ({result.execution_ms / 1000:.3f} sec)"
        elif result.query_type == "USE":
            return "Database changed"
        else:
            return f"Query OK ({result.execution_ms / 1000:.3f} sec)"

    def confirm_destructive(self, sql: str) -> bool:
        """
        Print a warning for destructive queries.
        In TUI mode, this is handled by the UI layer.
        Returns True (prompt handled by UI).
        """
        self.console.print(
            Panel(
                f"[bold red]⚠️  DESTRUCTIVE OPERATION WARNING[/bold red]\n\n"
                f"[yellow]{sql}[/yellow]\n\n"
                "[red]This operation cannot be undone![/red]",
                title="[bold red]Confirm Execution",
                border_style="red",
            )
        )
        return True
