# ============================================================
# DBMA - Database Management Agent
# core/mysql_manager.py — MySQL Connection & Operations Manager
# ============================================================

import time
import json
from typing import Optional, List, Dict, Any, Tuple
import mysql.connector
from mysql.connector import Error as MySQLError, errorcode
from loguru import logger

from config import mysql_config


class QueryResult:
    """Structured result from a MySQL query execution."""

    def __init__(
        self,
        success: bool,
        query: str,
        columns: Optional[List[str]] = None,
        rows: Optional[List[Tuple]] = None,
        affected_rows: int = 0,
        last_insert_id: Optional[int] = None,
        error: Optional[str] = None,
        execution_ms: int = 0,
        query_type: str = "UNKNOWN",
    ):
        self.success = success
        self.query = query
        self.columns = columns or []
        self.rows = rows or []
        self.affected_rows = affected_rows
        self.last_insert_id = last_insert_id
        self.error = error
        self.execution_ms = execution_ms
        self.query_type = query_type

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "query": self.query,
            "columns": self.columns,
            "rows": [list(r) for r in self.rows],
            "affected_rows": self.affected_rows,
            "last_insert_id": self.last_insert_id,
            "error": self.error,
            "execution_ms": self.execution_ms,
            "query_type": self.query_type,
        }

    def __repr__(self):
        if self.success:
            return f"<QueryResult OK rows={len(self.rows)} time={self.execution_ms}ms>"
        return f"<QueryResult ERROR: {self.error}>"


class MySQLManager:
    """
    Manages MySQL connections and provides a rich API for
    database introspection, query execution, and schema extraction.
    """

    def __init__(self):
        self._connection: Optional[mysql.connector.MySQLConnection] = None
        self._cursor = None
        self._current_database: Optional[str] = None
        self._connected: bool = False

    # ── Connection Management ─────────────────────────────────

    def connect(self, database: Optional[str] = None) -> bool:
        """Establish connection to MySQL server."""
        try:
            params = mysql_config.get_connection_params(database)
            self._connection = mysql.connector.connect(**params)
            self._cursor = self._connection.cursor(buffered=True)
            self._connected = True
            self._current_database = database
            logger.info(f"Connected to MySQL at {mysql_config.host}:{mysql_config.port}")
            return True
        except MySQLError as e:
            logger.error(f"MySQL connection failed: {e}")
            self._connected = False
            return False

    def disconnect(self):
        """Close the MySQL connection gracefully."""
        try:
            if self._cursor:
                self._cursor.close()
            if self._connection and self._connection.is_connected():
                self._connection.close()
            logger.info("Disconnected from MySQL")
        except MySQLError as e:
            logger.warning(f"Error during disconnect: {e}")
        finally:
            self._connected = False
            self._connection = None
            self._cursor = None

    def is_connected(self) -> bool:
        """Check if connection is alive."""
        try:
            if self._connection and self._connection.is_connected():
                self._connection.ping(reconnect=True, attempts=3, delay=1)
                return True
        except Exception:
            pass
        return False

    def reconnect(self) -> bool:
        """Attempt to reconnect."""
        self.disconnect()
        return self.connect(self._current_database)

    # ── Database Selection ────────────────────────────────────

    def use_database(self, database_name: str) -> QueryResult:
        """Switch to a specific database."""
        result = self.execute_query(f"USE `{database_name}`")
        if result.success:
            self._current_database = database_name
            logger.info(f"Switched to database: {database_name}")
        return result

    def get_current_database(self) -> Optional[str]:
        """Returns name of current database."""
        return self._current_database

    # ── Query Execution ───────────────────────────────────────

    def execute_query(self, query: str) -> QueryResult:
        """
        Execute any MySQL query and return structured QueryResult.
        Handles SELECT, INSERT, UPDATE, DELETE, DDL, and meta-commands.
        """
        if not self.is_connected():
            if not self.reconnect():
                return QueryResult(
                    success=False,
                    query=query,
                    error="Not connected to MySQL. Reconnection failed.",
                )

        query = query.strip().rstrip(";").strip()
        query_type = self._detect_query_type(query)

        start_time = time.time()
        try:
            # Handle multi-statement or USE commands specially
            if query_type == "USE":
                db_name = query.split()[-1].strip("`'\"")
                self._connection.database = db_name
                self._current_database = db_name
                elapsed = int((time.time() - start_time) * 1000)
                return QueryResult(
                    success=True,
                    query=query,
                    query_type=query_type,
                    execution_ms=elapsed,
                )

            self._cursor.execute(query)

            if query_type in ("SELECT", "SHOW", "DESCRIBE", "EXPLAIN"):
                columns = [desc[0] for desc in self._cursor.description] if self._cursor.description else []
                rows = self._cursor.fetchall()
                elapsed = int((time.time() - start_time) * 1000)
                return QueryResult(
                    success=True,
                    query=query,
                    columns=columns,
                    rows=list(rows),
                    execution_ms=elapsed,
                    query_type=query_type,
                )
            else:
                self._connection.commit()
                affected = self._cursor.rowcount
                last_id = self._cursor.lastrowid
                elapsed = int((time.time() - start_time) * 1000)
                return QueryResult(
                    success=True,
                    query=query,
                    affected_rows=affected,
                    last_insert_id=last_id,
                    execution_ms=elapsed,
                    query_type=query_type,
                )

        except MySQLError as e:
            elapsed = int((time.time() - start_time) * 1000)
            logger.error(f"Query failed: {e}\nQuery: {query}")
            return QueryResult(
                success=False,
                query=query,
                error=str(e),
                execution_ms=elapsed,
                query_type=query_type,
            )

    def execute_script(self, script: str) -> List[QueryResult]:
        """Execute multiple semicolon-separated SQL statements."""
        statements = [s.strip() for s in script.split(";") if s.strip()]
        results = []
        for stmt in statements:
            result = self.execute_query(stmt)
            results.append(result)
            if not result.success:
                logger.warning(f"Script stopped at failed statement: {stmt}")
                break
        return results

    def _detect_query_type(self, query: str) -> str:
        """Detect the type of SQL query."""
        first_word = query.strip().split()[0].upper() if query.strip() else ""
        type_map = {
            "SELECT": "SELECT",
            "SHOW": "SHOW",
            "DESCRIBE": "DESCRIBE",
            "DESC": "DESCRIBE",
            "EXPLAIN": "EXPLAIN",
            "INSERT": "INSERT",
            "UPDATE": "UPDATE",
            "DELETE": "DELETE",
            "CREATE": "CREATE",
            "DROP": "DROP",
            "ALTER": "ALTER",
            "TRUNCATE": "TRUNCATE",
            "USE": "USE",
            "SET": "SET",
            "BEGIN": "TRANSACTION",
            "COMMIT": "TRANSACTION",
            "ROLLBACK": "TRANSACTION",
            "CALL": "PROCEDURE",
            "GRANT": "PRIVILEGE",
            "REVOKE": "PRIVILEGE",
        }
        return type_map.get(first_word, "UNKNOWN")

    # ── Database Introspection ────────────────────────────────

    def list_databases(self) -> List[str]:
        """Return list of all MySQL databases."""
        result = self.execute_query("SHOW DATABASES")
        if result.success:
            return [row[0] for row in result.rows]
        return []

    def list_tables(self, database: Optional[str] = None) -> List[str]:
        """Return list of tables in current or specified database."""
        db = database or self._current_database
        if not db:
            return []
        result = self.execute_query(f"SHOW TABLES FROM `{db}`")
        if result.success:
            return [row[0] for row in result.rows]
        return []

    def get_table_schema(self, table_name: str, database: Optional[str] = None) -> Dict[str, Any]:
        """Get full schema info for a specific table."""
        db = database or self._current_database
        if not db:
            return {}

        schema = {"table": table_name, "database": db, "columns": [], "indexes": [], "foreign_keys": []}

        # Columns
        col_result = self.execute_query(f"DESCRIBE `{db}`.`{table_name}`")
        if col_result.success:
            for row in col_result.rows:
                schema["columns"].append({
                    "name": row[0],
                    "type": row[1],
                    "null": row[2],
                    "key": row[3],
                    "default": row[4],
                    "extra": row[5],
                })

        # Indexes
        idx_result = self.execute_query(f"SHOW INDEX FROM `{db}`.`{table_name}`")
        if idx_result.success:
            for row in idx_result.rows:
                schema["indexes"].append({
                    "key_name": row[2],
                    "column": row[4],
                    "unique": not bool(row[1]),
                    "type": row[10],
                })

        # Foreign keys (via information_schema)
        fk_query = f"""
            SELECT 
                COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME,
                CONSTRAINT_NAME
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = '{db}'
              AND TABLE_NAME = '{table_name}'
              AND REFERENCED_TABLE_NAME IS NOT NULL
        """
        fk_result = self.execute_query(fk_query)
        if fk_result.success:
            for row in fk_result.rows:
                schema["foreign_keys"].append({
                    "column": row[0],
                    "ref_table": row[1],
                    "ref_column": row[2],
                    "constraint_name": row[3],
                })

        return schema

    def get_full_database_schema(self, database: Optional[str] = None) -> Dict[str, Any]:
        """
        Extract complete schema of entire database including all tables,
        columns, indexes, foreign keys, views, and procedures.
        Returns a rich dict suitable for LLM context injection.
        """
        db = database or self._current_database
        if not db:
            return {}

        logger.info(f"Extracting full schema for database: {db}")
        tables = self.list_tables(db)

        full_schema = {
            "database": db,
            "tables": {},
            "views": [],
            "procedures": [],
            "table_count": len(tables),
        }

        for table in tables:
            full_schema["tables"][table] = self.get_table_schema(table, db)

        # Views
        view_result = self.execute_query(
            f"SELECT TABLE_NAME FROM information_schema.VIEWS WHERE TABLE_SCHEMA = '{db}'"
        )
        if view_result.success:
            full_schema["views"] = [row[0] for row in view_result.rows]

        # Stored procedures
        proc_result = self.execute_query(
            f"SELECT ROUTINE_NAME FROM information_schema.ROUTINES "
            f"WHERE ROUTINE_SCHEMA = '{db}' AND ROUTINE_TYPE = 'PROCEDURE'"
        )
        if proc_result.success:
            full_schema["procedures"] = [row[0] for row in proc_result.rows]

        return full_schema

    def get_table_sample(self, table_name: str, limit: int = 3) -> QueryResult:
        """Get sample rows from a table for LLM context."""
        return self.execute_query(f"SELECT * FROM `{table_name}` LIMIT {limit}")

    def get_row_count(self, table_name: str) -> int:
        """Get approximate row count for a table."""
        result = self.execute_query(
            f"SELECT TABLE_ROWS FROM information_schema.TABLES "
            f"WHERE TABLE_SCHEMA = '{self._current_database}' AND TABLE_NAME = '{table_name}'"
        )
        if result.success and result.rows:
            return result.rows[0][0] or 0
        return 0

    def format_schema_for_llm(self, schema: Dict[str, Any]) -> str:
        """
        Format the full database schema into a clean string
        that an LLM can understand and use for SQL generation.
        """
        lines = [f"DATABASE: `{schema['database']}`\n"]
        lines.append(f"Total Tables: {schema['table_count']}\n")
        lines.append("=" * 60)

        for table_name, table_info in schema.get("tables", {}).items():
            lines.append(f"\nTABLE: `{table_name}`")
            lines.append("Columns:")
            for col in table_info.get("columns", []):
                nullable = "NULL" if col["null"] == "YES" else "NOT NULL"
                key_info = f" [{col['key']}]" if col["key"] else ""
                default = f" DEFAULT={col['default']}" if col["default"] else ""
                extra = f" {col['extra']}" if col["extra"] else ""
                lines.append(
                    f"  - {col['name']}: {col['type']} {nullable}{key_info}{default}{extra}"
                )

            if table_info.get("foreign_keys"):
                lines.append("Foreign Keys:")
                for fk in table_info["foreign_keys"]:
                    lines.append(
                        f"  - {fk['column']} → {fk['ref_table']}.{fk['ref_column']}"
                    )

            if table_info.get("indexes"):
                unique_idxs = [i for i in table_info["indexes"] if i["unique"] and i["key_name"] != "PRIMARY"]
                if unique_idxs:
                    lines.append("Unique Indexes:")
                    for idx in unique_idxs:
                        lines.append(f"  - {idx['key_name']} on ({idx['column']})")

        if schema.get("views"):
            lines.append(f"\nVIEWS: {', '.join(schema['views'])}")

        if schema.get("procedures"):
            lines.append(f"\nSTORED PROCEDURES: {', '.join(schema['procedures'])}")

        return "\n".join(lines)
