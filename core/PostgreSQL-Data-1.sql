SELECT s.mysql_db_name, m.role, m.content, m.sql_query, m.created_at
FROM dbma_messages m
JOIN dbma_sessions s ON m.thread_id = s.thread_id
ORDER BY m.created_at DESC;
