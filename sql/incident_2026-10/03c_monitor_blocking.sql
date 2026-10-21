-- 03c | Who is blocking whom - read-only, safe on any server.
--
-- For Shlomo: run it on the live server BEFORE the next KILL and send a screenshot.
-- Needs VIEW SERVER STATE - BIX does not have it, so it cannot be used from our side.
--
-- How to read it:
--   program_name 'Mashup Engine...'          -> Power BI refresh
--   program_name 'Microsoft SQL Server Management Studio...' -> someone's manual SSMS window
--   open_transaction_count > 0 on an idle session -> a transaction left open (never committed)
--   blocking_session_id = X                  -> this row is waiting for session X

SELECT s.session_id,
       r.blocking_session_id,
       s.login_name,
       s.host_name,
       s.program_name,
       s.status                       AS session_status,
       s.open_transaction_count,
       r.status                       AS request_status,
       r.wait_type,
       r.wait_time / 1000             AS wait_sec,
       r.total_elapsed_time / 1000    AS running_sec,
       s.last_request_start_time,
       r.wait_resource,
       LEFT(t.text, 500)              AS query_text
FROM sys.dm_exec_sessions s
LEFT JOIN sys.dm_exec_requests r ON r.session_id = s.session_id
OUTER APPLY sys.dm_exec_sql_text(ISNULL(r.sql_handle,
            (SELECT c.most_recent_sql_handle FROM sys.dm_exec_connections c WHERE c.session_id = s.session_id))) t
WHERE s.is_user_process = 1
  AND s.session_id <> @@SPID
  AND (s.login_name = 'BIX'
       OR r.blocking_session_id > 0
       OR s.session_id IN (SELECT blocking_session_id FROM sys.dm_exec_requests WHERE blocking_session_id > 0))
ORDER BY r.blocking_session_id DESC, s.session_id;

-- Which objects are locked, and by whom
SELECT l.request_session_id AS session_id,
       OBJECT_NAME(p.object_id) AS object_name,
       l.resource_type, l.request_mode, l.request_status, COUNT(*) AS lock_count
FROM sys.dm_tran_locks l
LEFT JOIN sys.partitions p ON p.hobt_id = l.resource_associated_entity_id
WHERE l.resource_database_id = DB_ID()
  AND l.request_session_id <> @@SPID
  AND l.resource_type IN ('OBJECT', 'PAGE', 'KEY', 'RID')
GROUP BY l.request_session_id, OBJECT_NAME(p.object_id), l.resource_type, l.request_mode, l.request_status
ORDER BY l.request_session_id, lock_count DESC;
