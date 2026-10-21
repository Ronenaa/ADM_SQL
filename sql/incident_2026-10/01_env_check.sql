-- 01 | Environment check - read-only, safe on any server.
-- Run on STG, and send the same file to Shlomo to run on the live server, so the two can be compared.
      
SELECT @@SERVERNAME                                         AS server_name,
       DB_NAME()                                            AS db,
       SUSER_SNAME()                                        AS login_name,
       CAST(CONNECTIONPROPERTY('local_net_address') AS varchar(50)) AS server_ip,
       d.is_read_committed_snapshot_on                      AS rcsi_on,
       d.snapshot_isolation_state_desc                      AS snapshot_isolation,
       (SELECT CAST(value_in_use AS int) FROM sys.configurations WHERE name = 'max degree of parallelism')       AS maxdop,
       (SELECT CAST(value_in_use AS int) FROM sys.configurations WHERE name = 'cost threshold for parallelism') AS cost_threshold,
       (SELECT cpu_count FROM sys.dm_os_sys_info)           AS cpu_count
FROM sys.databases d
WHERE d.name = DB_NAME();

-- Row counts of the tables the incident touches (metadata only, no table scan)
SELECT o.name AS table_name, SUM(p.rows) AS row_count
FROM sys.partitions p
JOIN sys.objects o ON o.object_id = p.object_id
WHERE p.index_id IN (0, 1)
  AND o.name IN ('HOTSAOT_COTROT', 'HOTSAOT_SHOROT', 'HOTSAOT_SHROTIM_New',
                 'tblOrderPriceS', 'HZMNOT', 'PurchaseOrderLines', 'PurchaseOrder',
                 'CHSHBONIOT_COTROT', 'CHSHBONIOT_SHOROT', 'MOTSRIM', 'TRGOM_ANGLIT')
GROUP BY o.name
ORDER BY row_count DESC;

-- Index maintenance / other jobs that may run during the refresh window (needs msdb read access)
SELECT j.name AS job_name, j.enabled, s.name AS schedule_name,
       s.freq_type, s.active_start_time
FROM msdb.dbo.sysjobs j
JOIN msdb.dbo.sysjobschedules js ON js.job_id = j.job_id
JOIN msdb.dbo.sysschedules s     ON s.schedule_id = js.schedule_id
ORDER BY s.active_start_time;
