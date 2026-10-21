-- 03d | Scenario 2: our refresh query is running, a writer arrives - STG only.
--
-- Steps:
--   Window B: start 03b_reader_read_committed.sql.
--   Window D (this file): run it a few seconds later, while B is still running.
--   The result shows how long the writer had to wait for our reader.
--   Repeat with 03b_reader_read_uncommitted.sql and compare.
--
-- Updates ONE row to its own value and rolls back immediately. Gives up after 2 minutes.

IF @@SERVERNAME <> N'ADM-SQL2020\ADMNEW'
BEGIN
    RAISERROR(N'This is not the STG server - stopping.', 16, 1);
    RETURN;
END

SET LOCK_TIMEOUT 120000;
DECLARE @start datetime2 = SYSDATETIME();

BEGIN TRAN;
    UPDATE HOTSAOT_COTROT SET HEROT = HEROT
    WHERE NOMRTOR = (SELECT MAX(NOMRTOR) FROM HOTSAOT_COTROT);
ROLLBACK;

SELECT @@SPID AS writer_spid,
       DATEDIFF(MILLISECOND, @start, SYSDATETIME()) AS writer_wait_ms,
       @@TRANCOUNT AS open_tran_should_be_0;

SET LOCK_TIMEOUT -1;
