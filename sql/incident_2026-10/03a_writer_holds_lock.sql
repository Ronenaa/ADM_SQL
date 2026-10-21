-- 03a | Scenario 1: a writer holds a lock, our refresh query arrives - STG only.
--
-- Opens a transaction, updates ONE expense header row to its own value (no real change),
-- holds the lock for 60 seconds, then ROLLS BACK automatically. Nothing is left changed or locked.
--
-- Steps:
--   Window A (this file): run it.
--   Window B: within 60s, run 03b_reader_read_committed.sql. Expectation: B waits until A finishes.
--   Note the start time of B and its 'Reader end' time in the Messages tab.

IF @@SERVERNAME <> N'ADM-SQL2020\ADMNEW'
BEGIN
    RAISERROR(N'This is not the STG server - stopping.', 16, 1);
    RETURN;
END

DECLARE @start datetime2 = SYSDATETIME();

BEGIN TRAN;

    ;WITH x AS (SELECT TOP (1) HEROT FROM HOTSAOT_COTROT ORDER BY NOMRTOR DESC)
    UPDATE x SET HEROT = HEROT;

    PRINT CONCAT('Writer SPID ', @@SPID, ' is holding the lock for 60 seconds...');
    WAITFOR DELAY '00:01:00';

ROLLBACK;

SELECT @@SPID AS writer_spid, @@TRANCOUNT AS open_tran_should_be_0,
       DATEDIFF(SECOND, @start, SYSDATETIME()) AS held_sec;

