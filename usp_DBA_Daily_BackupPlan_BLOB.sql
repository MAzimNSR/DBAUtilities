USE DBAUtility
GO
/*
----Create table for backup logging --one time task
CREATE TABLE dbo.DBA_Backup_Log
(
    LogID           INT IDENTITY(1,1) PRIMARY KEY,
    DatabaseName    SYSNAME,
    BackupType      VARCHAR(10),
    BackupStartTime DATETIME2,
    BackupEndTime   DATETIME2,
    BackupStatus    VARCHAR(20),
    ErrorMessage    NVARCHAR(MAX)
);
GO
ALTER TABLE dbo.DBA_Backup_Log
ADD
    BackupDurationSec AS DATEDIFF(SECOND, BackupStartTime, BackupEndTime),
    BackupSizeGB      DECIMAL(10,2),
    ThroughputMBps    DECIMAL(10,2);
GO

----Create sp for backup logging 

CREATE OR ALTER PROCEDURE dbo.usp_DBA_Backup_Failure_Alert
AS
BEGIN
    DECLARE @Body NVARCHAR(MAX);

    SELECT @Body =
    STRING_AGG(
        'Database: ' + DatabaseName +
        CHAR(13) +
        'Type: ' + BackupType +
        CHAR(13) +
        'Start: ' + CONVERT(VARCHAR, BackupStartTime, 120) +
        CHAR(13) +
        'End: ' + CONVERT(VARCHAR, BackupEndTime, 120) +
        CHAR(13) +
        'Error: ' + ISNULL(ErrorMessage,'') +
        CHAR(13) + CHAR(13),
        ''
    )
    FROM dbo.DBA_Backup_Log
    WHERE BackupStatus = 'FAILED'
      AND BackupStartTime >= DATEADD(HOUR,-24,GETDATE());

    IF @Body IS NOT NULL
    BEGIN
        EXEC msdb.dbo.sp_send_dbmail
            @profile_name = 'noreply@premier',
            @recipients   = '360DBA-team@dataqhealth.com',
            @subject      = 'SQL Server Azure Blob Backup FAILURE',
            @body         = @Body;
    END
END;
GO
*/



/*
================================================================================
Author : Muhammad Azim 
Purpose : Adaptive FULL backup to Azure Blob with intelligent striping
- Uses instance-name folder (e.g. prod-db01)
- Applies striping ONLY if estimated backup >= 200 GB
- Keeps minimum number of backup parts
- Azure Blob (BACKUP TO URL)
- Compression-aware logic
================================================================================
*/


----Test Run
/*
---Dry Run
EXEC dbo.usp_DBA_Daily_BackupPlan_BLOB 1

---Actual run
EXEC usp_DBA_Daily_BackupPlan_BLOB
*/

CREATE OR ALTER PROCEDURE dbo.usp_DBA_Daily_BackupPlan_BLOB  
    @DryRun BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    /*============================================================
      CONFIGURATION
    ============================================================*/
    DECLARE
        @MaxBackupFileSizeGB   INT           = 200,     -- Azure Blob practical limit
        @CompressionRatio     DECIMAL(4,2)  = 0.50,    -- Conservative estimate
        @MaxStripes           INT           = 8,
        @RetryMax             INT           = 3;

    /*============================================================
      COMMON VARIABLES
    ============================================================*/
    DECLARE
        @DBName               SYSNAME,
        @sql                  NVARCHAR(MAX),
        @pathFull             NVARCHAR(500),
        @pathDiff             NVARCHAR(500),
        @fileBase             NVARCHAR(500),
        @date1                NVARCHAR(30),
        @dow                  VARCHAR(15),
        @InstanceFolder       SYSNAME,
        @UsedDataGB           DECIMAL(10,2),
        @EstimatedBackupGB    DECIMAL(10,2),
        @BackupFileCount      INT,
        @UrlList              NVARCHAR(MAX),
        @i                    INT,
        @BufferCount          INT,
        @MaxTransfer          INT,
        @Retry                INT,
        @StartTime            DATETIME2;

    /*============================================================
      DATE / DAY LOGIC
    ============================================================*/
    SET @dow =
        CASE 
            WHEN DATEPART(HOUR, GETDATE()) BETWEEN 0 AND 10
                THEN DATENAME(WEEKDAY, DATEADD(DAY,-1,GETDATE()))
            ELSE DATENAME(WEEKDAY, GETDATE())
        END;

    SET @date1 =
        CASE 
            WHEN DATEPART(HOUR, GETDATE()) BETWEEN 0 AND 10
                THEN FORMAT(DATEADD(DAY,-1,GETDATE()), 'yyyyMMdd')
            ELSE FORMAT(GETDATE(), 'yyyyMMdd')
        END;

    /*============================================================
      INSTANCE-AWARE BLOB PATH
    ============================================================*/
    --SELECT @InstanceFolder = ConfigValue
    --FROM dbo.DBA_Backup_Config
    --WHERE ConfigKey = 'BlobInstanceFolder';

    IF @InstanceFolder IS NULL
        SET @InstanceFolder = REPLACE(@@SERVERNAME,'\','-');

    DECLARE @BlobBasePath NVARCHAR(500) = 'https://yourblobstoragecontainer.blob.core.windows.net/mssql/' + @InstanceFolder + '/backups/';

    SET @pathFull = @BlobBasePath + 'full/';
    SET @pathDiff = @BlobBasePath + 'differential/';

    /*============================================================
      DATABASE LIST
    ============================================================*/
    IF OBJECT_ID('tempdb..#DBs') IS NOT NULL DROP TABLE #DBs;

    SELECT d.name
    INTO #DBs
    FROM sys.databases d
    WHERE d.state_desc = 'ONLINE'
      AND d.name NOT IN ('master','model','msdb','tempdb','distribution','SSISDB','dbautility')
      AND d.name NOT LIKE 'JUNKDB%'
      AND d.name NOT LIKE '%Copy%'
      AND d.name NOT LIKE '%test%'
      AND d.name NOT LIKE '%old%'
      AND d.name NOT LIKE '%[_]DBA'
      AND d.name NOT LIKE '%bkp%'
      AND d.name NOT LIKE '[_][_]%'
      AND d.name NOT LIKE '%[0-9]%';

    /*============================================================
      MAIN LOOP
    ============================================================*/
    WHILE EXISTS (SELECT 1 FROM #DBs)
    BEGIN
        SELECT TOP (1) @DBName = name FROM #DBs ORDER BY name;
        DELETE FROM #DBs WHERE name = @DBName;

        SET @StartTime = SYSDATETIME();
        SET @Retry = 0;

        /*--------------------------------------------------------
          ESTIMATE BACKUP SIZE (USED DATA × COMPRESSION RATIO)
        --------------------------------------------------------*/
        SET @sql = N'
        USE [' + @DBName + N'];
        SELECT @UsedDataGB_OUT =
            SUM(FILEPROPERTY(name, ''SpaceUsed'')) * 8.0 / 1024 / 1024
        FROM sys.database_files;';

        EXEC sys.sp_executesql
            @sql,
            N'@UsedDataGB_OUT DECIMAL(10,2) OUTPUT',
            @UsedDataGB_OUT = @UsedDataGB OUTPUT;

        SET @EstimatedBackupGB = @UsedDataGB * @CompressionRatio;

        /*--------------------------------------------------------
          DETERMINE NUMBER OF BACKUP FILES (BASED ON BACKUP SIZE)
        --------------------------------------------------------*/
        SET @BackupFileCount =
            CASE
                WHEN @EstimatedBackupGB < @MaxBackupFileSizeGB THEN 1
                ELSE CEILING(@EstimatedBackupGB / @MaxBackupFileSizeGB)
            END;

        IF @BackupFileCount > @MaxStripes
            SET @BackupFileCount = @MaxStripes;

        /*--------------------------------------------------------
          I/O TUNING (BASED ON ESTIMATED BACKUP SIZE)
        --------------------------------------------------------*/
        SET @BufferCount =
            CASE 
                WHEN @EstimatedBackupGB >= 500 THEN 2200
                WHEN @EstimatedBackupGB >= 200 THEN 1500
                WHEN @EstimatedBackupGB >= 50  THEN 800
                ELSE 400
            END;

        SET @MaxTransfer =
            CASE 
                WHEN @EstimatedBackupGB >= 200 THEN 4194304
                ELSE 2097152
            END;

        /*--------------------------------------------------------
          BUILD TO URL LIST (MINIMUM STRIPES)
        --------------------------------------------------------*/
        SET @fileBase =
            CASE WHEN @dow <> 'Sunday'
                THEN @pathDiff + @DBName + '_diff_' + @date1
                ELSE @pathFull + @DBName + '_full_' + @date1
            END;

        SET @UrlList = N'';
        SET @i = 1;

        WHILE @i <= @BackupFileCount
        BEGIN
            SET @UrlList +=
                CASE WHEN @i > 1 THEN N',' ELSE N'' END +
                N'
    URL = N''' + @fileBase + N'_p' + CAST(@i AS NVARCHAR) + N'.bak''';

            SET @i += 1;
        END;

        /*--------------------------------------------------------
          BUILD BACKUP COMMAND
        --------------------------------------------------------*/
        SET @sql = N'
BACKUP DATABASE [' + @DBName + N']
TO ' + @UrlList + N'
WITH
    COMPRESSION,
    CHECKSUM,
    BUFFERCOUNT = ' + CAST(@BufferCount AS NVARCHAR) + N',
    BLOCKSIZE = 65536,
    MAXTRANSFERSIZE = ' + CAST(@MaxTransfer AS NVARCHAR) + N',
    STATS = 5,
    NAME = N''' + @DBName + CASE WHEN @dow<>'Sunday' THEN '_DIFF' ELSE '_FULL' END + N'''';

        IF @dow <> 'Sunday'
            SET @sql += N', DIFFERENTIAL';

        /*--------------------------------------------------------
          EXECUTION WITH RETRY
        --------------------------------------------------------*/
        BEGIN TRY
            WHILE @Retry < @RetryMax
            BEGIN
                BEGIN TRY
                    IF @DryRun = 1
                        PRINT @sql;
                    ELSE
                        EXEC sys.sp_executesql @sql;
                    BREAK;
                END TRY
                BEGIN CATCH
                    SET @Retry += 1;
                    IF @Retry >= @RetryMax THROW;
                    WAITFOR DELAY '00:00:20';
                END CATCH
            END;

            INSERT dbo.DBA_Backup_Log
            (
                DatabaseName,
                BackupType,
                BackupStartTime,
                BackupEndTime,
                BackupStatus
            )
            VALUES
            (
                @DBName,
                CASE WHEN @dow<>'Sunday' THEN 'DIFF' ELSE 'FULL' END,
                @StartTime,
                SYSDATETIME(),
                'SUCCESS'
            );
        END TRY
        BEGIN CATCH
            INSERT dbo.DBA_Backup_Log
            (
                DatabaseName,
                BackupType,
                BackupStartTime,
                BackupEndTime,
                BackupStatus,
                ErrorMessage
            )
            VALUES
            (
                @DBName,
                CASE WHEN @dow<>'Sunday' THEN 'DIFF' ELSE 'FULL' END,
                @StartTime,
                SYSDATETIME(),
                'FAILED',
                ERROR_MESSAGE()
            );
        END CATCH
    END
END;
EXEC usp_DBA_Backup_Failure_Alert

GO