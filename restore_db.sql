-----------------------------------------------------------------------------------------------
--  DECLARE PARAMETERS FOR RUNNING THE MIGRATION HERE                                        --
-----------------------------------------------------------------------------------------------
declare @source_db				as varchar(100)		= 'mig_crh_source'
declare @source_backup_file		as varchar(100)		= 'F:\Backup\oltp\ContractRegistrationService_anon_20210430.bak'
declare @staging_db             as varchar(100)     = 'mig_crh_staging'
declare @staging_backup_file	as varchar(100)		= 'F:\Backup\oltp\UnifiedRegisters_anon_20210430.bak'

--  RESTORE PARAMETERS
declare @restore_source_db		as tinyint			= 0		-- should the source database be restored 1/0
declare @restore_staging_db		as tinyint			= 1		-- should the target database be restored 1/0



-----------------------------------------------------------------------------------------------
--  SCRIPT BELOW - DO NOT EDIT ANYTHING BELOW                                                --
-----------------------------------------------------------------------------------------------
declare @runId					as uniqueidentifier	=	newid()


use master

print '***************************************************************************************'
print '*                                                                                     *'
print '* CRH MIGRATION RUN ' + convert(varchar(40), @runId) + '                              *'
print '*                                                                                     *'
print '***************************************************************************************'
print '    1      Restore databases from backup'
print '======================================================================================='
print ''
print '    1.1.   Restore source database'
print '    -----------------------------------------------------------------------------------'

IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = @source_db)
BEGIN
  IF @restore_source_db = 0 
  BEGIN
    print '!!  Source database did not exist; overriding restore setting to TRUE'
	set @restore_source_db = 1
  END
END

IF @restore_source_db = 1
BEGIN
	IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = @source_db)
	BEGIN
		exec('CREATE DATABASE [' + @source_db + ']')
	END
	exec('ALTER DATABASE [' + @source_db + '] SET SINGLE_USER WITH ROLLBACK IMMEDIATE')
	exec('RESTORE DATABASE [' + @source_db + '] 
		  FROM DISK = N''' + @source_backup_file + ''' 
		  WITH FILE = 1, NOUNLOAD, REPLACE, STATS = 5,
          MOVE ''ContractRegistrationService'' TO ''F:\data\oltp\' + @source_db + '.mdf'',
		  MOVE ''ContractRegistrationService_log'' TO ''F:\log\oltp\' + @source_db + '_log.ldf''')
	exec('ALTER DATABASE [' + @source_db + '] SET MULTI_USER')
	print 'OK  Source database restored to ' + @source_db
END
else
BEGIN
    print '!!  Database not restored (due to setting)'
END


print ''
print '    1.2.   Restore staging database'
print '    -----------------------------------------------------------------------------------'

IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = @staging_db)
BEGIN
  IF @restore_staging_db = 0 
  BEGIN
    print '!!  Staging database did not exist; overriding restore setting to TRUE'
	set @restore_staging_db = 1
  END
END

IF @restore_staging_db = 1
BEGIN
	IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = @staging_db)
	BEGIN
		exec('CREATE DATABASE [' + @staging_db + ']')
	END
	exec('ALTER DATABASE [' + @staging_db + '] SET SINGLE_USER WITH ROLLBACK IMMEDIATE')
	exec('RESTORE DATABASE [' + @staging_db + '] 
		  FROM DISK = N''' + @staging_backup_file + ''' 
		  WITH FILE = 1, NOUNLOAD, REPLACE, STATS = 5,
          MOVE ''UnifiedRegisters'' TO ''F:\data\oltp\' + @staging_db + '.mdf'',
		  MOVE ''UnifiedRegisters_log'' TO ''F:\log\oltp\' + @staging_db + '_log.ldf''')
	exec('ALTER DATABASE [' + @staging_db + '] SET MULTI_USER')
	print 'OK  Source database restored to ' + @staging_db
	print @@error
END
else
BEGIN
    print '!!  Database not restored (due to setting)'
END





  

      
