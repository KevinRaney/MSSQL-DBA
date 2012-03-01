
/*
-- To allow advanced options to be changed.
EXEC sp_configure 'show advanced options', 1
GO
-- To update the currently configured value for advanced options.
RECONFIGURE
GO
-- To enable the feature.
EXEC sp_configure 'xp_cmdshell', 1
GO
-- To update the currently configured value for this feature.
RECONFIGURE
GO
*/

-- exec sp_GetDiskInfo

if object_id('sp_GetDiskInfo','P') is not null
  drop procedure sp_GetDiskInfo;
go

create procedure sp_GetDiskInfo as 
begin
  set nocount on;

  --SSIS Return definition
  if 1 = 2
    select cast(null as varchar(10)) DriveLetter,
           cast(null as bigint) FreeSpace,
           cast(null as bigint) TotalSpace,
           cast(null as bigint) AvailableSpace
     where 1 = 2

  declare @varSQL varchar(1000), @varDrive varchar(10)

  create table #tmpDriveSpaceInfo (
    DriveLetter varchar(10),
    xpFixedDrive_FreeSpace_MB bigint,
    FSutil_FreeSpace_Bytes integer,
    FSutil_Space_Bytes integer,
    FSutil_AvailSpace_Bytes integer);

  create table #tmpFSutilDriveSpaceInfo (
    DriveLetter varchar(10),
    info varchar(50));

  insert into #tmpDriveSpaceInfo (DriveLetter, xpFixedDrive_FreeSpace_MB)
  exec master..xp_fixeddrives

  declare CUR_DriveLooper cursor local for 
   select DriveLetter 
     from #tmpDriveSpaceInfo

  open CUR_DriveLooper
  
  fetch next from CUR_DriveLooper into @varDrive
  while @@fetch_status = 0
  begin
    set @varSQL = 'exec master..XP_CMDSHELL ' + ''''+ 'fsutil volume diskfree ' + @varDrive + ':' + ''''

    insert into #tmpFSutilDriveSpaceInfo (info)
           exec(@varSQL)

    update #tmpFSutilDriveSpaceInfo set DriveLetter = @varDrive where DriveLetter IS NULL

    fetch next from CUR_DriveLooper into @varDrive
  end

  delete from #tmpFSutilDriveSpaceInfo where info IS NULL

  select DriveLetter,
         ltrim(rtrim(left(info,29))) as InfoType,
         ltrim(rtrim(substring (info, charindex (':',info) + 2, 20))) as Size_Bytes
    into #tmpFSutilDriveSpaceInfo_Fixed
    from #tmpFSutilDriveSpaceInfo;

  select a.DriveLetter,
--  a.xpFixedDrive_FreeSpace_MB,
  (select cast(Size_Bytes as bigint) from #tmpFSutilDriveSpaceInfo_Fixed where DriveLetter = a.DriveLetter and InfoType = 'Total # of free bytes') as FreeSpace,
  (select cast(Size_Bytes as bigint) from #tmpFSutilDriveSpaceInfo_Fixed where DriveLetter = a.DriveLetter and InfoType = 'Total # of bytes') as TotalSpace,
  (select cast(Size_Bytes as bigint) from #tmpFSutilDriveSpaceInfo_Fixed where DriveLetter = a.DriveLetter and InfoType = 'Total # of avail free bytes') as AvailableSpace
  from #tmpDriveSpaceInfo a


  CLOSE CUR_DriveLooper
  DEALLOCATE CUR_DriveLooper
  DROP TABLE #tmpFSutilDriveSpaceInfo
  DROP TABLE #tmpDriveSpaceInfo
  DROP TABLE #tmpFSutilDriveSpaceInfo_Fixed

end
  