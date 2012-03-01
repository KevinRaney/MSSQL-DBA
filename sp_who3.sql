use master;
go

if object_id('sp_dbcc_inputbuffer', 'P') is not null
  drop procedure sp_dbcc_inputbuffer;
go
  
create procedure sp_dbcc_inputbuffer (@SPID int) as
begin
  set nocount on;
  set ansi_warnings off;

  dbcc inputbuffer(@SPID);
  
end
go

if object_id('sp_who3', 'P') is not null
  drop procedure sp_who3;
go

create procedure sp_who3 (@DatabaseName varchar(50) = null, 
                          @SPID int = null, 
                          @Login varchar(255) = null,
                          @SystemProcessesOff varchar(1) = 'Y',
                          @Summary varchar(1) = 'N',
                          @BlocksOnly varchar(1) = 'N',
                          @ProfileEveryXSeconds int = null,
                          @ProfileForXSeconds int = null) as
                          
/**************************************************************************************
  Purpose: To extend the functionality of the builtin sp_who2 Stored Procedure
  
  Usage:
    exec sp_who3 
    exec sp_who3 @Summary = 'Y'
    exec sp_who3 @DatabaseName = 'ITIS'
    exec sp_who3 @SPID = 14
    exec sp_who3 @Login = 'kraney'
    exec sp_who3 @blocksOnly = 'Y'
    exec sp_who3 @SystemProcessesOff = 'N'
    exec sp_who3 @ProfileEveryXSeconds = 5, @ProfileForXSeconds = 30
    exec sp_who3 @SPID = 138, @ProfileEveryXSeconds = 5, @ProfileForXSeconds = 30
    exec sp_who3 @Login = 'dbwriter', @ProfileEveryXSeconds = 5, @ProfileForXSeconds = 30

  Developer         Date        Notes
  ----------------  ----------  --------------------------------------------------------
  Kevin R. Raney    09/02/2011  Initial creation
  Kevin R. Raney    09/09/2011  Added the ability to profile sessions over a certain time
                                period.
  Kevin R. Raney    09/12/2011  Added SQL Text to the results.

  Requested Enhancements:
  - None at this time
       
**************************************************************************************/

                          
begin
  
  set nocount on;
  
  if (@ProfileEveryXSeconds is not null and @ProfileForXSeconds is null) or 
     (@ProfileEveryXSeconds is null and @ProfileForXSeconds is not null) 
  begin
    raiserror('Profile Paramaters are used incorrectly.',11,1);  
    return
  end
  
  if @ProfileForXSeconds > 86400
  begin
    raiserror('ProfileForXSeconds cannot exceed 1 day (86,400 seconds).',11,1);  
    return
  end
     
  if OBJECT_ID('tempdb..#spwho') > 0 
    drop table #spwho;

  create table #spwho (
   SPID int not null,
   Status varchar (255) not null,
   Login varchar (255) not null,
   HostName varchar (255) not null,
   BlkBy varchar(10) not null,
   DBName varchar (255) null,
   Command varchar (255) not null,
   CPUTime int not null,
   DiskIO int not null,
   LastBatch varchar (255) not null,
   ProgramName varchar (255) null,
   SPID2 int not null,
   REQUESTID int not null); -- this column needs to be commented for SQL Server 2000

  if OBJECT_ID('tempdb..#spwho1') > 0 
    drop table #spwho1;

  create table #spwho1 (
   spwhoID int identity(1,1) not null,
   SPID int not null,
   Status varchar (255) not null,
   Login varchar (255) not null,
   HostName varchar (255) not null,
   BlkBy varchar(10) not null,
   DBName varchar (255) null,
   Command varchar (255) not null,
   CPUTime int not null,
   DiskIO int not null,
   LastBatch varchar (255) not null,
   ProgramName varchar (255) null,
   SPID2 int not null,
   REQUESTID int not null, -- this column needs to be commented for SQL Server 2000
   CaptureDate datetime not null default getdate(),
   SQLText varchar(max));

  insert #spwho
    exec sp_who2;
 
  insert into #spwho1 (SPID, Status, Login, HostName, BlkBy, DBName, 
                       Command, CPUTime, DiskIO, LastBatch, ProgramName, 
                       SPID2, REQUESTID)
    select SPID, Status, Login, HostName, BlkBy, DBName, 
           Command, CPUTime, DiskIO, LastBatch, ProgramName, 
           SPID2, REQUESTID 
      from #spwho;

  
  create table #dbccbuffer (
    EventType varchar(50),
    Parameters int,
    EventInfo varchar(max));

  declare @spwhoID int,
          @spidlookup int,
          @SQLText varchar(max);
          
  declare curspwho cursor local for 
  select spwhoID, SPID
    from #spwho1
   where SQLText is null;
   
  open curspwho;
  
  fetch next from curspwho into @spwhoID, @spidlookup;
  
  while @@fetch_status = 0 
  begin
    truncate table #dbccbuffer;
    
    insert into #dbccbuffer    
    exec sp_dbcc_inputbuffer @spidlookup;
    
    select @SQLText = isnull(EventInfo,'Unknown')
      from #dbccbuffer;
    
    update #spwho1
       set SQLText = @SQLText
     where spwhoID = @spwhoID;
     
    fetch next from curspwho into @spwhoID, @spidlookup;
  end
  
  close curspwho;
  deallocate curspwho;
  
  if @ProfileEveryXSeconds is not null and @ProfileForXSeconds is not null 
  begin

    declare @StartTime datetime,
            @WaitforTime varchar(9)
    
    select @WaitforTime = cast(datepart(hour,dateadd(second,@ProfileEveryXSeconds,cast('1/1/2000' as datetime))) as varchar(10)) + ':' + 
                      right('0' + cast(datepart(minute,dateadd(second,@ProfileEveryXSeconds,cast('1/1/2000' as datetime))) as varchar(2)),2) + ':' +
                      right('0' + cast(datepart(second,dateadd(second,@ProfileEveryXSeconds,cast('1/1/2000' as datetime))) as varchar(2)),2)

    set @StartTime = getdate()
    while @ProfileForXSeconds >= datediff(second,@StartTime,getdate())
    begin

      truncate table #spwho;
      
      insert #spwho
        exec sp_who2;
     
      insert into #spwho1 (SPID, Status, Login, HostName, BlkBy, DBName, 
                           Command, CPUTime, DiskIO, LastBatch, ProgramName, 
                           SPID2, REQUESTID)
        select SPID, Status, Login, HostName, BlkBy, DBName, 
               Command, CPUTime, DiskIO, LastBatch, ProgramName, 
               SPID2, REQUESTID 
          from #spwho;

      declare curspwho cursor local for 
      select spwhoID, SPID
        from #spwho1
       where SQLText is null;

      open curspwho;
      
      fetch next from curspwho into @spwhoID, @spidlookup;
      
      while @@fetch_status = 0 
      begin
        truncate table #dbccbuffer;
        
        insert into #dbccbuffer    
        exec sp_dbcc_inputbuffer @spidlookup;
        
        select @SQLText = isnull(EventInfo,'Unknown')
          from #dbccbuffer;
        
        update #spwho1
           set SQLText = @SQLText
         where spwhoID = @spwhoID;
         
        fetch next from curspwho into @spwhoID, @spidlookup;
      end
      
      close curspwho;
      deallocate curspwho;

      waitfor delay @WaitForTime
      
    end
  end

  if isnull(@Summary,'N') != 'N'  
    select DBName, 
           Login, 
           count(Login) Sessions, 
           sum(case when BlkBy = '  .' then 0 else 1 end) TotalBlocks,
           sum(CPUTime) TotalCPUTime,
           sum(DiskIO) TotalDiskIO,
           max(LastBatch) LastBatch
      from #spwho1
     where SPID > 50
     group by DBName, 
              Login

  if isnull(@Summary,'N') = 'N'
    select *
    from #spwho1
    where (SPID > 50 and @SPID is null and @DatabaseName is null and @Login is null and @SystemProcessesOff = 'Y' and @BlocksOnly = 'N')
       or (isnull(@SystemProcessesOff,'Y') != 'Y' and @DatabaseName is null and @Login is null and @SPID is null and @BlocksOnly = 'N')
       or (DBName = @DatabaseName and @DatabaseName is not null and @Login is null and @SPID is null and @SystemProcessesOff = 'Y' and @BlocksOnly = 'N')
       or (SPID = @SPID and @SPID is not null and @DatabaseName is null and @Login is null and @SystemProcessesOff = 'Y' and @BlocksOnly = 'N')
       or (Login = @Login and @Login is not null and @DatabaseName is null and @SPID is null and @SystemProcessesOff = 'Y' and @BlocksOnly = 'N')
       or (@BlocksOnly = 'Y' and (BlkBy != '  .' or spid in (select distinct blkBy from #spwho1 where blkBy != '  .')))
    order by spid, CaptureDate desc

  if OBJECT_ID('tempdb..#spwho') is not null
    drop table #spwho

end  
