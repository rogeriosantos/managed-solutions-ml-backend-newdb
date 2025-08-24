@echo off
REM Batch script to sync all CIMCO data to analytics database
REM This will sync records in batches of 5000

echo Starting full data sync...
echo.

set BATCH_SIZE=5000
set BATCH_COUNT=0
set TOTAL_PROCESSED=0

:SYNC_LOOP
set /a BATCH_COUNT+=1
echo Processing batch %BATCH_COUNT%...

REM Make the API call
curl -s -X POST "http://localhost:8000/api/v1/data/sync/jobs?limit=%BATCH_SIZE%&incremental=false"

echo.
echo Batch %BATCH_COUNT% completed. Waiting 3 seconds...
timeout /t 3 /nobreak >nul

REM Continue for up to 25 batches (125k records)
if %BATCH_COUNT% LSS 25 goto SYNC_LOOP

echo.
echo Sync completed! Processed %BATCH_COUNT% batches.
echo.
echo Checking final table counts...
curl -s "http://localhost:8000/api/v1/data/postgres/tables/counts"
echo.
echo.
echo Sync script finished!
pause