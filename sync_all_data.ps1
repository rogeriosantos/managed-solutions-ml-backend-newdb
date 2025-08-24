# PowerShell script to sync all CIMCO data to analytics database
# This script will sync 100k records in batches of 5000

$baseUrl = "http://localhost:8000/api/v1/data"
$batchSize = 5000
$totalRecords = 100000
$batches = [math]::Ceiling($totalRecords / $batchSize)

Write-Host "Starting full data sync..." -ForegroundColor Green
Write-Host "Total estimated records: $totalRecords" -ForegroundColor Yellow
Write-Host "Batch size: $batchSize" -ForegroundColor Yellow
Write-Host "Estimated batches: $batches" -ForegroundColor Yellow
Write-Host ""

# Initialize counters
$totalProcessed = 0
$totalInserted = 0
$totalUpdated = 0
$totalFailed = 0
$batchCount = 0

# Function to make API call and parse response
function Invoke-SyncBatch {
    param($BatchNumber)
    
    Write-Host "Processing batch $BatchNumber..." -ForegroundColor Cyan
    
    try {
        $response = Invoke-RestMethod -Uri "$baseUrl/sync/jobs?limit=$batchSize&incremental=false" -Method POST
        
        if ($response.status -eq "success") {
            Write-Host "  ✓ Processed: $($response.jobs_processed)" -ForegroundColor Green
            Write-Host "  ✓ Inserted: $($response.jobs_inserted)" -ForegroundColor Green
            Write-Host "  ✓ Updated: $($response.jobs_updated)" -ForegroundColor Green
            Write-Host "  ✓ Failed: $($response.jobs_failed)" -ForegroundColor $(if ($response.jobs_failed -gt 0) { "Red" } else { "Green" })
            Write-Host "  ✓ Sync ID: $($response.sync_id)" -ForegroundColor Gray
            
            return @{
                processed = $response.jobs_processed
                inserted = $response.jobs_inserted
                updated = $response.jobs_updated
                failed = $response.jobs_failed
                success = $true
            }
        } else {
            Write-Host "  ✗ Batch failed: $($response.error)" -ForegroundColor Red
            return @{ success = $false }
        }
    }
    catch {
        Write-Host "  ✗ Error calling API: $($_.Exception.Message)" -ForegroundColor Red
        return @{ success = $false }
    }
}

# Main sync loop
Write-Host "Starting sync batches..." -ForegroundColor Green
Write-Host "Press Ctrl+C to stop at any time" -ForegroundColor Yellow
Write-Host ""

$startTime = Get-Date

do {
    $batchCount++
    $result = Invoke-SyncBatch -BatchNumber $batchCount
    
    if ($result.success) {
        $totalProcessed += $result.processed
        $totalInserted += $result.inserted
        $totalUpdated += $result.updated
        $totalFailed += $result.failed
        
        # If we processed fewer records than batch size, we're done
        if ($result.processed -lt $batchSize) {
            Write-Host ""
            Write-Host "Reached end of data (processed $($result.processed) < $batchSize)" -ForegroundColor Yellow
            break
        }
    } else {
        Write-Host "Batch failed, stopping sync" -ForegroundColor Red
        break
    }
    
    # Progress update every 5 batches
    if ($batchCount % 5 -eq 0) {
        $elapsed = (Get-Date) - $startTime
        $recordsPerSecond = [math]::Round($totalProcessed / $elapsed.TotalSeconds, 2)
        Write-Host ""
        Write-Host "=== Progress Update ===" -ForegroundColor Magenta
        Write-Host "Batches completed: $batchCount" -ForegroundColor White
        Write-Host "Total processed: $totalProcessed" -ForegroundColor White
        Write-Host "Total inserted: $totalInserted" -ForegroundColor White
        Write-Host "Total updated: $totalUpdated" -ForegroundColor White
        Write-Host "Total failed: $totalFailed" -ForegroundColor White
        Write-Host "Records/second: $recordsPerSecond" -ForegroundColor White
        Write-Host "Elapsed time: $($elapsed.ToString('hh\:mm\:ss'))" -ForegroundColor White
        Write-Host ""
    }
    
    # Small delay between batches to avoid overwhelming the server
    Start-Sleep -Seconds 2
    
} while ($totalProcessed -lt $totalRecords -and $batchCount -lt 50) # Safety limit of 50 batches

# Final summary
$endTime = Get-Date
$totalTime = $endTime - $startTime

Write-Host ""
Write-Host "=== SYNC COMPLETE ===" -ForegroundColor Green
Write-Host "Total batches: $batchCount" -ForegroundColor White
Write-Host "Total processed: $totalProcessed" -ForegroundColor White
Write-Host "Total inserted: $totalInserted" -ForegroundColor White
Write-Host "Total updated: $totalUpdated" -ForegroundColor White
Write-Host "Total failed: $totalFailed" -ForegroundColor White
Write-Host "Total time: $($totalTime.ToString('hh\:mm\:ss'))" -ForegroundColor White
Write-Host "Average records/second: $([math]::Round($totalProcessed / $totalTime.TotalSeconds, 2))" -ForegroundColor White

# Check final table counts
Write-Host ""
Write-Host "Checking final table counts..." -ForegroundColor Cyan
try {
    $counts = Invoke-RestMethod -Uri "$baseUrl/postgres/tables/counts" -Method GET
    Write-Host "Job records in database: $($counts.table_counts.job_records)" -ForegroundColor Green
}
catch {
    Write-Host "Could not retrieve table counts: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""
Write-Host "Sync script completed!" -ForegroundColor Green