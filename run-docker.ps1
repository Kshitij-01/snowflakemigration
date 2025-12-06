# Run Docker Container for Snowflake Migration Pipeline
# This script runs the container with environment variables from credentials.txt

Write-Host "Loading credentials from credentials.txt..." -ForegroundColor Cyan

if (-not (Test-Path "credentials.txt")) {
    Write-Host "ERROR: credentials.txt not found!" -ForegroundColor Red
    Write-Host "Please create credentials.txt from credentials.example.txt" -ForegroundColor Yellow
    exit 1
}

# Load credentials
$creds = @{}
Get-Content credentials.txt | ForEach-Object {
    if ($_ -match "^([^=]+)=(.*)$") {
        $creds[$matches[1].Trim()] = $matches[2].Trim()
    }
}

Write-Host "Starting Docker container..." -ForegroundColor Cyan

# Build environment variables string
$envVars = @()
$envVars += "-e AZURE_OPENAI_API_KEY=$($creds['AZURE_OPENAI_API_KEY'])"
$envVars += "-e AZURE_OPENAI_ENDPOINT=$($creds['AZURE_OPENAI_ENDPOINT'])"
$envVars += "-e AZURE_OPENAI_API_VERSION=$($creds['AZURE_OPENAI_API_VERSION'])"
$envVars += "-e SNOWFLAKE_ACCOUNT=$($creds['SNOWFLAKE_ACCOUNT'])"
$envVars += "-e SNOWFLAKE_USER=$($creds['SNOWFLAKE_USER'])"
$envVars += "-e SNOWFLAKE_PASSWORD=$($creds['SNOWFLAKE_PASSWORD'])"
$envVars += "-e SNOWFLAKE_WAREHOUSE=$($creds['SNOWFLAKE_WAREHOUSE'])"
$envVars += "-e SNOWFLAKE_DATABASE=$($creds['SNOWFLAKE_DATABASE'])"

# Mount output directory
$outputPath = Join-Path $PSScriptRoot "output"
if (-not (Test-Path $outputPath)) {
    New-Item -ItemType Directory -Path $outputPath | Out-Null
}

# Run container
$envVarsString = $envVars -join " "
docker run -d `
    --name snowflake-migration `
    -p 8000:8000 `
    -v "${outputPath}:/app/output" `
    $envVarsString `
    snowflake-migration:latest

if ($LASTEXITCODE -eq 0) {
    Write-Host "`nContainer started successfully!" -ForegroundColor Green
    Write-Host "Application is available at: http://localhost:8000" -ForegroundColor Yellow
    Write-Host "`nTo view logs: docker logs -f snowflake-migration" -ForegroundColor Cyan
    Write-Host "To stop: docker stop snowflake-migration" -ForegroundColor Cyan
    Write-Host "To remove: docker rm snowflake-migration" -ForegroundColor Cyan
} else {
    Write-Host "`nFailed to start container." -ForegroundColor Red
    exit 1
}

