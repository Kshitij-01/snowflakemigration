# Build Docker Image for Snowflake Migration Pipeline
# Make sure Docker Desktop is running before executing this script

Write-Host "Building Snowflake Migration Docker image..." -ForegroundColor Cyan

# Build the image
docker build -t snowflake-migration:latest .

if ($LASTEXITCODE -eq 0) {
    Write-Host "`nDocker image built successfully!" -ForegroundColor Green
    Write-Host "Image name: snowflake-migration:latest" -ForegroundColor Yellow
    Write-Host "`nTo run the container:" -ForegroundColor Cyan
    Write-Host "  docker run -p 8000:8000 -e AZURE_OPENAI_API_KEY=your-key -e SNOWFLAKE_ACCOUNT=your-account snowflake-migration:latest" -ForegroundColor White
    Write-Host "`nOr use docker-compose:" -ForegroundColor Cyan
    Write-Host "  docker-compose up" -ForegroundColor White
} else {
    Write-Host "`nBuild failed. Make sure Docker Desktop is running." -ForegroundColor Red
    exit 1
}

