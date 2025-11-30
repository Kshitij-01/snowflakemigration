# Azure Deployment Script for Snowflake Migration Pipeline
# Prerequisites: Azure CLI installed and logged in (az login)

param(
    [string]$ResourceGroup = "sqltosnowflake",
    [string]$Location = "eastus2",
    [string]$AcrName = "sqltosnowflakeacr",
    [string]$AppName = "snowflake-migration",
    [string]$AppServicePlan = "snowflake-migration-plan"
)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Snowflake Migration Pipeline - Azure Deployment" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Check if logged in to Azure
Write-Host "`n[1/8] Checking Azure CLI login..." -ForegroundColor Yellow
$account = az account show 2>$null | ConvertFrom-Json
if (-not $account) {
    Write-Host "Not logged in. Running 'az login'..." -ForegroundColor Red
    az login
}
Write-Host "Logged in as: $($account.user.name)" -ForegroundColor Green

# Check if resource group exists
Write-Host "`n[2/8] Checking resource group '$ResourceGroup'..." -ForegroundColor Yellow
$rgExists = az group exists --name $ResourceGroup
if ($rgExists -eq "false") {
    Write-Host "Creating resource group '$ResourceGroup' in '$Location'..." -ForegroundColor Yellow
    az group create --name $ResourceGroup --location $Location
}
Write-Host "Resource group ready." -ForegroundColor Green

# Create Azure Container Registry
Write-Host "`n[3/8] Creating Azure Container Registry '$AcrName'..." -ForegroundColor Yellow
$acrExists = az acr show --name $AcrName --resource-group $ResourceGroup 2>$null
if (-not $acrExists) {
    az acr create --resource-group $ResourceGroup --name $AcrName --sku Basic --admin-enabled true
}
Write-Host "ACR ready." -ForegroundColor Green

# Get ACR credentials
Write-Host "`n[4/8] Getting ACR credentials..." -ForegroundColor Yellow
$acrCreds = az acr credential show --name $AcrName | ConvertFrom-Json
$acrServer = "$AcrName.azurecr.io"
$acrUsername = $acrCreds.username
$acrPassword = $acrCreds.passwords[0].value

# Login to ACR
Write-Host "`n[5/8] Logging in to ACR..." -ForegroundColor Yellow
az acr login --name $AcrName

# Build and push Docker image
Write-Host "`n[6/8] Building and pushing Docker image..." -ForegroundColor Yellow
$imageTag = "$acrServer/${AppName}:latest"
docker build -t $imageTag .
docker push $imageTag
Write-Host "Image pushed: $imageTag" -ForegroundColor Green

# Create App Service Plan
Write-Host "`n[7/8] Creating App Service Plan..." -ForegroundColor Yellow
$planExists = az appservice plan show --name $AppServicePlan --resource-group $ResourceGroup 2>$null
if (-not $planExists) {
    az appservice plan create `
        --name $AppServicePlan `
        --resource-group $ResourceGroup `
        --sku B2 `
        --is-linux
}
Write-Host "App Service Plan ready." -ForegroundColor Green

# Create Web App
Write-Host "`n[8/8] Creating Web App with container..." -ForegroundColor Yellow
$webAppExists = az webapp show --name $AppName --resource-group $ResourceGroup 2>$null
if (-not $webAppExists) {
    az webapp create `
        --resource-group $ResourceGroup `
        --plan $AppServicePlan `
        --name $AppName `
        --deployment-container-image-name $imageTag
}

# Configure container registry
az webapp config container set `
    --name $AppName `
    --resource-group $ResourceGroup `
    --docker-custom-image-name $imageTag `
    --docker-registry-server-url "https://$acrServer" `
    --docker-registry-server-user $acrUsername `
    --docker-registry-server-password $acrPassword

# Read credentials from file and set as environment variables
Write-Host "`nConfiguring environment variables..." -ForegroundColor Yellow
$credsFile = Join-Path $PSScriptRoot "credentials.txt"
if (Test-Path $credsFile) {
    $creds = @{}
    Get-Content $credsFile | ForEach-Object {
        if ($_ -match "^([^=]+)=(.*)$") {
            $creds[$matches[1].Trim()] = $matches[2].Trim()
        }
    }
    
    az webapp config appsettings set `
        --name $AppName `
        --resource-group $ResourceGroup `
        --settings `
            AZURE_OPENAI_API_KEY="$($creds['AZURE_OPENAI_API_KEY'])" `
            AZURE_OPENAI_ENDPOINT="$($creds['AZURE_OPENAI_ENDPOINT'])" `
            SNOWFLAKE_ACCOUNT="$($creds['SNOWFLAKE_ACCOUNT'])" `
            SNOWFLAKE_USER="$($creds['SNOWFLAKE_USER'])" `
            SNOWFLAKE_PASSWORD="$($creds['SNOWFLAKE_PASSWORD'])" `
            SNOWFLAKE_WAREHOUSE="$($creds['SNOWFLAKE_WAREHOUSE'])" `
            SNOWFLAKE_DATABASE="$($creds['SNOWFLAKE_DATABASE'])" `
            WEBSITES_PORT=8000
} else {
    Write-Host "WARNING: credentials.txt not found. Set environment variables manually." -ForegroundColor Red
    az webapp config appsettings set `
        --name $AppName `
        --resource-group $ResourceGroup `
        --settings WEBSITES_PORT=8000
}

# Enable HTTPS only
Write-Host "`nEnabling HTTPS only..." -ForegroundColor Yellow
az webapp update --name $AppName --resource-group $ResourceGroup --https-only true

# Get the URL
$webAppUrl = "https://$AppName.azurewebsites.net"

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Deployment Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "`nYour app is available at:" -ForegroundColor White
Write-Host "$webAppUrl" -ForegroundColor Green
Write-Host "`nAPI docs at:" -ForegroundColor White
Write-Host "$webAppUrl/docs" -ForegroundColor Green
Write-Host "`nNote: It may take a few minutes for the container to start." -ForegroundColor Yellow

