# Docker Build and Run Instructions

## Prerequisites

1. **Docker Desktop** must be installed and running
   - Download from: https://www.docker.com/products/docker-desktop
   - Make sure Docker Desktop is started before building/running

2. **credentials.txt** file must exist (copy from `credentials.example.txt`)

## Quick Start

### Option 1: Using PowerShell Scripts (Windows)

1. **Build the Docker image:**
   ```powershell
   .\build-docker.ps1
   ```

2. **Run the container:**
   ```powershell
   .\run-docker.ps1
   ```

### Option 2: Manual Docker Commands

1. **Build the image:**
   ```bash
   docker build -t snowflake-migration:latest .
   ```

2. **Run the container:**
   ```bash
   docker run -d \
     --name snowflake-migration \
     -p 8000:8000 \
     -e AZURE_OPENAI_API_KEY=your-key \
     -e AZURE_OPENAI_ENDPOINT=your-endpoint \
     -e SNOWFLAKE_ACCOUNT=your-account \
     -e SNOWFLAKE_USER=your-user \
     -e SNOWFLAKE_PASSWORD=your-password \
     -e SNOWFLAKE_WAREHOUSE=COMPUTE_WH \
     -e SNOWFLAKE_DATABASE=MIGRATION_DB \
     -v ./output:/app/output \
     snowflake-migration:latest
   ```

### Option 3: Using Docker Compose

1. **Set environment variables** in your shell or `.env` file:
   ```bash
   export AZURE_OPENAI_API_KEY=your-key
   export AZURE_OPENAI_ENDPOINT=your-endpoint
   export SNOWFLAKE_ACCOUNT=your-account
   export SNOWFLAKE_USER=your-user
   export SNOWFLAKE_PASSWORD=your-password
   export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
   export SNOWFLAKE_DATABASE=MIGRATION_DB
   ```

2. **Start services:**
   ```bash
   docker-compose up -d
   ```

## Access the Application

Once the container is running, access the web UI at:
- **Frontend:** http://localhost:8000
- **API Docs:** http://localhost:8000/docs
- **Health Check:** http://localhost:8000/api/health

## Useful Docker Commands

### View Logs
```bash
docker logs -f snowflake-migration
```

### Stop Container
```bash
docker stop snowflake-migration
```

### Start Container (if stopped)
```bash
docker start snowflake-migration
```

### Remove Container
```bash
docker rm snowflake-migration
```

### Remove Image
```bash
docker rmi snowflake-migration:latest
```

### Execute Commands in Container
```bash
docker exec -it snowflake-migration bash
```

## Troubleshooting

### Docker Desktop Not Running
- Error: `The system cannot find the file specified`
- Solution: Start Docker Desktop application

### Port Already in Use
- Error: `port is already allocated`
- Solution: Stop the existing container or use a different port:
  ```bash
  docker run -p 8001:8000 ...
  ```

### Credentials Not Found
- Error: `AZURE_OPENAI_API_KEY not found`
- Solution: Make sure `credentials.txt` exists or set environment variables

### Build Fails
- Check that all files are present (requirements.txt, etc.)
- Check Docker Desktop is running
- Check disk space

## Notes

- The `output/` directory is mounted as a volume, so migration results persist on your host machine
- Credentials can be provided via environment variables (recommended for production) or `credentials.txt` file
- The container runs the FastAPI server which serves both the API and frontend static files

