# CIMCO ML Analytics

A FastAPI-based machine learning application for analyzing machine downtime data from CIMCO AS software. The system predicts potential machine issues, optimizes maintenance schedules, and identifies efficiency patterns.

## Features

- **Data Synchronization**: Connect to CIMCO MySQL database and sync data to analytics PostgreSQL database
- **Machine Learning**: Predictive models for maintenance timing, downtime forecasting, and anomaly detection
- **Real-time Analytics**: Live monitoring with WebSocket connections and OEE calculations
- **Reporting**: Automated report generation with PDF/Excel export capabilities
- **API-First**: RESTful APIs with OpenAPI documentation

## Quick Start

### Prerequisites

- Python 3.11+
- Docker and Docker Compose
- Access to CIMCO MySQL database

### Local Development

1. **Clone and setup**:
   ```bash
   git clone <repository-url>
   cd cimco-ml-analytics
   ```

2. **Create environment file**:
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Start services with Docker Compose**:
   ```bash
   docker-compose up -d
   ```

4. **Install dependencies** (for local development):
   ```bash
   pip install -r requirements.txt
   ```

5. **Run the application**:
   ```bash
   uvicorn app.main:app --reload
   ```

6. **Access the API**:
   - API Documentation: http://localhost:8000/docs
   - Health Check: http://localhost:8000/health

### Manual Setup

If you prefer to run without Docker:

1. **Install PostgreSQL and Redis**
2. **Create PostgreSQL database**:
   ```sql
   CREATE DATABASE cimco_analytics;
   ```
3. **Update .env file** with your database credentials
4. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
5. **Run database migrations** (when available):
   ```bash
   alembic upgrade head
   ```
6. **Start the application**:
   ```bash
   uvicorn app.main:app --reload
   ```

## Project Structure

```
cimco-ml-analytics/
├── app/
│   ├── api/v1/           # API endpoints
│   ├── core/             # Core configuration and database
│   ├── schemas/          # Pydantic models
│   └── main.py           # FastAPI application
├── tests/                # Test suite
├── models/               # ML model storage
├── alembic/              # Database migrations
├── docker-compose.yml    # Docker services
├── Dockerfile            # Application container
└── requirements.txt      # Python dependencies
```

## API Endpoints

### Data Management
- `GET /api/v1/data/sync-status` - Get synchronization status
- `POST /api/v1/data/sync` - Trigger manual sync

### Machine Learning
- `POST /api/v1/ml/predict/maintenance` - Predict maintenance needs
- `POST /api/v1/ml/predict/downtime` - Predict machine downtime
- `POST /api/v1/ml/train` - Trigger model training

### Analytics
- `GET /api/v1/analytics/oee-metrics` - Get OEE metrics
- `GET /api/v1/analytics/machines/{id}/efficiency` - Get machine efficiency

## Development

### Running Tests

```bash
pytest
```

### Code Quality

```bash
# Format code
black app/ tests/

# Lint code
flake8 app/ tests/
```

### Database Migrations

```bash
# Create migration
alembic revision --autogenerate -m "Description"

# Apply migrations
alembic upgrade head
```

## Configuration

Key configuration options in `.env`:

- **CIMCO_DB_***: CIMCO MySQL database connection
- **POSTGRES_***: Analytics PostgreSQL database
- **REDIS_***: Redis cache and message broker
- **SECRET_KEY**: JWT token signing key
- **MODEL_STORAGE_PATH**: ML model storage location

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

[Add your license information here]