# Steam Price Pipeline - Version 1.0

## Overview

This is an improved, production-ready Steam price monitoring pipeline that crawls Steam and SteamSpy APIs, processes pricing data, and provides real-time notifications via Discord. The system is built with fault tolerance, monitoring, and scalability in mind.

## Key Improvements

### 🔧 **Fault Tolerance & Recovery**
- **Checkpoint-based resumption**: Pipeline automatically resumes from the last successful point
- **ProcessedTracker**: Tracks processed and failed items across restarts
- **Retry logic**: Configurable retry mechanisms with exponential backoff
- **Graceful error handling**: Failed items are marked and can be retried separately

### 📊 **Monitoring & Observability**
- **Prometheus metrics**: Comprehensive metrics collection for all pipeline components
- **Grafana dashboard**: Visual monitoring of pipeline performance and health
- **Real-time alerting**: Monitor success rates, error rates, and performance
- **Performance tracking**: API response times, task durations, queue sizes

### ⚙️ **Configuration Management**
- **Centralized config**: All settings in `config/settings.py`
- **Environment-based**: Easy deployment across different environments
- **Type-safe configuration**: Dataclass-based configuration with validation

### 🔄 **Improved Pipeline Flow**
- **Sequential execution**: Proper task dependencies (crawl → transform → distribute)
- **Concurrent long-running tasks**: Consumer and notifier run independently
- **Resource management**: Proper connection pooling and cleanup
- **Performance optimization**: Reduced polling intervals and batch processing

## Architecture

```
Steam API → Crawler → Transformer → Kafka → [Consumer → MySQL]
                                         ↘ [CDC → Discord Notifier]
```

## Installation & Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Environment Configuration

Create `config/.env`:

```env
# Database
MYSQL_HOST=localhost
MYSQL_PORT=3307
MYSQL_USER=root
MYSQL_PASSWORD=root
MYSQL_DB=steam_db

# Kafka
KAFKA_SERVER=localhost:9092

# Discord (optional)
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
ENABLE_DISCORD=true

# Monitoring
PROMETHEUS_GATEWAY=localhost:9091
ENABLE_METRICS=true
```

### 3. Start Monitoring Stack

```bash
# Start monitoring services
docker-compose -f docker-compose.monitoring.yml up -d

# Verify services
curl http://localhost:9091/metrics  # Pushgateway
curl http://localhost:9090/targets  # Prometheus
# Open http://localhost:3000 for Grafana (admin/admin)
```

### 4. Database Setup

Use init_db.sql 
## Usage

### Running the Pipeline

```bash
# Full pipeline (default)
python flows/steam_price_pipeline_improved.py

# Monitoring mode (long-running tasks only)
python flows/steam_price_pipeline_improved.py monitor

# Recovery mode (resume from failures)
python flows/steam_price_pipeline_improved.py recovery
```

### Using Prefect

```bash
# Start Prefect server
prefect server start

# Deploy flow
prefect deployment build flows/steam_price_pipeline_improved.py:steam_price_pipeline -n steam-pipeline -q default

# Run deployment
prefect deployment run steam-price-pipeline/steam-pipeline
```

## Monitoring & Alerting

### Grafana Dashboard

The provided Grafana dashboard includes:

- **Games Processed Rate**: Real-time processing throughput
- **Pipeline Status**: Current status of all pipeline components  
- **API Success Rate**: Success percentage of API calls
- **Response Time**: 95th percentile API response times
- **Kafka Metrics**: Message production/consumption rates
- **Task Duration**: Pipeline task execution times
- **Queue Sizes**: Pending and failed queue monitoring

### Key Metrics

- `steam_games_processed_total`: Total games processed by task and status
- `steam_api_requests_total`: API request counts by endpoint and status
- `steam_kafka_messages_total`: Kafka message counts by topic and operation
- `steam_task_duration_seconds`: Task execution time histograms
- `steam_pipeline_status`: Current pipeline status (1=running, 0=stopped)

### Alerting Rules

Create `monitoring/steam_rules.yml`:

```yaml
groups:
  - name: steam_pipeline
    rules:
      - alert: HighAPIFailureRate
        expr: rate(steam_api_requests_total{status="failed"}[5m]) / rate(steam_api_requests_total[5m]) > 0.1
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "High API failure rate detected"

      - alert: PipelineDown
        expr: steam_pipeline_status{pipeline="main_pipeline"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Steam pipeline is down"
```

## Fault Tolerance Features

### Checkpoint System

The pipeline uses a checkpoint system to track progress:

- **Crawl checkpoints**: Track which app IDs have been processed
- **Transform checkpoints**: Track which records have been transformed  
- **Automatic recovery**: Resume from last checkpoint on restart
- **Failure isolation**: Failed items are tracked separately for retry

### Recovery Scenarios

1. **API rate limiting**: Automatic backoff and retry
2. **Network timeouts**: Retry with exponential backoff
3. **Database connection loss**: Connection pooling with retry logic  
4. **Kafka unavailability**: Message buffering and retry
5. **Partial failures**: Continue processing other items, retry failed ones



## Performance Tuning



### Optimization Tips

1. **Increase workers** for faster API crawling (watch rate limits)
2. **Adjust batch sizes** for memory vs. performance trade-offs
3. **Tune polling intervals** based on data freshness requirements
4. **Monitor queue sizes** to identify bottlenecks

## Troubleshooting

### Common Issues

1. **MySQL connection errors**: Check connection parameters and network
2. **Kafka consumer lag**: Monitor consumer group lag in Grafana
3. **API rate limiting**: Reduce `max_workers` or increase `sleep_between_batches`
4. **Memory issues**: Reduce batch sizes and worker counts
5. **Checkpoint corruption**: Clear checkpoint files in `data/checkpoints/`

### Debug Commands

```bash
# Check pipeline status
curl http://localhost:9091/metrics | grep steam_pipeline_status

# View current queues
curl http://localhost:9091/metrics | grep steam_queue_size  

# Clear checkpoints (force full restart)
rm -rf data/checkpoints/*
```

## Contributing

1. Follow the existing code structure
2. Add monitoring for new features
3. Include fault tolerance mechanisms
4. Update documentation and dashboards
5. Test recovery scenarios

## License

MIT License