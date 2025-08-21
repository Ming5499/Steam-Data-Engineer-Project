# monitoring/metrics.py
from prometheus_client import CollectorRegistry, Counter, Histogram, Gauge, push_to_gateway
from prometheus_client.exposition import basic_auth_handler
import time
from contextlib import contextmanager
from typing import Optional
from config.settings import config

class MetricsCollector:
    def __init__(self):
        self.registry = CollectorRegistry()
        self.enabled = config.monitoring.enable_metrics
        
        if not self.enabled:
            return
        
        # Counters
        self.games_processed = Counter(
            'steam_games_processed_total',
            'Total number of games processed',
            ['task', 'status'],  # status: success, failed
            registry=self.registry
        )
        
        self.api_requests = Counter(
            'steam_api_requests_total',
            'Total number of API requests',
            ['api', 'status'],  # api: steam, steamspy, status: success, failed, timeout
            registry=self.registry
        )
        
        self.kafka_messages = Counter(
            'steam_kafka_messages_total',
            'Total number of Kafka messages',
            ['topic', 'operation'],  # operation: produced, consumed
            registry=self.registry
        )
        
        self.discord_notifications = Counter(
            'steam_discord_notifications_total',
            'Total number of Discord notifications',
            ['status'],  # status: success, failed
            registry=self.registry
        )
        
        # Histograms
        self.task_duration = Histogram(
            'steam_task_duration_seconds',
            'Task execution duration in seconds',
            ['task_name'],
            registry=self.registry
        )
        
        self.api_response_time = Histogram(
            'steam_api_response_time_seconds',
            'API response time in seconds',
            ['api'],
            registry=self.registry
        )
        
        # Gauges
        self.active_workers = Gauge(
            'steam_active_workers',
            'Number of active worker threads',
            registry=self.registry
        )
        
        self.queue_size = Gauge(
            'steam_queue_size',
            'Current queue size',
            ['queue_type'],  # queue_type: pending, failed
            registry=self.registry
        )
        
        self.pipeline_status = Gauge(
            'steam_pipeline_status',
            'Pipeline status (1=running, 0=stopped)',
            ['pipeline'],
            registry=self.registry
        )
    
    def increment_games_processed(self, task: str, status: str = 'success'):
        """Increment games processed counter"""
        if self.enabled:
            self.games_processed.labels(task=task, status=status).inc()
    
    def increment_api_requests(self, api: str, status: str = 'success'):
        """Increment API requests counter"""
        if self.enabled:
            self.api_requests.labels(api=api, status=status).inc()
    
    def increment_kafka_messages(self, topic: str, operation: str):
        """Increment Kafka messages counter"""
        if self.enabled:
            self.kafka_messages.labels(topic=topic, operation=operation).inc()
    
    def increment_discord_notifications(self, status: str = 'success'):
        """Increment Discord notifications counter"""
        if self.enabled:
            self.discord_notifications.labels(status=status).inc()
    
    def set_active_workers(self, count: int):
        """Set active workers gauge"""
        if self.enabled:
            self.active_workers.set(count)
    
    def set_queue_size(self, queue_type: str, size: int):
        """Set queue size gauge"""
        if self.enabled:
            self.queue_size.labels(queue_type=queue_type).set(size)
    
    def set_pipeline_status(self, pipeline: str, status: int):
        """Set pipeline status (1=running, 0=stopped)"""
        if self.enabled:
            self.pipeline_status.labels(pipeline=pipeline).set(status)
    
    @contextmanager
    def time_task(self, task_name: str):
        """Context manager to time task execution"""
        if not self.enabled:
            yield
            return
            
        start_time = time.time()
        try:
            yield
        finally:
            duration = time.time() - start_time
            self.task_duration.labels(task_name=task_name).observe(duration)
    
    @contextmanager
    def time_api_request(self, api: str):
        """Context manager to time API requests"""
        if not self.enabled:
            yield
            return
            
        start_time = time.time()
        try:
            yield
        finally:
            duration = time.time() - start_time
            self.api_response_time.labels(api=api).observe(duration)
    
    def push_metrics(self, job_suffix: str = ""):
        """Push metrics to Prometheus pushgateway"""
        if not self.enabled:
            return
            
        job_name = f"{config.monitoring.job_name}{job_suffix}"
        try:
            push_to_gateway(
                config.monitoring.prometheus_gateway,
                job=job_name,
                registry=self.registry
            )
            print(f"Metrics pushed to gateway for job: {job_name}")
        except Exception as e:
            print(f"Failed to push metrics: {e}")


metrics = MetricsCollector()


def track_task_performance(task_name: str):
    """Decorator to track task performance"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            with metrics.time_task(task_name):
                try:
                    result = func(*args, **kwargs)
                    metrics.increment_games_processed(task_name, 'success')
                    return result
                except Exception as e:
                    metrics.increment_games_processed(task_name, 'failed')
                    raise e
        return wrapper
    return decorator

def track_api_request(api_name: str):
    """Decorator to track API requests"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            with metrics.time_api_request(api_name):
                try:
                    result = func(*args, **kwargs)
                    metrics.increment_api_requests(api_name, 'success')
                    return result
                except Exception as e:
                    metrics.increment_api_requests(api_name, 'failed')
                    raise e
        return wrapper
    return decorator