from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from kafka import KafkaProducer, KafkaConsumer
import json
import time
from datetime import datetime
import pandas as pd
import os
import atexit


from config.settings import config
from tasks.steam_tasks_improved import consume_prices_task, discord_notifier_task
from utils.common import get_date_string, ensure_directory
from monitoring.metrics import metrics

@task(retries=3, retry_delay_seconds=5, log_prints=True)
def mock_publish_to_kafka_task():
    """Mock task to publish sample data to Kafka topic steam-dynamic-price"""
    producer = KafkaProducer(bootstrap_servers=config.kafka.bootstrap_servers)
    sample_data = [
        {
            "type": "price",
            "appid": 12345,
            "discount": 10,
            "price": 500,
            "initial_price": 555.56,
            "timestamp": datetime.now().timestamp()
        }
    ]
    
    for data in sample_data:
        producer.send(config.kafka.steam_dynamic_topic, json.dumps(data).encode('utf-8'))
    producer.flush()
    print(f"✅ Mocked {len(sample_data)} messages published to {config.kafka.steam_dynamic_topic}")
    time.sleep(2)

@task(retries=3, retry_delay_seconds=5, log_prints=True)
def mock_cdc_publish_task():
    """Mock task to publish a single CDC data to mysql_cdc_topic"""
    producer = KafkaProducer(bootstrap_servers=config.kafka.bootstrap_servers)
    sample_cdc = {
        "payload": {
            "before": {"game_id": 12345, "discount": 10, "price": 500, "initial_price": 555.56},
            "after": {"game_id": 12345, "discount": 15, "price": 475, "initial_price": 558.82, "timestamp": datetime.now().timestamp() * 1000}
        }
    }
    

    producer.send(config.kafka.mysql_cdc_topic, json.dumps(sample_cdc).encode('utf-8'))
    producer.flush()
    print(f"✅ Mocked 1 CDC message published to {config.kafka.mysql_cdc_topic}")
    time.sleep(2)

@flow(
    name="test_steam_pipeline",
    task_runner=ConcurrentTaskRunner(),
    retries=2,
    retry_delay_seconds=10,
    log_prints=True
)
def test_steam_pipeline():
    """Test flow to run consume and discord notifier tasks with controlled duration"""
    pipeline_start = datetime.now()
    print(f"🚀 Starting test pipeline at {pipeline_start.strftime('%H:%M:%S')}...")
    
    try:
        metrics.set_pipeline_status('test_pipeline', 1)
        
        # Submit mock and consume tasks
        mock_publish_future = mock_publish_to_kafka_task.submit()
        mock_cdc_future = mock_cdc_publish_task.submit()
        consume_future = consume_prices_task.submit()
        
        # Run notifier in background
        notifier_process = discord_notifier_task.submit()
        
        # Wait for mock and consume tasks
        mock_publish_result = mock_publish_future.wait()
        mock_cdc_result = mock_cdc_future.wait()
        consume_result = consume_future.wait()
        
        if any(future.get_state().is_failed() for future in [mock_publish_future, mock_cdc_future, consume_future]):
            print("❌ One or more tasks failed, stopping pipeline")
            raise Exception("Pipeline task failed")
        
        print("✅ Mock and consume tasks completed successfully!")
        

        print("⏳ Letting discord notifier run for 20 seconds to process CDC...")
        time.sleep(20) 
        
        pipeline_duration = datetime.now() - pipeline_start
        print(f"⏱️ Test pipeline completed in {pipeline_duration.total_seconds():.2f} seconds")
        metrics.push_metrics("_test_pipeline")
        
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        metrics.set_pipeline_status('test_pipeline', 0)
        metrics.push_metrics("_test_pipeline_failed")
        raise e
    finally:
        metrics.set_pipeline_status('test_pipeline', 0)

if __name__ == "__main__":
    test_steam_pipeline()