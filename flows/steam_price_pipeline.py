# flows/steam_price_pipeline.py

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner
from datetime import datetime, timedelta
import asyncio
from datetime import time

from tasks.steam_tasks_improved import (
    crawl_price_task, 
    transform_price_task, 
    publish_to_kafka_task, 
    discord_notifier_task, 
    consume_prices_task
)
from monitoring.metrics import metrics
from config.settings import config


@flow(
    name="steam_price_pipeline_improved",
    task_runner=ConcurrentTaskRunner(),
    retries=2,
    retry_delay_seconds=60,
    log_prints=True
)
def steam_price_pipeline():
    pipeline_start = datetime.now()
    print(f"🚀 Starting improved pipeline at {pipeline_start.strftime('%H:%M:%S')}...")
    
    try:
        metrics.set_pipeline_status('main_pipeline', 1)
        
        # Phase 1: Data Collection & Transformation (Concurrent)
        print("📊 Phase 1: Data Collection & Transformation")
        crawl_future = crawl_price_task.submit()  
        transform_future = transform_price_task.submit() 
        publish_future = publish_to_kafka_task.submit() 
        
        # Wait for all to complete
        crawl_result = crawl_future.wait()
        transform_result = transform_future.wait()
        publish_result = publish_future.wait()
        
        if any(future.get_state().is_failed() for future in [crawl_future, transform_future, publish_future]):
            print("❌ One or more tasks failed, stopping pipeline")
            raise Exception("Pipeline task failed")
        
        # Phase 4: Data Processing (Long-running concurrent tasks)
        print("⚡ Phase 4: Real-time Processing")
        consume_future = consume_prices_task.submit()
        notifier_future = discord_notifier_task.submit()

        time.sleep(5)
        
        if consume_future.get_state().is_failed():
            print("❌ Consumer task failed to start")
            raise Exception("Consumer task failed to start")
            
        if config.discord.enable_notifications and notifier_future.get_state().is_failed():
            print("❌ Discord notifier task failed to start")
            raise Exception("Discord notifier task failed to start")
        
        print("✅ All tasks started successfully!")
        print("🔄 Long-running tasks (consumer, notifier) will continue in background")
        
        pipeline_duration = datetime.now() - pipeline_start
        print(f"⏱️  Main pipeline completed in {pipeline_duration.total_seconds():.2f} seconds")
        metrics.push_metrics("_pipeline_complete")
        
        return {
            'status': 'success',
            'duration': pipeline_duration.total_seconds(),
            'long_running_tasks': {
                'consumer': consume_future,
                'notifier': notifier_future
            }
        }
        
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        metrics.set_pipeline_status('main_pipeline', 0)
        metrics.push_metrics("_pipeline_failed")
        raise e
    finally:
        metrics.set_pipeline_status('main_pipeline', 0)

@flow(
    name="steam_price_monitoring_flow",
    task_runner=ConcurrentTaskRunner(),
    log_prints=True
)
def steam_price_monitoring_flow():
    print("🔍 Starting monitoring flow...")
    
    try:
        # Start only the long-running tasks
        consume_future = consume_prices_task.submit()
        notifier_future = discord_notifier_task.submit()
        
        print("📊 Monitoring tasks started successfully")
        print("Press Ctrl+C to stop monitoring...")
        
        try:
            consume_future.wait()
            notifier_future.wait()
        except KeyboardInterrupt:
            print("🛑 Monitoring stopped by user")
            
    except Exception as e:
        print(f"❌ Monitoring flow failed: {e}")
        raise e

@flow(
    name="steam_price_recovery_flow",
    task_runner=ConcurrentTaskRunner(),
    log_prints=True
)
def steam_price_recovery_flow():
    print("🔧 Starting recovery flow...")
    
    try:
        result = steam_price_pipeline()
        print("✅ Recovery completed successfully")
        return result
        
    except Exception as e:
        print(f"❌ Recovery failed: {e}")
        raise e

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        
        if mode == "monitor":
            print("Running in monitoring mode...")
            steam_price_monitoring_flow()
        elif mode == "recovery":
            print("Running in recovery mode...")
            steam_price_recovery_flow()
        else:
            print(f"Unknown mode: {mode}")
            print("Available modes: monitor, recovery")
            print("Running default pipeline...")
            steam_price_pipeline()
    else:
        steam_price_pipeline()