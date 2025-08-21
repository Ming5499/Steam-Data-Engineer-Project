# flows/steam_review_pipeline.py

import sys
import os
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner
from datetime import datetime

from tasks.steam_review_task_improved import (
    crawl_reviews_task,
    load_and_publish_reviews_task_watch,
    consume_reviews_task
)

from monitoring.metrics import metrics
from config.settings import config

@flow(
    name="steam_review_pipeline",
    task_runner=ConcurrentTaskRunner(),
    retries=2,
    retry_delay_seconds=60,
    log_prints=True
)


def steam_review_pipeline():
    pipeline_start = datetime.now()
    print(f"🚀 Starting Steam review pipeline at {pipeline_start.strftime('%H:%M:%S')}...")

    try:
        metrics.set_pipeline_status('review_pipeline', 1)

        # Phase 1 + 2 parallel
        print("🤝 Crawl reviews + Watcher (MongoDB insert & Kafka publish)")
        crawl_future = crawl_reviews_task.submit()
        watcher_future = load_and_publish_reviews_task_watch.submit()

        crawl_future.wait()
        if crawl_future.state.is_failed():
            print("❌ Crawl failed, cancelling watcher")
            watcher_future.cancel()
            raise Exception("Review crawl task failed")

        watcher_future.wait()
        if watcher_future.state.is_failed():
            print("❌ Watcher failed")
            raise Exception("Watcher (Mongo+Kafka) task failed")

        print("✅ Crawl + Watcher completed successfully!")

        pipeline_duration = datetime.now() - pipeline_start
        print(f"⏱️ Pipeline completed in {pipeline_duration.total_seconds():.2f} seconds")

        metrics.push_metrics("_review_pipeline_complete")

        return {
            'status': 'success',
            'duration': pipeline_duration.total_seconds(),
        }

    except Exception as e:
        print(f"❌ Review pipeline failed: {e}")
        metrics.set_pipeline_status('review_pipeline', 0)
        metrics.push_metrics("_review_pipeline_failed")
        raise e
    finally:
        metrics.set_pipeline_status('review_pipeline', 0)


@flow(
    name="steam_review_monitoring_flow",
    task_runner=ConcurrentTaskRunner(),
    log_prints=True
)
def steam_review_monitoring_flow():
    print("🔍 Starting review monitoring flow...")
    
    try:
        # Start only the long-running tasks
        consume_future = consume_reviews_task.submit()
        
        print("📊 Review monitoring tasks started successfully")
        print("Press Ctrl+C to stop monitoring...")
        try:
            consume_future.wait()
        except KeyboardInterrupt:
            print("🛑 Review monitoring stopped by user")
            
    except Exception as e:
        print(f"❌ Review monitoring flow failed: {e}")
        raise e

@flow(
    name="steam_review_recovery_flow",
    task_runner=ConcurrentTaskRunner(),
    log_prints=True
)
def steam_review_recovery_flow():
    print("🔧 Starting review recovery flow...")
    
    try:
        result = steam_review_pipeline()
        print("✅ Review recovery completed successfully")
        return result
        
    except Exception as e:
        print(f"❌ Review recovery failed: {e}")
        raise e

@flow(
    name="steam_review_crawl_only_flow",
    task_runner=ConcurrentTaskRunner(),
    log_prints=True
)
def steam_review_crawl_only_flow():
    pipeline_start = datetime.now()
    print(f"🚀 Starting Steam review crawl-only flow at {pipeline_start.strftime('%H:%M:%S')}...")
    
    try:
        metrics.set_pipeline_status('review_crawl_only', 1)
        
        # Phase 1: Review Data Collection
        print("📊 Phase 1: Review Data Collection")
        crawl_future = crawl_reviews_task.submit()
        crawl_result = crawl_future.wait()
        
        if crawl_future.get_state().is_failed():
            print("❌ Review crawl task failed")
            raise Exception("Review crawl task failed")
        
        # Phase 2: Load to MongoDB
        print("📥 Phase 2: Load to MongoDB")
        mongo_future = load_and_publish_reviews_task_watch.submit()
        # mongo_future = load_to_mongo_task_watch.submit()
        mongo_result = mongo_future.wait()
        
        if mongo_future.get_state().is_failed():
            print("❌ MongoDB load task failed")
            raise Exception("MongoDB load task failed")
        
        # Calculate and log pipeline duration
        pipeline_duration = datetime.now() - pipeline_start
        print(f"⏱️ Review crawl-only flow completed in {pipeline_duration.total_seconds():.2f} seconds")
        
        # Push final metrics
        metrics.push_metrics("_review_crawl_only_complete")
        
        return {
            'status': 'success',
            'duration': pipeline_duration.total_seconds()
        }
        
    except Exception as e:
        print(f"❌ Review crawl-only flow failed: {e}")
        metrics.set_pipeline_status('review_crawl_only', 0)
        metrics.push_metrics("_review_crawl_only_failed")
        raise e
    finally:
        metrics.set_pipeline_status('review_crawl_only', 0)

@flow(
    name="steam_unified_pipeline",
    task_runner=ConcurrentTaskRunner(),
    retries=1,
    retry_delay_seconds=30,
    log_prints=True
)

def steam_unified_pipeline():
    pipeline_start = datetime.now()
    print(f"🚀 Starting unified Steam pipeline at {pipeline_start.strftime('%H:%M:%S')}...")
    
    try:
        metrics.set_pipeline_status('unified_pipeline', 1)
        
        # Import price pipeline
        from flows.steam_price_pipeline import steam_price_pipeline
        
        # Run both pipelines concurrently
        print("🔄 Starting both price and review pipelines concurrently...")
        
        price_future = steam_price_pipeline.submit()
        review_future = steam_review_pipeline.submit()
        
        # Wait for both to complete
        price_result = price_future.wait()
        review_result = review_future.wait()
        
        # Check results
        price_failed = price_future.get_state().is_failed()
        review_failed = review_future.get_state().is_failed()
        
        if price_failed or review_failed:
            error_msg = []
            if price_failed:
                error_msg.append("Price pipeline failed")
            if review_failed:
                error_msg.append("Review pipeline failed")
            raise Exception(" and ".join(error_msg))
        
        # Calculate and log pipeline duration
        pipeline_duration = datetime.now() - pipeline_start
        print(f"⏱️ Unified pipeline completed in {pipeline_duration.total_seconds():.2f} seconds")
        
        # Push final metrics
        metrics.push_metrics("_unified_pipeline_complete")
        
        return {
            'status': 'success',
            'duration': pipeline_duration.total_seconds(),
            'price_pipeline': price_future,
            'review_pipeline': review_future
        }
        
    except Exception as e:
        print(f"❌ Unified pipeline failed: {e}")
        metrics.set_pipeline_status('unified_pipeline', 0)
        metrics.push_metrics("_unified_pipeline_failed")
        raise e
    finally:
        metrics.set_pipeline_status('unified_pipeline', 0)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        
        if mode == "monitor":
            print("Running review pipeline in monitoring mode...")
            steam_review_monitoring_flow()
        elif mode == "recovery":
            print("Running review pipeline in recovery mode...")
            steam_review_recovery_flow()
        elif mode == "crawl":
            print("Running review pipeline in crawl-only mode...")
            steam_review_crawl_only_flow()
        elif mode == "unified":
            print("Running unified pipeline (price + reviews)...")
            steam_unified_pipeline()
        else:
            print(f"Unknown mode: {mode}")
            print("Available modes: monitor, recovery, crawl, unified")
            print("Running default review pipeline...")
            steam_review_pipeline()
    else:
        steam_review_pipeline()