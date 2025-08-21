# config/settings.py
import os
from dataclasses import dataclass, field
from typing import Optional
from dotenv import load_dotenv

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Load .env
load_dotenv('config/.env')

@dataclass
class DatabaseConfig:
    host: str = os.getenv("MYSQL_HOST", "localhost")
    port: int = int(os.getenv("MYSQL_PORT", 3307))
    user: str = os.getenv("MYSQL_USER", "root")
    password: str = os.getenv("MYSQL_PASSWORD", "root")
    database: str = os.getenv("MYSQL_DB", "steam_db")
    max_retries: int = 5
    retry_delay: int = 2

@dataclass
class KafkaConfig:
    bootstrap_servers: list = None
    steam_dynamic_topic: str = "steam-dynamic-price"  
    steam_review_topic: str = "steam-dynamic-review"   
    review_topic: str = "steam-dynamic-review"
    mysql_cdc_topic: str = "mysql-server.prices"
    consumer_timeout_ms: int = 10000
    
    def __post_init__(self):
        if self.bootstrap_servers is None:
            self.bootstrap_servers = [os.getenv("KAFKA_SERVER", "kafka:9092")]

@dataclass
class APIConfig:
    steam_api_url: str = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
    steamspy_api_url: str = "https://steamspy.com/api.php"
    request_timeout: int = 10
    max_retries: int = 3
    retry_delay: int = 2

@dataclass
class CrawlConfig:
    max_workers: int = 20
    batch_size: int = 50
    sleep_between_batches: float = 0.5
    transform_polling_interval: int = 3

@dataclass
class MonitoringConfig:
    prometheus_gateway: str = os.getenv("PROMETHEUS_GATEWAY", "pushgateway:9091")
    job_name: str = "steam_pipeline"
    enable_metrics: bool = os.getenv("ENABLE_METRICS", "true").lower() == "true"

@dataclass
class DiscordConfig:
    webhook_url: Optional[str] = os.getenv("DISCORD_WEBHOOK_URL")
    enable_notifications: bool = os.getenv("ENABLE_DISCORD", "true").lower() == "true"

@dataclass
class FileConfig:
    data_dir: str = "data"
    raw_dir: str = "data/raw"
    processed_dir: str = "data/processed"
    checkpoint_dir: str = "data/checkpoints"

@dataclass
class MongoConfig:
    uri: str = os.getenv("MONGO_URI", "mongodb://mongo:27017")
    database: str = os.getenv("MONGO_DB", "steam")
    collection: str = os.getenv("MONGO_COLLECTION", "reviews")

@dataclass
class AppConfig:
    database: DatabaseConfig = field(default_factory=DatabaseConfig) 
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    api: APIConfig = field(default_factory=APIConfig)
    crawl: CrawlConfig = field(default_factory=CrawlConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
    discord: DiscordConfig = field(default_factory=DiscordConfig)
    files: FileConfig = field(default_factory=FileConfig)
    mongo: MongoConfig = field(default_factory=MongoConfig)
    
    def __post_init__(self):
        os.makedirs(self.files.raw_dir, exist_ok=True)
        os.makedirs(self.files.processed_dir, exist_ok=True)
        os.makedirs(self.files.checkpoint_dir, exist_ok=True)

# Global config instance
config = AppConfig()
