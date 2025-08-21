# utils/common.py
import mysql.connector
from mysql.connector import Error
import time
import json
import os
import csv
import pytz
from datetime import datetime
from contextlib import contextmanager
from typing import Optional, Dict, Any, Set
from config.settings import config

import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
class DatabaseManager:
    @staticmethod
    @contextmanager
    def get_connection():
        """Context manager for MySQL connections with retry logic"""
        conn = None
        try:
            conn = DatabaseManager._get_connection_with_retry()
            yield conn
        except Error as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn and conn.is_connected():
                conn.close()
    
    @staticmethod
    def _get_connection_with_retry():
        """Get MySQL connection with retry logic"""
        for i in range(config.database.max_retries):
            try:
                return mysql.connector.connect(
                    host=config.database.host,
                    port=config.database.port,
                    user=config.database.user,
                    password=config.database.password,
                    database=config.database.database
                )
            except Error as e:
                print(f"Failed to connect to MySQL, retry {i+1}/{config.database.max_retries}: {e}")
                if i == config.database.max_retries - 1:
                    raise e
                time.sleep(config.database.retry_delay ** i)

class CheckpointManager:
    
    @staticmethod
    def save_checkpoint(task_name: str, data: Dict[str, Any]):
        """Save checkpoint data"""
        checkpoint_file = os.path.join(config.files.checkpoint_dir, f"{task_name}.json")
        with open(checkpoint_file, 'w') as f:
            json.dump(data, f)
    
    @staticmethod
    def load_checkpoint(task_name: str) -> dict:
        path = f"checkpoints/{task_name}.json"
        if os.path.exists(path):
            with open(path, "r") as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    print(f"⚠️ Corrupted checkpoint file {path}, resetting...")
                    return {}
        return {}
    
    @staticmethod
    def clear_checkpoint(task_name: str):
        """Clear checkpoint after successful completion"""
        checkpoint_file = os.path.join(config.files.checkpoint_dir, f"{task_name}.json")
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)

class ProcessedTracker:
    
    def __init__(self, task_name: str):
        self.task_name = task_name
        self.processed_ids: Set[int] = set()
        self.failed_ids: Set[int] = set()
        self._load_state()
    
    def _load_state(self):
        """Load processed and failed IDs from checkpoint"""
        checkpoint = CheckpointManager.load_checkpoint(self.task_name)
        if checkpoint:
            self.processed_ids = set(checkpoint.get('processed_ids', []))
            self.failed_ids = set(checkpoint.get('failed_ids', []))
    
    def mark_processed(self, app_id: int):
        """Mark an app ID as processed"""
        self.processed_ids.add(app_id)
        self.failed_ids.discard(app_id)  
        self._save_state()
    
    def mark_failed(self, app_id: int):
        """Mark an app ID as failed"""
        self.failed_ids.add(app_id)
        self._save_state()
    
    def is_processed(self, app_id: int) -> bool:
        """Check if app ID is already processed"""
        return app_id in self.processed_ids
    
    def get_retry_ids(self) -> Set[int]:
        """Get IDs that failed and need retry"""
        return self.failed_ids.copy()
    
    def _save_state(self):
        """Save current state to checkpoint"""
        CheckpointManager.save_checkpoint(self.task_name, {
            'processed_ids': list(self.processed_ids),
            'failed_ids': list(self.failed_ids)
        })
    
    def clear(self):
        """Clear all state"""
        self.processed_ids.clear()
        self.failed_ids.clear()
        CheckpointManager.clear_checkpoint(self.task_name)

def get_processed_ids_from_csv(csv_file: str) -> Set[int]:
    processed_ids = set()
    if os.path.exists(csv_file) and os.path.getsize(csv_file) > 0:
        try:
            with open(csv_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        processed_ids.add(int(row["appid"]))
                    except (ValueError, KeyError):
                        continue
        except Exception as e:
            print(f"Error reading processed IDs from {csv_file}: {e}")
    return processed_ids

def get_date_string() -> str:
    vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')  # UTC+7
    return datetime.now(vietnam_tz).strftime('%d%m%Y')

def ensure_directory(file_path: str):
    directory = os.path.dirname(file_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
