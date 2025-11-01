# producer.py
import json
import csv
import time
import random
import threading
from datetime import datetime
from kafka import KafkaProducer
from settings import BOOTSTRAP_SERVERS, TOPIC_CONFIG
from schema import TRANSACTION_SCHEMA

class AdaptiveThrottler:
    """Dynamically adjusts message rate based on Kafka cluster health"""
    def __init__(self, base_delay=0.1, max_delay=5.0):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self._current_delay = base_delay
        
    def adjust(self, success_rate):
        """Modify delay based on success rate (0.0-1.0)"""
        if success_rate < 0.8:
            self._current_delay = min(self._current_delay * 1.5, self.max_delay)
        elif success_rate > 0.95 and self._current_delay > self.base_delay:
            self._current_delay = max(self._current_delay * 0.9, self.base_delay)
        return self._current_delay

def validate_schema(record, institution_id):
    """Enforce schema with data type validation"""
    validated = {}
    for field, field_type in TRANSACTION_SCHEMA.items():
        value = record.get(field)
        try:
            # Dynamic type conversion
            if field_type == "datetime":
                validated[field] = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").isoformat()
            elif field_type == "float":
                validated[field] = float(value)
            else:
                validated[field] = field_type(value)
        except (TypeError, ValueError):
            validated[field] = None  # Handle bad data
    validated["institution_id"] = str(institution_id)
    return validated

def produce_from_csv(file_path, institution_id):
    """Stream CSV data to Kafka with adaptive throttling"""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=5
    )
    
    throttler = AdaptiveThrottler()
    success_count = 0
    total_count = 0
    delay = throttler.base_delay 
    
    while True:  # Continuous streaming loop
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Add synthetic anomalies periodically
                if random.random() < 0.001:
                    row['amount'] = str(float(row['amount']) * 100)  # Fraud pattern
                
                validated_record = validate_schema(row, institution_id)
                topic = TOPIC_CONFIG[institution_id]
                
                try:
                    future = producer.send(topic, validated_record)
                    future.get(timeout=10)  # Wait for ack
                    success_count += 1
                except Exception as e:
                    print(f"🚨 Delivery failed: {e}")
                
                total_count += 1
                if total_count % 100 == 0:
                    success_rate = success_count / total_count
                    delay = throttler.adjust(success_rate)
                
                time.sleep(delay)

if __name__ == "__main__":
    # Start streaming for all institutions concurrently
    institutions = {
        1: "../data/institution1.csv",
        2: "../data/institution2.csv",
        3: "../data/institution3.csv"
    }
    
    threads = []
    for inst_id, file_path in institutions.items():
        t = threading.Thread(
            target=produce_from_csv,
            args=(file_path, inst_id),
            daemon=True
        )
        t.start()
        threads.append(t)
    
    for t in threads:
        t.join()