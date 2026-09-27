import os
import sys
import time
import json
import pandas as pd
import logging
from kafka import KafkaProducer

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.settings import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClickstreamProducer:
    def __init__(self, bootstrap_servers=None, topic=None):
        self.bootstrap_servers = bootstrap_servers or config.KAFKA_BOOTSTRAP_SERVERS
        self.topic = topic or config.KAFKA_CLICKSTREAM_TOPIC
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8'),
                # Add retries and acknowledgements for reliability
                acks='all',
                retries=3
            )
            logger.info(f"Connected to Kafka broker at {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            self.producer = None

    def produce_events(self, csv_path, events_per_second=10, continuous=False):
        if not self.producer:
            logger.error("Producer not initialized. Exiting.")
            return

        if not os.path.exists(csv_path):
            logger.error(f"CSV file not found: {csv_path}")
            return

        logger.info(f"Loading data from {csv_path}...")
        df = pd.read_csv(csv_path)
        
        # Convert dataframe to list of dicts for faster iteration
        records = df.to_dict(orient='records')
        total_records = len(records)
        
        delay = 1.0 / events_per_second if events_per_second > 0 else 0
        
        iteration = 1
        while True:
            logger.info(f"Starting replay iteration {iteration} ({total_records} events)")
            count = 0
            for record in records:
                try:
                    # event_id as key for stable partitioning
                    key = record.get('event_id', str(count))
                    self.producer.send(self.topic, key=key, value=record)
                    count += 1
                    
                    if count % 1000 == 0:
                        logger.info(f"Produced {count} events...")
                        
                    if delay > 0:
                        time.sleep(delay)
                except KeyboardInterrupt:
                    logger.info("Interrupted by user. Shutting down...")
                    self.producer.flush()
                    self.producer.close()
                    return
                except Exception as e:
                    logger.error(f"Failed to send record: {e}")
            
            logger.info(f"Finished iteration {iteration}. Produced {count} events.")
            if not continuous:
                break
            iteration += 1

        self.producer.flush()
        logger.info("Production complete.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Clickstream Kafka Producer")
    parser.add_argument("--eps", type=int, default=10, help="Events per second")
    parser.add_argument("--continuous", action="store_true", help="Loop the dataset continuously")
    
    args = parser.parse_args()
    
    producer = ClickstreamProducer()
    csv_file = os.path.join(config.RAW_DIR, "clickstream.csv")
    producer.produce_events(csv_file, events_per_second=args.eps, continuous=args.continuous)
