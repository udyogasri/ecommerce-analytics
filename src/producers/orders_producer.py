import os
import sys
import time
import json
import uuid
import pandas as pd
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.settings import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OrdersProducer:
    def __init__(self, bootstrap_servers=None, topic=None):
        self.bootstrap_servers = bootstrap_servers or config.KAFKA_BOOTSTRAP_SERVERS
        self.topic = topic or "orders"
        
        logger.info(f"Connecting to Kafka broker at {self.bootstrap_servers}")
        self.producer = KafkaProducer(
            bootstrap_servers=[self.bootstrap_servers],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            retries=3,
            acks='all'
        )
        logger.info(f"Connected to Kafka broker at {self.bootstrap_servers}")

    def generate_events(self, data_path, events_per_second=2, max_events=None, continuous=False):
        logger.info(f"Loading data from {data_path}...")
        df = pd.read_csv(data_path)
        records = df.to_dict('records')
        
        delay = 1.0 / events_per_second
        count = 0
        iteration = 1
        
        try:
            while True:
                logger.info(f"Starting replay iteration {iteration} ({len(records)} events available)")
                for row in records:
                    if max_events and count >= max_events:
                        logger.info(f"Reached max events limit ({max_events}). Stopping.")
                        self.producer.close()
                        return
                    
                    # Generate a stable event_id for the replay, mixing iteration and order_id
                    event_id = str(uuid.uuid5(uuid.NAMESPACE_OID, f"order_{row['order_id']}_iter_{iteration}"))
                    
                    # Create the event payload exactly as the prompt specified
                    event = {
                        "event_id": event_id,
                        "order_id": str(row.get("order_id")),
                        "customer_id": str(row.get("customer_id")),
                        "order_timestamp": str(row.get("order_timestamp")),
                        "order_status": str(row.get("order_status")),
                        "order_total": float(row.get("order_total", 0.0)),
                        "sales_channel": str(row.get("sales_channel")),
                        "event_timestamp": datetime.now(timezone.utc).isoformat(),
                        "event_type": "order_created",
                        "schema_version": "1.0"
                    }
                    
                    try:
                        self.producer.send(self.topic, key=event["order_id"], value=event)
                        count += 1
                        
                        if count % 100 == 0:
                            logger.info(f"Produced {count} order events so far...")
                    except Exception as e:
                        logger.error(f"Failed to send record: {e}")
                    
                    time.sleep(delay)
                
                logger.info(f"Finished iteration {iteration}. Produced {count} total order events.")
                if not continuous:
                    break
                iteration += 1
                
        except KeyboardInterrupt:
            logger.info("Interrupted by user. Shutting down...")
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info("Producer shut down gracefully.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Orders Kafka Producer")
    parser.add_argument("--rate", type=float, default=2.0, help="Events per second")
    parser.add_argument("--max-events", type=int, default=None, help="Maximum events to publish")
    parser.add_argument("--continuous", action="store_true", help="Loop the dataset continuously")
    args = parser.parse_args()
    
    data_path = os.path.join(config.RAW_DIR, 'orders.csv')
    producer = OrdersProducer()
    producer.generate_events(data_path, events_per_second=args.rate, max_events=args.max_events, continuous=args.continuous)
