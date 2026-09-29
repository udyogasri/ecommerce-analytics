import os
import sys
import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_topics():
    bootstrap_servers = [config.KAFKA_BOOTSTRAP_SERVERS]
    topics_to_create = ["clickstream", "orders"]
    
    logger.info(f"Connecting to Kafka broker at {bootstrap_servers}...")
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers, 
            client_id='ecommerce_setup'
        )
    except NoBrokersAvailable:
        logger.error("No Kafka brokers available. Is Kafka running?")
        sys.exit(1)
        
    existing_topics = admin_client.list_topics()
    logger.info(f"Existing topics: {existing_topics}")
    
    new_topic_objects = []
    for topic in topics_to_create:
        if topic not in existing_topics:
            logger.info(f"Topic '{topic}' does not exist. Adding to creation list.")
            new_topic_objects.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
        else:
            logger.info(f"Topic '{topic}' already exists.")
            
    if new_topic_objects:
        try:
            admin_client.create_topics(new_topics=new_topic_objects, validate_only=False)
            logger.info(f"Successfully created topics: {[t.name for t in new_topic_objects]}")
        except TopicAlreadyExistsError as e:
            logger.warning(f"Topic already exists: {e}")
        except Exception as e:
            logger.error(f"Failed to create topics: {e}")
            sys.exit(1)
    else:
        logger.info("No new topics needed to be created.")
        
    admin_client.close()
    logger.info("Topic setup complete.")

if __name__ == "__main__":
    setup_topics()
