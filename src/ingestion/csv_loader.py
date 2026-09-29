import logging

logger = logging.getLogger(__name__)

class CSVLoader:
    """Utility class to load CSV files into PySpark DataFrames with explicit schemas."""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        
    def load(self, file_path, schema):
        """
        Load a CSV file into a DataFrame using the provided schema.
        Will not infer schema, enforcing data contracts from the start.
        """
        logger.info(f"Loading CSV file from {file_path}")
        try:
            df = self.spark.read.csv(
                file_path,
                header=True,
                schema=schema,
                mode="PERMISSIVE", # Allow parsing errors to become nulls so we can quarantine later
                columnNameOfCorruptRecord="_corrupt_record"
            )
            return df
        except Exception as e:
            logger.error(f"Failed to load CSV file {file_path}: {e}")
            raise e
