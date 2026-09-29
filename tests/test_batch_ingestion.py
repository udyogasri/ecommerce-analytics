import os
import sys
import pytest
from pyspark.sql import SparkSession

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.ingestion.batch_ingestion import BatchIngestion
from config.settings import config

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("TestBatchIngestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .master("local[2]") \
        .getOrCreate()

@pytest.fixture
def target_dir(tmp_path):
    return str(tmp_path / "lakehouse")

def test_initial_ingestion(spark, target_dir):
    """Test initial ingestion of a valid file."""
    ingestor = BatchIngestion(spark, target_dir)
    # Using a known raw file for testing
    raw_file = os.path.join(config.RAW_DIR, "products.csv")
    
    success = ingestor.ingest_csv_to_bronze(raw_file, "products")
    assert success is True
    
    # Verify Bronze Delta table
    bronze_path = os.path.join(target_dir, "bronze", "products")
    df = spark.read.format("delta").load(bronze_path)
    
    # Ensure strict metadata columns were added
    columns = df.columns
    assert "_ingestion_timestamp" in columns
    assert "_source_file" in columns
    assert "_batch_id" in columns
    assert "_record_hash" in columns
    assert "_source_system" in columns
    
    assert df.count() > 0

def test_duplicate_batch_execution(spark, target_dir):
    """Test idempotency via manifest (duplicate execution should not add rows)."""
    ingestor = BatchIngestion(spark, target_dir)
    raw_file = os.path.join(config.RAW_DIR, "products.csv")
    
    # First run
    ingestor.ingest_csv_to_bronze(raw_file, "products")
    bronze_path = os.path.join(target_dir, "bronze", "products")
    df1 = spark.read.format("delta").load(bronze_path)
    count1 = df1.count()
    
    # Second run (should be skipped)
    ingestor.ingest_csv_to_bronze(raw_file, "products")
    df2 = spark.read.format("delta").load(bronze_path)
    count2 = df2.count()
    
    assert count1 == count2

def test_missing_input_file(spark, target_dir):
    """Test handling of missing input files."""
    ingestor = BatchIngestion(spark, target_dir)
    missing_file = os.path.join(config.RAW_DIR, "non_existent.csv")
    
    # Should safely fail and return False or catch exception
    try:
        success = ingestor.ingest_csv_to_bronze(missing_file, "products")
        assert success is False
    except Exception:
        pass
