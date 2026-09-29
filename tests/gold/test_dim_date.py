import pytest
from pyspark.sql import SparkSession
from src.gold.dimensions.dim_date import DimDateBuilder

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("TestGold") \
        .master("local[2]") \
        .getOrCreate()

def test_dim_date_generation(spark, tmp_path):
    target_base = str(tmp_path / "lakehouse")
    builder = DimDateBuilder(spark, target_base)
    
    # Generate for a small range to make test fast
    stats = builder.process(start_date="2024-01-01", end_date="2024-01-10")
    
    # 10 days
    assert stats["rows"] == 10
    
    # Verify the table exists and can be read
    import os
    target_path = os.path.join(target_base, "gold", "dimensions", "dim_date")
    df = spark.read.format("delta").load(target_path)
    
    assert df.count() == 10
    columns = df.columns
    assert "date_key" in columns
    assert "is_weekend" in columns
