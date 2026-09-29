import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.quality.data_quality import DataQualityFramework

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("TestDQ") \
        .master("local[2]") \
        .getOrCreate()

def test_data_quality_split(spark):
    dq = DataQualityFramework(spark)
    
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("email", StringType(), True)
    ])
    
    data = [
        (1, "valid@test.com"),
        (2, "invalidemail.com"),
        (None, "nullid@test.com")
    ]
    
    df = spark.createDataFrame(data, schema)
    
    rules = [
        {"name": "id_not_null", "expr": "id IS NOT NULL"},
        {"name": "valid_email", "expr": "email LIKE '%@%.%'"}
    ]
    
    valid_df, invalid_df = dq.enforce_rules(df, rules)
    
    assert valid_df.count() == 1
    assert invalid_df.count() == 2
    
    # Check that quarantine records get a reason
    invalid_row = invalid_df.filter("id = 2").first()
    assert "rejection_reason" in invalid_df.columns
    assert invalid_row.rejection_reason is not None
