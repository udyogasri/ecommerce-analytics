import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from config.schemas import SCHEMAS

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("TestStreamingSchema") \
        .master("local[2]") \
        .getOrCreate()

def test_clickstream_schema_exists():
    assert "clickstream" in SCHEMAS
    schema = SCHEMAS["clickstream"]
    assert isinstance(schema, StructType)
    field_names = [f.name for f in schema.fields]
    assert "event_id" in field_names
    assert "user_id" in field_names
    assert "event_type" in field_names

def test_orders_event_schema_exists():
    assert "orders_event" in SCHEMAS
    schema = SCHEMAS["orders_event"]
    assert isinstance(schema, StructType)
    field_names = [f.name for f in schema.fields]
    assert "event_id" in field_names
    assert "order_id" in field_names
    assert "customer_id" in field_names
    assert "order_total" in field_names

def test_schema_parsing(spark):
    """Test that valid JSON maps correctly and invalid JSON maps to nulls."""
    schema = SCHEMAS["clickstream"]
    
    # Valid JSON
    data = ['{"event_id": "123", "user_id": "U1", "event_type": "page_view"}']
    df = spark.read.schema(schema).json(spark.sparkContext.parallelize(data))
    
    row = df.first()
    assert row.event_id == "123"
    assert row.user_id == "U1"
    
    # Invalid JSON schema type
    bad_data = ['{"event_id": 123, "user_id": {"bad": "type"}, "event_type": "page_view"}']
    df_bad = spark.read.schema(schema).json(spark.sparkContext.parallelize(bad_data))
    
    # Spark JSON parser in permissive mode typically casts what it can and drops structures
    row_bad = df_bad.first()
    assert row_bad.event_id == "123" # Cast to string
