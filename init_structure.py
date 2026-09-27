import os
import shutil
from pathlib import Path

def create_structure():
    base_dir = Path(r"c:\Users\allub\ecommerce-analytics")
    
    dirs = [
        "config",
        "data/raw",
        "data/sample",
        "data/local_test",
        "scripts",
        "src/ingestion",
        "src/storage",
        "src/etl",
        "src/producers",
        "src/consumers",
        "src/utils",
        "tests",
        "docs"
    ]
    
    for d in dirs:
        (base_dir / d).mkdir(parents=True, exist_ok=True)
        # Create __init__.py for python packages
        if d.startswith("src/"):
            (base_dir / d / "__init__.py").touch()
    
    (base_dir / "src" / "__init__.py").touch()
            
    files = [
        "config/settings.py",
        "config/schemas.py",
        "config/logging_config.py",
        "scripts/validate_environment.py",
        "scripts/validate_datasets.py",
        "scripts/upload_raw_to_s3.py",
        "src/ingestion/csv_loader.py",
        "src/ingestion/s3_uploader.py",
        "src/storage/s3_storage.py",
        "src/storage/delta_storage.py",
        "src/etl/spark_session.py",
        "src/producers/clickstream_producer.py",
        "src/utils/validators.py",
        "tests/test_schemas.py",
        "tests/test_datasets.py",
        "tests/test_s3_storage.py",
        "tests/test_delta_storage.py",
        "docs/architecture.md",
        "docs/data_dictionary.md",
        "docs/setup_windows.md",
        ".env.example",
        ".gitignore",
        "requirements.txt",
        "README.md"
    ]
    
    for f in files:
        (base_dir / f).touch()

    # Move datasets
    source_datasets = base_dir / "ecommerce_lakehouse_datasets"
    raw_dir = base_dir / "data" / "raw"
    if source_datasets.exists():
        for csv in source_datasets.glob("*.csv"):
            shutil.copy(csv, raw_dir / csv.name)
        print("Moved datasets to data/raw/")

if __name__ == "__main__":
    create_structure()
    print("Project structure created successfully.")
