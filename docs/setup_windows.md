# E-commerce Data Lakehouse - Setup Guide (Windows)

## Prerequisites

1. **Python 3.11**
   - Verify: `python --version`
2. **Java JDK 17**
   - Required by Apache Spark.
   - Verify: `java -version`
   - Ensure `JAVA_HOME` environment variable is set to the JDK installation path.
3. **Apache Kafka 4.1.x** (KRaft mode)
   - Verify: `kafka-topics.bat --version` or check your Kafka installation directory.
   - To install, download Kafka from the Apache website, extract to `C:\kafka`, and add `C:\kafka\bin\windows` to your PATH.
4. **AWS CLI**
   - Required for local credentials chain.
   - Install using MSI from AWS. Verify: `aws --version`
   - Configure credentials: `aws configure`
5. **Git**
   - Verify: `git --version`

## Step-by-Step Setup

1. **Create Virtual Environment**
   Open PowerShell in the project root:
   ```powershell
   python -m venv venv
   .\venv\Scripts\activate
   ```

2. **Install Dependencies**
   ```powershell
   pip install -r requirements.txt
   ```

3. **Configure Environment Variables**
   - Copy `.env.example` to `.env`.
   - Update `S3_BUCKET_NAME` to your actual S3 bucket name.
   - Ensure `AWS_PROFILE` is set to the CLI profile you configured.

4. **Verify Datasets**
   - Extract datasets into `data/raw/`.
   - Run the validation script:
     ```powershell
     python scripts/validate_datasets.py
     ```

5. **Validate Environment & Spark**
   ```powershell
   python scripts/validate_environment.py
   ```

6. **Start Kafka (KRaft mode)**
   - Format storage (only once):
     ```powershell
     .\bin\windows\kafka-storage.bat format -t <UUID> -c .\config\kraft\server.properties
     ```
   - Start Server:
     ```powershell
     .\bin\windows\kafka-server-start.bat .\config\kraft\server.properties
     ```

7. **Upload Raw Data to S3**
   - Dry run:
     ```powershell
     python scripts/upload_raw_to_s3.py --dry-run
     ```
   - Actual upload (requires AWS credentials and bucket access):
     ```powershell
     python scripts/upload_raw_to_s3.py
     ```
