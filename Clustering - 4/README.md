# Healthcare EMR Clustering Pipeline

This project is a cloud-native big data pipeline that clusters patient phenotypes using PySpark. It leverages **K-Means** and **Gaussian Mixture Models (GMM)** to categorize patients into Case, Control, and Unknown buckets based on domain knowledge of Type 1 and Type 2 Diabetes Mellitus.

Originally a monolithic Jupyter Notebook, this project has been heavily refactored into a scalable, enterprise-grade architecture using **AWS EMR Serverless**.

##  Tech Stack & Architecture

* **Data Processing:** Apache Spark / PySpark
* **Compute:** AWS EMR Serverless (Zero-idle-cost compute)
* **Storage:** Amazon S3
* **Infrastructure as Code (IaC):** AWS CDK (Python)
* **CI/CD:** GitHub Actions
* **Orchestration:** Apache Airflow (Local DAG to trigger Cloud EMR)

##  Project Structure

```text
Clustering - 4/
├── archive/              # Original monolithic Jupyter Notebooks
├── dags/                 # Apache Airflow DAGs for orchestration
├── data/
│   ├── external/         # Domain knowledge filter files (.txt)
│   ├── processed/        # Output feature vectors and labels
│   └── raw/              # Raw patient encounter and lab results (.csv)
├── infrastructure/       # AWS CDK Infrastructure as Code scripts (Python)
└── src/                  # Modularized PySpark application
    ├── clustering.py     # PCA, KMeans, and GMM models
    ├── data_loader.py    # S3 / Local data ingestion
    ├── features.py       # Sparse vector encoding and feature aggregation
    ├── main.py           # Entry point for the EMR job
    ├── models.py         # Data schemas
    └── phenotyping.py    # Rule-based cohort classification 
```

##  How to Deploy and Run

### 1. CI/CD Deployment

This project uses GitHub actions to automatically deploy the AWS infrastructure and sync the PySpark scripts to S3.
Pushing to the `main` branch will independently deploy the AWS CDK stack and push `src.zip` and `main.py` directly to the S3 bucket.

### 2. Manual Data Upload

To run the job, upload the raw data to the dynamically generated S3 bucket:

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text)
aws s3 sync "data" "s3://health-big-data-emr-${ACCOUNT_ID}/data"
```

### 3. Trigger EMR Serverless Job via CLI

Trigger the serverless execution (Alternatively, use the provided Airflow DAG):

```bash
EMR_APP_ID=$(aws emr-serverless list-applications --query "applications[?name=='healthcare-clustering-app'].id" --output text)
ROLE_ARN=$(aws iam get-role --role-name EmrServerlessExecutionRole --query "Role.Arn" --output text)

aws emr-serverless start-job-run \
    --application-id $EMR_APP_ID \
    --execution-role-arn $ROLE_ARN \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://health-big-data-emr-${ACCOUNT_ID}/scripts/main.py",
            "entryPointArguments": ["--data-uri", "s3://health-big-data-emr-${ACCOUNT_ID}/data", "--output-uri", "s3://health-big-data-emr-${ACCOUNT_ID}/output"],
            "sparkSubmitParameters": "--conf spark.executor.cores=4 --conf spark.executor.memory=16g --conf spark.driver.cores=2 --conf spark.driver.memory=8g --py-files s3://health-big-data-emr-${ACCOUNT_ID}/scripts/src.zip"
        }
    }'
```

### 4. Fetch Results

```bash
aws s3 cp s3://health-big-data-emr-${ACCOUNT_ID}/output/purity_metrics.txt .
cat purity_metrics.txt
```


