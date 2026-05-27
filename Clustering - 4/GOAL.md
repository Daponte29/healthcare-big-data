# What Is This

A Data Engineering and Unsupervised Machine Learning pipeline that clusters healthcare patient data. It transforms raw medical records (encounters, lab results, medications, and diagnostics) into mathematical feature vectors to categorize patients into **Type 1/Type 2 Diabetes Case vs Control** cohorts.

The primary objective was to refactor an exploratory Jupyter Notebook (`BD4H_HW3.ipynb`) into a scalable, modular, **AWS-native Big Data pipeline**. It uses AWS EMR Serverless to compute PySpark scripts with zero cluster idle costs.


---

# Cloud & Tech Stack

* **PySpark** — Feature extraction, sparse vector encoding, KMeans, GMM.
* **AWS EMR Serverless** — Ephemeral execution (only pay when the Spark job is running).
* **AWS S3** — Compute-storage separation. Stores raw data, domain knowledge configs, code (`src.zip`), and model outputs.
* **AWS CDK (Python)** — Infrastructure as Code. Automatically creates IAM roles, Serverless Apps, and deterministic S3 buckets.
* **GitHub Actions** — CI/CD. Pushing to `main` auto-deploys infrastructure and syncs Python code to S3.
* **Apache Airflow** — Orchestration template (`dags/health_dag.py`) built to demonstrate workflow scheduling.


---

# Pipeline Calculations


1. **Rule-Based Phenotyping**: Filtering data sets and using Set logic to define a "Ground Truth" of Case, Control, and Unknown.
2. **Feature Tuples**: Counting occurrences of ICD9/Medications, averaging lab values.
3. **Data Encoding**: `Vectors.sparse()` array creation across a dynamic dictionary of known features.
4. **Reduction & Clustering**: `StandardScaler`, PCA (k=10), KMeans (k=3), GaussianMixture (k=3), and computing **Purity** metrics.


