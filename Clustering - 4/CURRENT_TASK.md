# Status
Cloud Infrastructure (`aws_emr_serverless`) deployed perfectly. GitHub Actions CI/CD pipeline green and automatically zipping/pushing code artifacts to S3. Local PySpark notebook successfully modularized.

# Active
* Syncing physical `.csv` and `.txt` reference files containing healthcare dummy data over to the deployed AWS S3 bucket.
* Executing the EMR Serverless Job (`main.py` entrypoint) via AWS CLI / Console to test real-world computation.

# Recently Completed
* Obliterated leftover deep learning project config documentation.
* CDK pipeline initialized, bootstrapped via VSCode local terminal (fixing the $env:APPDATA/npm directory bug on Windows), and deployed. EMR Application `00g5vs2bi9u9rp09` created successfully. 
* GitHub Action workflow fixed — dropped Node.js 20 actions and bumped `github-script` to `v7` running Node.js 24.
* Refactored a giant script (`BD4H_HW3.ipynb`) into proper `src/data_loader.py`, `src/phenotyping.py`, `src/features.py`, and `src/clustering.py`.

# Blocked
* Nothing right now.

# Backlog (Next Steps)
* Push the `data/` directory to S3 utilizing AWS CLI.
* Run the EMR Serverless StartJobRun command (`aws emr-serverless start-job-run`).
* Download output from S3 bucket `/output/` folder and ensure PySpark saved the DataFrames and Purity metrics cleanly.
* Document how to teardown `cdk destroy` so zero costs are incurred.