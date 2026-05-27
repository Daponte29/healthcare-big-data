FROM public.ecr.aws/emr-serverless/spark/emr-6.10.0:latest

USER root

# Install the mathematical libraries our PySpark scripts rely on
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install numpy pandas scikit-learn

# Important: Must switch back to the hadoop user, otherwise EMR Serverless will fail to start
USER hadoop:hadoop