from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_s3 as s3,
    aws_iam as iam,
    aws_emrserverless as emrs,
    aws_ecr_assets as ecr_assets
)
from constructs import Construct

class WebHealthClusteringStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1. Create S3 Bucket for Data, Scripts, and Logs
        # Using a deterministic bucket name based on account to avoid cycle issues in CI/CD 
        account_id = Stack.of(self).account
        bucket_name = f"health-big-data-emr-{account_id}"

        data_bucket = s3.Bucket(
            self, "HealthcareBigDataBucket",
            bucket_name=bucket_name,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # 2. IAM Role for EMR Serverless Execution
        emr_exec_role = iam.Role(
            self, "EmrServerlessExecutionRole",
            assumed_by=iam.ServicePrincipal("emr-serverless.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"), # Make restrictive in prod
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonEC2ContainerRegistryReadOnly") # Needed to pull custom Docker image
            ]
        )

        # 3. Build & Push Custom PySpark + Numpy Docker Image
        # CDK automatically builds the Dockerfile running locally, creates an ECR repo, and pushes it up securely.
        custom_docker_image = ecr_assets.DockerImageAsset(
            self, "EmrServerlessCustomImage",
            directory="../image" # Path to our newly created Docker folder
        )

        # 4. EMR Serverless Application
        emr_app = emrs.CfnApplication(
            self, "HealthcareEmrApp",
            release_label="emr-6.10.0",
            type="SPARK",
            name="healthcare-clustering-app",
            image_configuration=emrs.CfnApplication.ImageConfigurationInputProperty(
                image_uri=custom_docker_image.image_uri
            ),
            maximum_capacity=emrs.CfnApplication.MaximumAllowedResourcesProperty(
                cpu="16 vCPU",
                memory="64 GB",
                disk="200 GB"
            )
        )

        # Output the IDs needed for Airflow
        from aws_cdk import CfnOutput
        CfnOutput(self, "S3BucketName", value=data_bucket.bucket_name)
        CfnOutput(self, "EmrServerlessAppId", value=emr_app.attr_application_id)
        CfnOutput(self, "EmrExecutionRoleArn", value=emr_exec_role.role_arn)
