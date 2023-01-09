# error_handler

The error_handler program is an AWS Lambda function that processes AWS Batch job failures.

It logs the job failure to a CloudWatch log and publishes the job failure to an SNS Topic.

Top-level Generate repo: https://github.com/podaac/generate

## aws infrastructure

The error_handler program includes the following AWS services:
- Lambda function to execute code deployed via zip file.
- Permissions that allow EventBridge to invoke the Lambda function.
- IAM role and policy for Lambda function execution.
- EventBridge rule to catch Batch job failures and target Lambda function.
- SNS Topic for Batch job failure with a topic policy and an email subscription.
- SNS Topic for Lambda function failure with a topic policy and an email subscription.
- CloudWatch metric alarm for Lambda function errors.

## terraform 

Deploys AWS infrastructure and stores state in an S3 backend using a DynamoDB table for locking.

To deploy:
1. Edit `terraform.tfvars` for environment to deploy to.
2. Edit `terraform_conf/backed-{prefix}.conf` for environment deploy.
3. Initialize terraform: `terraform init -backend-config=terraform_conf/backend-{prefix}.conf`
4. Plan terraform modifications: `terraform plan -out=tfplan`
5. Apply terraform modifications: `terraform apply tfplan`

`{prefix}` is the account or environment name.