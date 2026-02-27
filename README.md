# Search Keyword Performance Analyzer
This program can Processes files of any size —
from a laptop to a 100 GB+ AWS Glue cluster.


## local set up
git clone 
cd assignment_repo

# To run with chunked processor by setting this param  before running the below command  $env:PROCESSOR = "chunked"   please un-comment line number 13 in main PROCESSOR = os.getenv("PROCESSOR", "chunked") 
python main.py data.sql


## Running Test cases

# All tests without spark

python -m unittest tests/test_all.py -v

# Spark tests: it requires PySpark. 
python -m unittest tests/test_spark_processor.py -v

## Deploy to EC2

# 1. Provision the  ec2 instance (one-time)
aws cloudformation deploy --template-file D:\assignment_repo\infra\ec2_stack.yaml --stack-name assignment-ec2 --capabilities CAPABILITY_NAMED_IAM --region us-east-1 --parameter-overrides DataBucketName=assignment-bucket-ss KeyPairName=assignment-key

# 2. SSH in, clone the repo, then run:
./infra/deploy_ec2.sh s3://assignment-bucket-ss/input/data.sql s3://assignment-bucket-ss/output/

# Spark backend:
PROCESSOR=spark ./infra/deploy_ec2.sh s3://assignment-bucket-ss/input/data.sql s3://assignment-bucket-ss/output/


---

## Deploy to AWS Glue


./infra/deploy_glue.sh my-glue-scripts assignment-bucket-ss assignment-stack us-east-1


The script prints the exact `aws glue start-job-run` command at the end.


## Switching Back-ends

# main.py  — change only this one line:
PROCESSOR = os.getenv("PROCESSOR", "chunked")   #set to "spark"
# or at runtime:
# PROCESSOR=spark python main.py data/data.sql


Here are the references for the templates used in the deployment scripts:

CloudFormation Resources
EC2 Stack
EC2 Instance: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-properties-ec2-instance.html
Security Group: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-ec2-securitygroup.html
IAM Role: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-iam-role.html
Instance Profile: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-iam-instanceprofile.html

Glue Stack

Glue Job: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-glue-job.html
Glue Trigger: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-glue-trigger.html
CloudWatch Alarm: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-cloudwatch-alarm.html
CloudWatch Log Group: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-logs-loggroup.html


Glue PySpark Development

Glue Job Parameters: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html
PySpark in Glue: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python.html
extra-py-files usage: https://docs.aws.amazon.com/glue/latest/dg/reduced-start-times-spark-etl-jobs.html

AWS CLI References
CloudFormation deploy: https://docs.aws.amazon.com/cli/latest/reference/cloudformation/deploy.html
Glue start-job-run: https://docs.aws.amazon.com/cli/latest/reference/glue/start-job-run.html
S3 cp: https://docs.aws.amazon.com/cli/latest/reference/s3/cp.html