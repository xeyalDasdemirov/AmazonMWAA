# AmazonMWAA

Amazon Managed Workflows for Apache Airflow

I am going to run pyspark scipts using EMR in Amazon Managed Workflows for Apache Airflow.

Here are the steps: 

1. Setting up the S3 Buckets, you can find steps in this link: 
https://catalog.us-east-1.prod.workshops.aws/workshops/795e88bb-17e2-498f-82d1-2104f4824168/en-US/workshop-2-2-2/setup/s3

2. Setting up the Managed Airflow Instance: 
https://catalog.us-east-1.prod.workshops.aws/workshops/795e88bb-17e2-498f-82d1-2104f4824168/en-US/workshop-2-2-2/setup/mwaa

3. Creatng IAM Roles: 
https://catalog.us-east-1.prod.workshops.aws/workshops/795e88bb-17e2-498f-82d1-2104f4824168/en-US/workshop-2-2-2/setup/iam

4. Redshift: 
https://catalog.us-east-1.prod.workshops.aws/workshops/795e88bb-17e2-498f-82d1-2104f4824168/en-US/workshop-2-2-2/setup/redshift

Note: Instead of Create schema and redshift tables run the below scripts: 

--  Create pl schema.

CREATE schema pl;

--    Create soccer table.

CREATE TABLE IF not EXISTS pl.soccer (
  team      text,
  goals     int
);

