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

5. Data Processing: 

    5.1 S3 Sensor that waits for raw files to arrive into a predefined S3 bucket prefix
  
    5.2 An EMR job to generate reporting data sets
  
    5.3 S3-to-Redshift copy of the aggregated data 
  
  
  
6. Dag file is added to the dag folder in the repo, update your S3 buckets, folders, IAM roles and add dag file to the DAG folder in S3.

7. The Pyspark script is added to scripts folder in the repo, add Pyspark file to the s3://your-bucket/scripts/emr/.

8. Add Redshfit connection to Airflow: https://catalog.us-east-1.prod.workshops.aws/workshops/795e88bb-17e2-498f-82d1-2104f4824168/en-US/workshop-2-2-2/m1-processing/redshift

9. Dag run: 

        The DAG will be initially set to disabled state by default.
        You will need to enable the DAG (by switching the On/Off toggle button) to be picked up by the scheduler. Once the DAG is enabled we can also run         the DAG manually.
