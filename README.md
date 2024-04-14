# E-Commerce-Data-Pipeline
This project involves building a sophisticated event-driven data ingestion and  transformation pipeline focusing on e-commerce transactional data.

# Step by step process:
1)Transactions data is uploaded to s3_customer_transactions bucket.

2)An S3 event notification triggers a Lambda as soon as a file arrives in the s3_customer_transactions bucket.

3)The Lambda in turn runs the Glue Cralwer to update about the latest partition in the S3 bucket and kicks of a Glue Job.

4)The Glue job performs data validation, transformation and uses dim_prodcts and dim_customers table from Redshift for enrichment of data and finally loads it into fact_transactions table within Redhsift.

5)An EventBridge rule is configured to trigger a message to an SNS topic about the Glue job success or failure.

6)The SNS topic has 2 subscriptions, one is email based subscription and the other one is Lambda which moves the processed files from s3_customer_transactions to s3_customer_transactions_archive bucket.
