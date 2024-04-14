import boto3
import json

def lambda_handler(event, context):
    # Extract information from the SNS message
    sns_message = json.loads(event['Records'][0]['Sns']['Message'])
    job_name = sns_message['detail']['jobName']
    job_status = sns_message['detail']['state']
    
    # If Glue job status is "Succeeded"
    if job_status == 'SUCCEEDED':
        # Move files from one S3 bucket to another
        source_bucket = 'ecom-transactions-na'
        destination_bucket = 'ecom-transactions-archive-na'
        s3_client = boto3.client('s3')
        
        # List objects in source bucket
        response = s3_client.list_objects_v2(Bucket=source_bucket)
        if 'Contents' in response:
            for obj in response['Contents']:
                # Copy object to destination bucket
                copy_source = {'Bucket': source_bucket, 'Key': obj['Key']}
                destination_key = obj['Key']
                s3_client.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)
                # Delete object from source bucket
                s3_client.delete_object(Bucket=source_bucket, Key=obj['Key'])
        
        return {
            'statusCode': 200,
            'body': json.dumps(f"Files moved from {source_bucket} to {destination_bucket}")
        }
    
    # If Glue job status is "Failed"
    elif job_status == 'FAILED':
        return {
            'statusCode': 500,
            'body': json.dumps(f"Glue job {job_name} failed")
        }
    
    # If Glue job status is neither "Succeeded" nor "Failed"
    else:
        return {
            'statusCode': 400,
            'body': json.dumps("Unexpected job status")
        }
