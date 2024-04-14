import json
import boto3
import time

def lambda_handler(event, context):
    
    print("EVENT")
    print(event)
    
    s3_event = event['Records'][0]['eventName']
    
    if s3_event == 'ObjectCreated:Put':
        
        # Initialize Boto3 client for Glue
        glue_client = boto3.client('glue')
        crawler_name = 's3-ecom-transactions'
        
        # Wait until crawler completes previous run
        cnt = 0
        while True:
            response_get = glue_client.get_crawler(Name=crawler_name)
            state = response_get["Crawler"]["State"]
            if state == "READY":# Other known states: RUNNING, STOPPING
                break
            else:
                cnt+=1
                if cnt > 5:
                    print(f'Waited for 5 minutes for crawler:{crawler_name} to complete but it didnt')
                time.sleep(60)
        
        # Start Glue crawler
        response = glue_client.start_crawler(Name=crawler_name)
        print(f"Started Glue crawler '{crawler_name}'")
        
        # Wait until crawler completes
        cnt = 0
        while True:
            response_get = glue_client.get_crawler(Name=crawler_name)
            state = response_get["Crawler"]["State"]
            if state == "READY":# Other known states: RUNNING, STOPPING
                break
            else:
                cnt+=1
                if cnt > 5:
                    print(f'Waited for 5 minutes for crawler:{crawler_name} to complete but it didnt')
                time.sleep(60)
        
        # Trigger Glue job
        job_name = 'ecommerce-data-pipeline'
        response = glue_client.start_job_run(JobName=job_name)
        job_run_id = response['JobRunId']
        print(f"Started Glue job {job_name}, job_run_id: {job_run_id}")
        
        return {
        'statusCode': 200,
        'body': json.dumps(f'Started Glue Job: {job_name}, job_run_id: {job_run_id}')
        }
    
    else:
        return {
        'statusCode': 400,
        'body': json.dumps('Glue Job Not Started')
        }