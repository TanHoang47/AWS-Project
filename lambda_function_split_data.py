import json
import boto3

lambda_client = boto3.client('lambda')
glue_client = boto3.client('glue')
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Get information
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    object_size = event['Records'][0]['s3']['object']['size']
    
    object_size_MB = object_size / (1024 * 1024)
    # print(bucket_name)
    # print(object_key)

    # Split data processing between Lambda and Glue based on file size
    if object_size_MB < 25:
        response = lambda_client.invoke(
            FunctionName='arn:aws:lambda:us-east-1:666243375423:function:hoanglht2-lambda-process-data',
            InvocationType='Event',
            Payload=json.dumps({
                'bucket_name': bucket_name,
                'object_key': object_key
            })
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps('Lambda Execution started successfully')
        }
    else:
        response = glue_client.start_job_run(
            JobName='hoanglht2-glue-execution',
            Arguments={
                '--bucket_name': bucket_name,
                '--object_key': object_key
            }
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps('Glue Job started successfully')
        }
    
