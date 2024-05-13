import os
import json
import datetime
import base64
import boto3
from boto3.dynamodb.conditions import Key

region = 'us-east-1'
ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')

def lambda_handler(event, context):
    print(event)
    
    dynamodb = boto3.resource("dynamodb", region_name=region, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    table = dynamodb.Table("candidates")
    user_id = event["queryParams"]["userId"]
    dynamo_response = table.query(KeyConditionExpression=Key("id").eq(user_id))
    candidate = dynamo_response['Items'][0]
    filename = user_id + "_" + str(datetime.datetime.now().astimezone().isoformat()) + ".pdf"
    candidate['resumeS3Key'] = filename
    table.put_item(Item = candidate)
    
    s3 = boto3.client("s3")
    get_file_content = event["content"]
    decode_content = base64.b64decode(get_file_content)
    s3_upload = s3.put_object(Bucket="swipply-resume", Key=filename, Body=decode_content)
    
    sqsClient = boto3.client('sqs')
    sqsClient.send_message(
        QueueUrl='https://sqs.us-east-1.amazonaws.com/992382771028/resume-parsing',
        DelaySeconds=0,
        MessageBody=(
            user_id
        )
    )
    
    print(filename)
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
            'Content-Type': 'application/json'
        },
        'body': filename
    }