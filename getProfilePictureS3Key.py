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
    user_id = event["queryStringParameters"]["userId"]
    type = event["queryStringParameters"]["type"]
    table = dynamodb.Table(type)
    dynamo_response = table.query(KeyConditionExpression=Key("id").eq(user_id))
    candidate = dynamo_response['Items'][0]
    s3Key = candidate['profilePictureS3Key']
    
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
            'Content-Type': 'application/json'
        },
        'body': json.dumps(s3Key)
    }
