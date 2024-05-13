import json
import boto3
import os
import uuid
from boto3.dynamodb.conditions import Key

region = 'us-east-1'
ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')

dynamodb = boto3.resource("dynamodb", region_name=region, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
connectionTable = dynamodb.Table("chat-connections")
messageTable = dynamodb.Table('chat-messages')

def lambda_handler(event, context):
    print(event)
    
    receiver = event['queryStringParameters']['receiver']
    owner = event['queryStringParameters']['owner']
    
    roomId = None
    existing = connectionTable.query(IndexName='owner-receiver-index', KeyConditionExpression= Key('owner').eq(owner) & Key('receiver').eq(receiver), Select='ALL_ATTRIBUTES')
    print('ex', owner, receiver)
    if (existing['Count'] > 0):
        roomId = existing['Items'][0]['roomId']
        print('existing[Items][0]', existing['Items'][0])
    else:
        counterpart = connectionTable.query(IndexName='owner-receiver-index', KeyConditionExpression=Key('receiver').eq(owner) & Key('owner').eq(receiver), Select='ALL_ATTRIBUTES')
        if (counterpart['Count'] > 0):
            print('counterpart', counterpart['Items'][0])
            roomId = counterpart['Items'][0]['roomId']
    
    messages = {}
    if (roomId is not None):
        messages = messageTable.query(IndexName='roomId-createdAt-index', KeyConditionExpression=Key('roomId').eq(roomId), Select='ALL_ATTRIBUTES')
        print(messages)
    
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps(messages)
    }
