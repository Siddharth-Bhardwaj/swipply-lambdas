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
    connectionId = event['requestContext']['connectionId']
    receiver = event['queryStringParameters']['receiver']
    owner = event['queryStringParameters']['owner']

    roomId = str(uuid.uuid4())
    existing = connectionTable.query(IndexName='owner-receiver-index', KeyConditionExpression=Key('receiver').eq(receiver) & Key('owner').eq(owner), Select='ALL_ATTRIBUTES')
    if (existing['Count'] > 0):
        roomId = existing['Items'][0]['roomId']
        connectionTable.delete_item(Key={"id": existing['Items'][0]['id']})
    else:
        counterpart = connectionTable.query(IndexName='owner-receiver-index', KeyConditionExpression=Key('receiver').eq(owner) & Key('owner').eq(receiver), Select='ALL_ATTRIBUTES')
        if (counterpart['Count'] > 0):
            roomId = counterpart['Items'][0]['roomId']
    
    # map messages to roomID
    connectionTable.put_item(Item={'id': connectionId, 'receiver': receiver, 'owner': owner, 'roomId': roomId})
    
    messages = messageTable.query(IndexName='roomId-createdAt-index', KeyConditionExpression=Key('roomId').eq(roomId), Select='ALL_ATTRIBUTES')
    print(messages)

    return {
        'statusCode': 200,
        'body': json.dumps(messages)
    }