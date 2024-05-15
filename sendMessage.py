import json
import boto3
import os
from boto3.dynamodb.conditions import Key
import uuid
import datetime

region = 'us-east-1'
ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')

dynamodb = boto3.resource("dynamodb", region_name=region, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
connectionTable = dynamodb.Table("chat-connections")
messageTable = dynamodb.Table('chat-messages')

def lambda_handler(event, context):
    message = json.loads(event['body'])['message']
    owner = json.loads(event['body'])['owner']
    receiver = json.loads(event['body'])['receiver']

    apigatewaymanagementapi = boto3.client(
        'apigatewaymanagementapi', 
        endpoint_url = "https://" + event["requestContext"]["domainName"] + "/" + event["requestContext"]["stage"]
    )

    roomId = None
    existing = connectionTable.query(IndexName='owner-receiver-index', KeyConditionExpression=Key('receiver').eq(owner) & Key('owner').eq(receiver), Select='ALL_ATTRIBUTES')
    if (existing['Count'] == 1):
        connectionId = existing['Items'][0]['id']
        roomId = existing['Items'][0]['roomId']
        apigatewaymanagementapi.post_to_connection(
            Data=message,
            ConnectionId=connectionId
        )
    else:
        ownerConnection = connectionTable.query(IndexName='owner-receiver-index', KeyConditionExpression=Key('receiver').eq(receiver) & Key('owner').eq(owner), Select='ALL_ATTRIBUTES')
        if (ownerConnection['Count'] == 0):
            print('no connection and/or roomId found')
            return {}
        roomId = ownerConnection['Items'][0]['roomId']

    if (roomId is not None):
        messageItem = {
            'id': str(uuid.uuid4()),
            'roomId': roomId,
            'sender': owner,
            'message': message,
            'createdAt': str(datetime.datetime.now().astimezone().isoformat())
        }
        messageTable.put_item(Item = messageItem)

    return {}