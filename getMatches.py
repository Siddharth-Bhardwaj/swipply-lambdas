import os
import json
import datetime
import base64
import boto3
from boto3.dynamodb.conditions import Key
import decimal

region = 'us-east-1'
ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super().default(o)

def lambda_handler(event, context):
    print(event)
    dynamodb = boto3.resource("dynamodb", region_name=region, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    user_id = event["queryStringParameters"]["userId"]
    usertype = event["queryStringParameters"]["type"]
    entity_table = dynamodb.Table(usertype)
    match_table = dynamodb.Table('matches')
    dynamo_response = entity_table.query(KeyConditionExpression=Key("id").eq(user_id), Select='ALL_ATTRIBUTES')
    candidate = dynamo_response['Items'][0]
    
    dynamo_response = match_table.query(IndexName='initiator-receiver-index', KeyConditionExpression=Key('initiator').eq(user_id), Select='ALL_ATTRIBUTES')
    response = []
    for match in dynamo_response['Items']:
        receiver_entity = None
        if usertype == 'candidates':
            entity_table = dynamodb.Table('jobPosting')
        else:
            entity_table = dynamodb.Table('candidates')

        receiver_entity = entity_table.query(KeyConditionExpression=Key("id").eq(match['receiver']))
        match['initiator'] = candidate
        match['receiver'] = receiver_entity['Items'][0]
        
        if usertype == 'candidates':
            company_table = dynamodb.Table('company')
            company = company_table.query(KeyConditionExpression=Key("id").eq(match['receiver']['companyId']))
            if (company['Count'] > 0):
                match['receiver']['company'] = company['Items'][0]
                response.append(match)
        else:
            response.append(match)
    
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
            'Content-Type': 'application/json'
        },
        'body': json.dumps(response, cls=DecimalEncoder)
    }
