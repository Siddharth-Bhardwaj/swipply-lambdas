import os
import json
import boto3
import decimal
from boto3.dynamodb.conditions import Key

region = 'us-east-1'
ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super().default(o)

def lambda_handler(event, context):
    try:
        dynamodb = boto3.resource(
                "dynamodb",
                region_name=region,
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_KEY
            )
        table = dynamodb.Table("jobPosting")
        
        queryParams = event['queryStringParameters']
        companyId = queryParams['companyId']
        
        response = table.query(
                        IndexName='companyId-index',
                        KeyConditionExpression=Key('companyId').eq(companyId)
                    )
        response = response['Items']
        
        if len(response) > 0:
            return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
                'body': json.dumps(response, cls=DecimalEncoder)
            }
        
        
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'body': json.dumps([])
        }
        
    except Exception as e:
        print(e)
        return {
            'statusCode': 404,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'message': json.dumps('Jobs not found')
        }
    
