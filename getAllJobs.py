import os
import json
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
    try:
        recruiterId = event["queryStringParameters"]["recruiterId"]
        dynamodb = boto3.resource(
                "dynamodb",
                region_name=region,
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_KEY
            )
        
        table = dynamodb.Table("jobPosting")
        companyTable = dynamodb.Table("company")
        
        response = table.query(IndexName='recruiterId-createdAt-index', KeyConditionExpression=Key("recruiterId").eq(recruiterId), Select='ALL_ATTRIBUTES')
        
        Items = response['Items']
    
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'body': json.dumps(Items, cls=DecimalEncoder)
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
            'message': json.dumps('No jobs found.')
        }