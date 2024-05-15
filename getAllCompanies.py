import os
import json
import boto3
from boto3.dynamodb.conditions import Key

region = 'us-east-1'
ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')

def lambda_handler(event, context):
    try:
        dynamodb = boto3.resource(
                "dynamodb",
                region_name=region,
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_KEY
            )
        
        table = dynamodb.Table("company")
        
        response = table.scan()
        
        Items = response['Items']

        companyDetails = []
        
        for item in Items:
            companyDict = {}
            companyDict['name'] = item['name']
            companyDict['id'] = item['id']
            companyDetails.append(companyDict)
    
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'body': companyDetails
        }
        
    except Exception as e:
        return {
            'statusCode': 404,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'message': json.dumps('No company found.')
        }