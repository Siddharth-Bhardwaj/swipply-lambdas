import json
import os
import requests
import boto3
from boto3.dynamodb.conditions import Key

region = 'us-east-1'
ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')

def lambda_handler(event, context):
    print(event)
    candidateId = event['queryStringParameters']['userId']
    
    dynamodb = boto3.resource(
                    'dynamodb',
                    region_name=region,
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY
                )
    table = dynamodb.Table('candidates')
    
    dbresponse = table.query(
        KeyConditionExpression=Key("id").eq(candidateId)
    )
    
    parsedResumeData = dbresponse['Items'][0]['parsedResumeData']
    
    if 'lookingForTitle' in dbresponse['Items'][0]:
        lookingForTitle = dbresponse['Items'][0]['lookingForTitle']
    else:
        lookingForTitle = None

    if not lookingForTitle:
        question = '''
            How can I improve my current resume?
            Below is my current resume -
            {}
        '''.format(parsedResumeData)
    else:
        question = '''
            How can I improve my current resume based on my desired job role ({})?
            Below is my current resume -
            {}
        '''.format(lookingForTitle, parsedResumeData)

    api_key = os.environ.get("GEMINI_API_KEY")
    
    url = f'https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent?key={api_key}'
    
    headers = {
        'Content-Type': 'application/json',
    }
    
    payload = {
        'contents': [
            {
                'parts': [
                    {
                        'text': question
                    }
                ]
            }
        ]
    }
    
    response = requests.post(url, headers=headers, json=payload)
    parsedResponse = json.loads(response.text)
    
    if response.status_code == 200:
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'body': parsedResponse['candidates'][0]['content']['parts'][0]['text']
        }
    else:
        return {
            'statusCode': response.status_code,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'body': response.text
        }