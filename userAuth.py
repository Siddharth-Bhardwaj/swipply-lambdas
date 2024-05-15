import json
import boto3
import os
from datetime import datetime

region = 'us-east-1'
ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')

def lambda_handler(event, context):
    event['response']['autoConfirmUser'] = True
    event['response']['autoVerifyEmail'] = True
    
    dynamodb = boto3.resource(
                    'dynamodb',
                    region_name=region,
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY
                )
    table = dynamodb.Table('candidates')
    
    # Get user pool details from event
    username = event['userName']
    userattributes = event['request']['userAttributes']
    
    try:
        # Confirm user registration
        item = {
            'id' : username,
            'firstname' : userattributes['given_name'],
            'lastname' : userattributes['family_name'],
            'email' : userattributes['email'],
            'createdAt' : str(datetime.now().astimezone().isoformat())
        }
        print("here 3")
        dbresponse = table.put_item(
            Item=item
        )
        print(dbresponse, "here 4")
        return event

    except Exception as e:
        print("Error confirming user:", e)
        return event