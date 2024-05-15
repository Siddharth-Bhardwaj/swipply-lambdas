import json
import boto3
import os
import datetime
from datetime import datetime
from boto3.dynamodb.conditions import Key

region = 'us-east-1'
ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')

def lambda_handler(event, context):
    dynamodb = boto3.resource(
                    'dynamodb',
                    region_name=region,
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY
                )
    table = dynamodb.Table('recruiters')
    
    item = {
        'id': None,
        'firstname': None,
        'lastname': None,
        'gender': None,
        'countrycode': None,
        'phone': None,
        'companyId': None,
        'createdAt': None,
        'updatedAt': None,
        'email': None
    }
    
    eventBody = event
    
    try:
        # check and insert into db
        recruiter_id = eventBody['id']
        item['id']  = recruiter_id
            
        if 'firstname' in eventBody and eventBody['firstname']:
            item['firstname'] = eventBody['firstname']
        
        if 'lastname' in eventBody and eventBody['lastname']:
            item['lastname'] = eventBody['lastname']
            
        if 'gender' in eventBody and eventBody['gender']:
            item['gender'] = eventBody['gender']
            
        if 'countrycode' in eventBody and eventBody['countrycode']:
            item['countrycode'] = eventBody['countrycode']
            
        if 'phone' in eventBody and eventBody['phone']:
            item['phone'] = eventBody['phone']
            
        if 'email' in eventBody and eventBody['email']:
            item['email'] = eventBody['email']
            
        if 'companyId' in eventBody and eventBody['companyId']:
            item['companyId'] = eventBody['companyId']
            
        if 'createdAt' in eventBody and eventBody['createdAt']:
            item['createdAt'] = eventBody['createdAt']
        
        item['updatedAt'] = str(datetime.now().astimezone().isoformat())
        
        dbresponse = table.put_item(
            Item = item
        )
        
        returnedresponse = table.query(
            KeyConditionExpression=Key("id").eq(recruiter_id)
        )
        
        if len(returnedresponse['Items']) > 0:
            returnedresponse = returnedresponse['Items'][0]
            
            return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
                'body': returnedresponse
            }
            
        return {
            'statusCode': 400,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'message': json.dumps("Update profile failed, recruiter id does not exist")
        }

    except Exception as e:
        print(e, 'err')
        return {
            'statusCode': 400,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'message': json.dumps("Recruiter Profile update unsuccessful.")
        }