import os
import uuid
import json
import boto3
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
    table = dynamodb.Table('company')
    
    item = {
        'id': None,
        'name': None,
        'about': None,
        'website': None,
        'imageS3Key': None,
        'industrySector': None,
        'createdAt': None,
        'updatedAt': None,
        'contactName': None,
        'contactEmail': None,
        'contactCountryCode': None,
        'contactPhone': None,
    }
    
    eventBody = event
    
    try:
        # check and insert into db
        company_id = str(uuid.uuid4())
        if 'id' in eventBody and eventBody['id']:
            company_id = eventBody['id']
        item['id'] = company_id
            
        if 'name' in eventBody and eventBody['name']:
            item['name'] = eventBody['name']
            
        if 'about' in eventBody and eventBody['about']:
            item['about'] = eventBody['about']
            
        if 'website' in eventBody and eventBody['website']:
            item['website'] = eventBody['website']
        
        if 'imageS3Key' in eventBody and eventBody['imageS3Key']:
            item['imageS3Key'] = eventBody['imageS3Key']
            
        if 'industrySector' in eventBody and eventBody['industrySector']:
            item['industrySector'] = eventBody['industrySector']
            
        # contact-person
        if 'contactName' in eventBody and eventBody['contactName']:
            item['contactName'] = eventBody['contactName']
            
        if 'contactCountryCode' in eventBody and eventBody['contactCountryCode']:
            item['contactCountryCode'] = eventBody['contactCountryCode']
            
        if 'contactPhone' in eventBody and eventBody['contactPhone']:
            item['contactPhone'] = eventBody['contactPhone']
            
        if 'contactEmail' in eventBody and eventBody['contactEmail']:
            item['contactEmail'] = eventBody['contactEmail']
            
        if 'createdAt' in eventBody and eventBody['createdAt']:
            item['createdAt'] = eventBody['createdAt']
        else:
            item['createdAt'] = str(datetime.now().astimezone().isoformat())
        
        item['updatedAt'] = str(datetime.now().astimezone().isoformat())
        
        dbresponse = table.put_item(
            Item = item
        )
        
        returnedresponse = table.query(
            KeyConditionExpression=Key("id").eq(company_id)
        )
        
        if len(returnedresponse['Items']) > 0:
            returnedresponse = returnedresponse['Items'][0]
            
            return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
                },
                'body': returnedresponse
            }
            
        return {
            'statusCode': 400,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
            },
            'message': json.dumps("Update profile failed, company id does not exist")
        }

    except Exception as e:
        print(e, 'err')
        return {
            'statusCode': 400,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
            },
            'message': json.dumps("Company Profile unsuccessful")
        }