import json
import boto3
import os
import datetime
import requests
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
    table = dynamodb.Table('candidates')
    
    item = {
        'id': None,
        'firstname': None,
        'lastname': None,
        'email': None,
        'gender': None,
        'dob': None,
        'countrycode': None,
        'phone': None,
        'city': None,
        'state': None,
        'country': None,
        'zipcode': None,
        'latitude': None,
        'longitude': None,
        'yearsOfExperience': None,
        'resumeS3Key': None,
        'about': None,
        'createdAt': None,
        'updatedAt': None,
        'lookingForJobType': None,
        'parsedResumeData': None,
        'university': None,
        'degree': None,
        'major': None,
        'graduationDate': None,
        'currentOrganization': None,
        'currentRole': None,
        'github': None,
        'linkedin': None,
        'portfolio': None,
        'lookingForTitle': None,
        'minCompensation': None,
        'maxCompensation': None,
        'preferredLocationType': None,
        'profilePictureS3Key': None
    }
    
    eventBody = event
    
    try:
        # check and insert into db
        candidate_id = eventBody['id']
        item['id'] = candidate_id
           
        if 'firstname' in eventBody and eventBody['firstname']:
            item['firstname'] = eventBody['firstname']
            
        if 'lastname' in eventBody and eventBody['lastname']:
            item['lastname'] = eventBody['lastname']
            
        if 'email' in eventBody and eventBody['email']:
            item['email'] = eventBody['email']
        
        if 'gender' in eventBody and eventBody['gender']:
            item['gender'] = eventBody['gender']
        
        if 'dob' in eventBody and eventBody['dob']:
            item['dob'] = eventBody['dob']
            
        if 'countrycode' in eventBody and eventBody['countrycode']:
            item['countrycode'] = eventBody['countrycode']
        
        if 'phone' in eventBody and eventBody['phone']:
            item['phone'] = eventBody['phone']
            
        if 'yearsOfExperience' in eventBody and eventBody['yearsOfExperience']:
            item['yearsOfExperience'] = eventBody['yearsOfExperience']
            

        if 'resumeS3Key' in eventBody and eventBody['resumeS3Key']:
            item['resumeS3Key'] = eventBody['resumeS3Key']
        
        if 'profilePictureS3Key' in eventBody and eventBody['profilePictureS3Key']:
            item['profilePictureS3Key'] = eventBody['profilePictureS3Key']
        
        if 'about' in eventBody and eventBody['about']:
            item['about'] = eventBody['about']
            
        if 'lookingForJobType' in eventBody and eventBody['lookingForJobType']:
            item['lookingForJobType'] = eventBody['lookingForJobType']
            
        if 'parsedResumeData' in eventBody and eventBody['parsedResumeData']:
            item['parsedResumeData'] = eventBody['parsedResumeData']
        
        if 'university' in eventBody and eventBody['university']:
            item['university'] = eventBody['university']
            
        if 'degree' in eventBody and eventBody['degree']:
            item['degree'] = eventBody['degree']
            
        if 'major' in eventBody and eventBody['major']:
            item['major'] = eventBody['major']
            
        if 'graduationDate' in eventBody and eventBody['graduationDate']:
            item['graduationDate'] = eventBody['graduationDate']
        
        if 'currentOrganization' in eventBody and eventBody['currentOrganization']:
            item['currentOrganization'] = eventBody['currentOrganization']
            
        if 'currentRole' in eventBody and eventBody['currentRole']:
            item['currentRole'] = eventBody['currentRole']
            
        if 'github' in eventBody and eventBody['github']:
            item['github'] = eventBody['github']
            
        if 'linkedin' in eventBody and eventBody['linkedin']:
            item['linkedin'] = eventBody['linkedin']
        
        if 'portfolio' in eventBody and eventBody['portfolio']:
            item['portfolio'] = eventBody['portfolio']
            
        if 'state' in eventBody and eventBody['state']:
            item['state'] = eventBody['state']
            
        if 'zipcode' in eventBody and eventBody['zipcode']:
            item['zipcode'] = eventBody['zipcode']
        
        if 'city' in eventBody and eventBody['city']:
            city = eventBody['city']
            item['city'] = city
            
            if 'country' in eventBody and eventBody['country']:
                country = eventBody['country']
                item['country'] = country
                geo_url = f'https://api.api-ninjas.com/v1/geocoding?city={city}&country={country}'
                
                headers = {'x-api-key':'AliKigSvDFC1tPYqJn6ycg==rlSTMsxtLaOJhnAB'}
                
                response = requests.get(geo_url,headers=headers)
                
                response = response.json()
                
                if len(response) > 0:
                    coords = response[0]
                    item['latitude'] = str(coords['latitude'])
                    item['longitude'] = str(coords['longitude'])
        
        if 'createdAt' in eventBody and eventBody['createdAt']:
            item['createdAt'] = eventBody['createdAt']
        
        if 'lookingForTitle' in eventBody and eventBody['lookingForTitle']:
            item['lookingForTitle'] = eventBody['lookingForTitle']
        
        if 'minCompensation' in eventBody and eventBody['minCompensation']:
            item['minCompensation'] = eventBody['minCompensation']
        
        if 'maxCompensation' in eventBody and eventBody['maxCompensation']:
            item['maxCompensation'] = eventBody['maxCompensation']
        
        if 'preferredLocationType' in eventBody and eventBody['preferredLocationType']:
            item['preferredLocationType'] = eventBody['preferredLocationType']
        
        item['updatedAt'] = str(datetime.now().astimezone().isoformat())
        
        dbresponse = table.put_item(
            Item = item
        )
        print(8)
        returnedresponse = table.query(
            KeyConditionExpression=Key("id").eq(candidate_id)
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
            'message': json.dumps("Update profile failed, candidate id does not exist")
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
            'message': json.dumps("Candidate Profile update failed.")
        }