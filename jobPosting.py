import os
import json
import uuid
import boto3
import datetime
import requests
from datetime import datetime
from boto3.dynamodb.conditions import Key
from elasticsearch import Elasticsearch, RequestsHttpConnection

region = 'us-east-1'
ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')
esUrl = 'https://search-job-postings-mup2q4kbmniqzrkzncb3hpiobe.aos.us-east-1.on.aws'

def lambda_handler(event, context):
    dynamodb = boto3.resource(
                    'dynamodb',
                    region_name=region,
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY
                )
    table = dynamodb.Table('jobPosting')
    
    item = {
        'id': None,
        'companyId': None,
        'recruiterId': None,
        'city': None,
        'state': None,
        'country': None,
        'zipcode': None,
        'latitude': None,
        'longitude': None,
        'title': None,
        'createdAt': None,
        'updatedAt': None,
        'skills': None,
        'employmentType': None,
        'locationType': None,
        'minimumYearsOfExperience': None,
        'about': None,
        'responsibilities': None,
        'qualifications': None,
        'isActive': True,
        'hourlyCompensation': None
    }
    
    eventBody = event
    
    try:
        # check and insert into db
        job_id = str(uuid.uuid4())
        if 'id' in eventBody and eventBody['id']:
            job_id = eventBody['id']
        item['id'] = job_id
        
            
        if 'companyId' in eventBody and eventBody['companyId']:
            item['companyId'] = eventBody['companyId']
            
        if 'recruiterId' in eventBody and eventBody['recruiterId']:
            item['recruiterId'] = eventBody['recruiterId']
            
        if 'isActive' in eventBody:
            item['isActive'] = eventBody['isActive']
        
        if 'state' in eventBody and eventBody['state']:
            item['state'] = eventBody['state']
            
        if 'zipcode' in eventBody and eventBody['zipcode']:
            item['zipcode'] = eventBody['zipcode']
            
        if 'title' in eventBody and eventBody['title']:
            item['title'] = eventBody['title']
        
        if 'skills' in eventBody and eventBody['skills']:
            item['skills'] = eventBody['skills']
            
        if 'employmentType' in eventBody and eventBody['employmentType']:
            item['employmentType'] = eventBody['employmentType']
            
        if 'locationType' in eventBody and eventBody['locationType']:
            item['locationType'] = eventBody['locationType']
            
        if 'minimumYearsOfExperience' in eventBody and eventBody['minimumYearsOfExperience']:
            item['minimumYearsOfExperience'] = eventBody['minimumYearsOfExperience']
            
        if 'about' in eventBody and eventBody['about']:
            item['about'] = eventBody['about']
            
        if 'responsibilities' in eventBody and eventBody['responsibilities']:
            item['responsibilities'] = eventBody['responsibilities']
            
        if 'qualifications' in eventBody and eventBody['qualifications']:
            item['qualifications'] = eventBody['qualifications']
        
        if 'hourlyCompensation' in eventBody and eventBody['hourlyCompensation']:
            item['hourlyCompensation'] = eventBody['hourlyCompensation']
        
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
        else:
            item['createdAt'] = str(datetime.now().astimezone().isoformat())
        
        item['updatedAt'] = str(datetime.now().astimezone().isoformat())
        
        dbresponse = table.put_item(Item=item)
        
        esClient = Elasticsearch(
            esUrl,
            use_ssl=True,
            verify_certs=True,
            http_auth=('admin', 'Password@123'),
            connection_class=RequestsHttpConnection
        )
        
        esObject = {
            'id': job_id,
            'type': 'JobPosting',
            'title': item['title'],
            'minimumYearsOfExperience': 0,
            'companyId': item['companyId'],
            'createdAt': item['createdAt'],
            'lastUpdatedAt': item['updatedAt'],
            'locationType': item['locationType'],
            'employmentType': item['employmentType']
        }
        
        if item['hourlyCompensation']:
            esObject['compensation'] = item['hourlyCompensation']
        if item['city']:
            esObject['city'] = item['city']
        if item['state']:
            esObject['state'] = item['state']
        if item['country']:
            esObject['country'] = item['country']
        if item['latitude'] and item['longitude']:
            location = {
                'lat': item['latitude'],
                'lon': item['longitude']
            }
            esObject['location'] = location
        if item['minimumYearsOfExperience']:
            esObject['minimumYearsOfExperience'] = item['minimumYearsOfExperience']
        if item['skills'] and len(item['skills']) > 0:
            keywords = []
            for skill in item['skills']:
                skillObj = {
                    'keyword': skill['label'],
                    'weight': 1
                }
                if 'weight' in skill and isinstance(skill['weight'], int):
                    skillObj['weight'] = skill['weight']
                keywords.append(skillObj)
            esObject['keywords'] = keywords

        try:
            deleteQuery = {'query': {'match': {'id': job_id}}}
            esClient.delete_by_query(index='job-posting', body=deleteQuery)
        except Exception:
            pass
        
        esClient.index(index='job-posting', body=esObject)
        
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'body': json.dumps("Job posting created!")
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
            'message': json.dumps("Job Posting failed!")
        }