import os
import json
import math
import boto3
from decimal import Decimal
from boto3.dynamodb.conditions import Key
from elasticsearch import Elasticsearch, RequestsHttpConnection

region = 'us-east-1'
ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')
esUrl = 'https://search-candidates-lijqnalbccggs2lmh3pl5aweti.aos.us-east-1.on.aws'

def lambda_handler(event, context):
    try:
        jobId = event['id']
        
        dynamodb = boto3.resource(
                    "dynamodb",
                    region_name=region,
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY
                )
        jobPostingTable = dynamodb.Table('jobPosting')
        
        jobData = jobPostingTable.query(KeyConditionExpression=Key("id").eq(jobId))
        jobData = jobData['Items']        
                
        if len(jobData) < 1:
            return {
                'statusCode': 404,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
                'message': json.dumps('Job posting not found!')
            }
            
        jobData = jobData[0]
        
        esQuery = {
            "query": {
                "function_score": {
                  "query": {
                      "bool": {}
                  },
                  "functions": [],
                  "score_mode" : "sum",
                  "boost_mode": "multiply"
                }
            },
            "sort": [
                {
                    "_score": {
                        "order": "desc"
                    }
                }
            ]
        }
        
        functionScore = esQuery['query']['function_score']
        
        if 'employmentType' in jobData and jobData['employmentType']:
            query = {
                "must": {
                    "match": {
                        "lookingForJobType": jobData['employmentType']
                    }
                }
            }
            functionScore['query']['bool'] = query
            
        if 'latitude' and 'longitude' in jobData:
            lat = jobData['latitude']
            lon = jobData['longitude']
            
            if lat and lon:
                lat = Decimal(lat)
                lon = Decimal(lon)
            
                gaussFunc = {
                    "gauss": {
                        "location": {
                            "origin": {
                                "lat": lat,
                                "lon": lon
                            },
                            "offset": "200km",
                            "scale": "100km",
                            "decay": 0.5
                        }
                    },
                    "weight": 5
                }
            
                functionsCopy = functionScore['functions']
                functionsCopy.append(gaussFunc)
                functionScore['functions'] = functionsCopy
                
        if 'ignoreIds' in event and event['ignoreIds']:
            mustNotQuery = []
            if 'must_not' in functionScore['query']['bool'] and isinstance(functionScore['query']['bool']['must_not'], list):
                mustNotQuery = functionScore['query']['bool']['should']
            
            for ignoreId in event['ignoreIds']:
                idObj = { 'match': {} }
                idObj['match']['id'] = ignoreId
                
                mustNotQuery.append(idObj)
                
            functionScore['query']['bool']['must_not'] = mustNotQuery
                
        if 'skills' in jobData and jobData['skills']:
            shouldQuery = []
            if 'should' in functionScore['query']['bool'] and isinstance(functionScore['query']['bool']['should'], list):
                shouldQuery = functionScore['query']['bool']['should']
            
            kw = 'skills.skill.keyword'
            skills = jobData['skills']
            
            for skill in skills:
                skillObj = { 
                    'term': {
                        kw: {}
                    } 
                }
                skillName = skill['label']
                importance = skill['weight']
                
                skillObj['term'][kw]['value'] = skillName
                skillObj['term'][kw]['boost'] = str(importance)
                
                shouldQuery.append(skillObj)
                
            functionScore['query']['bool']['should'] = shouldQuery
            
        if 'minimumYearsOfExperience' in jobData and jobData['minimumYearsOfExperience']:
            linearFunc = {
                "linear": {
                    "yearsOfExperience": {
                        "origin": jobData['minimumYearsOfExperience'],
                        "scale": "3",
                        "decay": 0.5
                    }
                },
                # TODO check weight conditions
                "weight": 3
            }
            
            functionsCopy = functionScore['functions']
            functionsCopy.append(linearFunc)
            functionScore['functions'] = functionsCopy
                
        if 'locationType' in jobData and jobData['locationType']:
            shouldQuery = []
            if 'should' in functionScore['query']['bool'] and isinstance(functionScore['query']['bool']['should'], list):
                shouldQuery = functionScore['query']['bool']['should']
                
            queryObj = {
                "query_string": {
                    "default_field": "preferredLocationType",
                    "query": jobData['locationType'],
                    "boost": 5
                }
            }
            
            shouldQuery.append(queryObj)
            functionScore['query']['bool']['should'] = shouldQuery
            
        limit = 10
        if 'limit' in event and event['limit']:
            limit = event['limit']
            
        page = 1
        if 'page' in event and event['page']:
            page = event['page']
        
        offset = (page - 1) * limit
        esQuery['from'] = offset
        
        esClient = Elasticsearch(
            esUrl,
            use_ssl=True,
            verify_certs=True,
            http_auth=('admin', 'Password@123'),
            connection_class=RequestsHttpConnection
        )
        
        print('query', esQuery)
        esResponse = esClient.search(index="candidates", body=esQuery, size=limit)
        print('response', esResponse)
        
        # TODO check total condition
        totalResults = esResponse['_shards']['successful']
        totalPages = math.ceil(totalResults / limit)
        hits = esResponse['hits']['hits'] if len(esResponse['hits']['hits']) > 0 else []
        print(hits)
        candidateIds = []
        
        for candidate in hits:
            source = candidate['_source']
            print('source', source)
            candidateIds.append({'id': source['id'], 'createdAt': source['dateCreatedAt']})
        
        responseBody = {
            'page': page,
            'totalPages': totalPages,
            'candidates': []
        }

        if len(candidateIds) > 0:
            candidatesTable = dynamodb.Table('candidates')
            print(candidateIds)
            batch_keys = {
                candidatesTable.name: {
                    'Keys': [{ 'id': c['id'], 'createdAt': c['createdAt'] } for c in candidateIds]
                }
            }
                
            dbResponse = dynamodb.batch_get_item(RequestItems=batch_keys)
            dbResponse = dbResponse['Responses']
            
            candidatesData = []
            companiesData = {}
                        
            if candidatesTable.name in dbResponse:
                data = dbResponse[candidatesTable.name]
                
                if len(data) > 0:
                    for candidate in data:
                        candidatesData.append(candidate)
                        
            responseBody['candidates'] = candidatesData
        
            return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
                'body': responseBody
            }
        
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'body': responseBody
        }
        
    except Exception as e:
        print(e, "err")
        return {
            'statusCode': 400,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'message': json.dumps('Error in fetching recommendations!')
        }
    
    
    
    
    
    
