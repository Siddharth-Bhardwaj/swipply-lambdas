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
esUrl = 'https://search-job-postings-mup2q4kbmniqzrkzncb3hpiobe.aos.us-east-1.on.aws'

def lambda_handler(event, context):
    try:
        candidateId = event['id']
        
        dynamodb = boto3.resource(
                    "dynamodb",
                    region_name=region,
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY
                )
        candidateTable = dynamodb.Table('candidates')
        
        candidateData = candidateTable.query(KeyConditionExpression=Key("id").eq(candidateId))
        candidateData = candidateData['Items']
            
        if len(candidateData) < 1:
            return {
                'statusCode': 404,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
                'message': json.dumps('User not found!')
            }
            
        candidateData = candidateData[0]
        
        esQuery = {
            "query": {
                "function_score": {
                  "query": {
                      "bool": {}
                  },
                  "functions": [
                    {
                        "linear": {
                            "lastUpdatedAt": {
                               "origin": "now",
                               "scale": "10080m",
                               "decay": 0.5
                            }
                        },
                        "weight": 2
                      }
                  ],
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
        
        if 'lookingForJobType' in candidateData and candidateData['lookingForJobType']:
            query = {
                "must": {
                    "match": {
                        "employmentType": candidateData['lookingForJobType']
                    }
                }
            }
            functionScore['query']['bool'] = query
        
        if 'latitude' and 'longitude' in candidateData:
            lat = candidateData['latitude']
            lon = candidateData['longitude']
            
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
                
        # TODO
        if 'ignoreIds' in event and event['ignoreIds']:
            mustNotQuery = []
            if 'must_not' in functionScore['query']['bool'] and isinstance(functionScore['query']['bool']['must_not'], list):
                mustNotQuery = functionScore['query']['bool']['should']
            
            for ignoreId in event['ignoreIds']:
                idObj = { 'match': {} }
                idObj['match']['id'] = ignoreId
                
                mustNotQuery.append(idObj)
            
            functionScore['query']['bool']['must_not'] = mustNotQuery
            
        if 'skills' in candidateData and candidateData['skills'] and len(candidateData['skills']) > 0:
            shouldQuery = []
            if 'should' in functionScore['query']['bool'] and isinstance(functionScore['query']['bool']['should'], list):
                shouldQuery = functionScore['query']['bool']['should']
            
            kw = 'keywords.keyword.keyword'
            skills = candidateData['skills']
            
            for skill in skills:
                skillObj = { 'term': {} }
                skillName = skill['skill']
                proficiency = skill['proficiency']
                
                skillObj['term'][kw]['value'] = skillName
                skillObj['term'][kw]['boost'] = str(proficiency)
                
                shouldQuery.append(skillObj)
                
            functionScore['query']['bool']['should'] = shouldQuery
            
        # TODO: not needed for candidate's job recommendations I think
        if 'yearsOfExperience' in candidateData and candidateData['yearsOfExperience']:
            linearFunc = {
                "linear": {
                    "minimumYearsOfExperience": {
                        "origin": int(candidateData['yearsOfExperience']),
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
            
        if 'lookingForTitle' in candidateData and candidateData['lookingForTitle']:
            shouldQuery = []
            if 'should' in functionScore['query']['bool'] and isinstance(functionScore['query']['bool']['should'], list):
                shouldQuery = functionScore['query']['bool']['should']
            
            queryObj = {
                "query_string": {
                    "default_field": "title",
                    "query": candidateData['lookingForTitle'],
                    "boost": 2
                }
            }
            
            shouldQuery.append(queryObj)
            functionScore['query']['bool']['should'] = shouldQuery
            
        if 'preferredLocationType' in candidateData and candidateData['preferredLocationType']:
            shouldQuery = []
            if 'should' in functionScore['query']['bool'] and isinstance(functionScore['query']['bool']['should'], list):
                shouldQuery = functionScore['query']['bool']['should']
                
            queryObj = {
                "query_string": {
                    "default_field": "locationType",
                    "query": candidateData['preferredLocationType'],
                    "boost": 2
                }
            }
            
            shouldQuery.append(queryObj)
            functionScore['query']['bool']['should'] = shouldQuery
        
        if 'minCompensation' in candidateData and candidateData['minCompensation']:
            shouldQuery = []
            if 'should' in functionScore['query']['bool'] and isinstance(functionScore['query']['bool']['should'], list):
                shouldQuery = functionScore['query']['bool']['should']
            
            rangeQuery = {
                "range": {
                    "hourlyCompensation": {
                        "gte": candidateData['minCompensation'],
                        "boost": 2
                    }
                }
            }
            
            if len(shouldQuery) < 1:
                shouldQuery.append(rangeQuery)
            else:
                for i in range(0, len(shouldQuery)):
                    if 'range' in shouldQuery[i]:
                        shouldQuery[i]['range']['hourlyCompensation']['boost'] = 5
                        shouldQuery[i]['range']['hourlyCompensation']['gte'] = candidateData['minCompensation']
            functionScore['query']['bool']['should'] = shouldQuery
            
            
        if 'maxCompensation' in candidateData and candidateData['maxCompensation']:
            shouldQuery = []
            if 'should' in functionScore['query']['bool'] and isinstance(functionScore['query']['bool']['should'], list):
                shouldQuery = functionScore['query']['bool']['should']
            
            rangeQuery = {
                "range": {
                    "hourlyCompensation": {
                        "lte": candidateData['maxCompensation'],
                        "boost": 2
                    }
                }
            }
            
            if len(shouldQuery) < 1:
                shouldQuery.append(rangeQuery)
            else:
                for i in range(0, len(shouldQuery)):
                    if 'range' in shouldQuery[i]:
                        shouldQuery[i]['range']['hourlyCompensation']['boost'] = 2
                        shouldQuery[i]['range']['hourlyCompensation']['lte'] = candidateData['maxCompensation']
            
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
            
        esResponse = esClient.search(index="job-posting", body=esQuery, size=limit)
        
        totalResults = esResponse['_shards']['successful']
        totalPages = math.ceil(totalResults / limit)
        hits = esResponse['hits']['hits'] if len(esResponse['hits']['hits']) > 0 else []
        jobIds = []
        companyIds = set()
        
        for post in hits:
            source = post['_source']
            jobIds.append({'id': source['id'], 'createdAt': source['createdAt']})
            companyIds.add(source['companyId'])
            
        companyIds = list(companyIds)
           
        responseBody = {
            'page': page,
            'totalPages': totalPages,
            'jobs': []
        }
            
        if len(jobIds) > 0:
            jobPostsTable = dynamodb.Table('jobPosting')
            
            batch_keys = {
                jobPostsTable.name: {
                    'Keys': [{ 'id': j['id'], 'createdAt': j['createdAt'] } for j in jobIds]
                }
            }
            
            if len(companyIds) > 0:
                companiesTable = dynamodb.Table('company')
                
                batch_keys[companiesTable.name] = {
                    'Keys': [{ 'id': companyId } for companyId in companyIds]
                }
                
                dbResponse = dynamodb.batch_get_item(RequestItems=batch_keys)
                dbResponse = dbResponse['Responses']
            
                jobsData = []
                companiesData = {}
            
                if companiesTable.name in dbResponse:
                    data = dbResponse[companiesTable.name]
                    if len(data) > 0:
                        for company in data:
                            companiesData[company['id']] = company
                        
                if jobPostsTable.name in dbResponse:
                    data = dbResponse[jobPostsTable.name]
                    if len(data) > 0:
                        for job in data:
                            jobData = job
                            jobCompanyId = job['companyId']
                            companyData = companiesTable.query(KeyConditionExpression=Key('id').eq(jobCompanyId))
                            if jobCompanyId is not None and companyData['Count'] > 0:
                                jobData['companyInfo'] = companyData['Items'][0]
                        
                            jobsData.append(jobData)
                        
                responseBody['jobs'] = jobsData
                        
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
            'message': json.dumps('No recommended jobs found!')
        }