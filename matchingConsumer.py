import os
import json
from redis import Redis 
import boto3
from boto3.dynamodb.conditions import Key, Attr
import uuid
import datetime
import requests
from botocore.exceptions import ClientError

region = 'us-east-1'
ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')

redis_endpoint = 'matching.wjjuer.clustercfg.use1.cache.amazonaws.com'
redis_port = '6379'

def send_email(sqsClient, match_table, candidate_table, job_table, recruiter_table, comapny_table, initiatorId, receiverId, initiatorType, receiverType):
    candidate = None
    job = None
    company = None
    recruiter = None
    if (initiatorType == 'candidate'):
        dynamo_response = candidate_table.query(KeyConditionExpression=Key('id').eq(initiatorId))
        if (dynamo_response is not None and dynamo_response['Count'] > 0):
            candidate = dynamo_response['Items'][0]
        
        dynamo_response = job_table.query(KeyConditionExpression=Key('id').eq(receiverId))
        if (dynamo_response is not None and dynamo_response['Count'] > 0):
            job = dynamo_response['Items'][0]
            if (job is not None):
                recruiter_dynamo_response = recruiter_table.query(KeyConditionExpression=Key('id').eq(job['recruiterId']))
                if (recruiter_dynamo_response is not None and recruiter_dynamo_response['Count'] > 0):
                    recruiter = recruiter_dynamo_response['Items'][0]
                company_dynamo_response = comapny_table.query(KeyConditionExpression=Key('id').eq(job['companyId']))
                if (company_dynamo_response is not None and company_dynamo_response['Count'] > 0):
                    company = company_dynamo_response['Items'][0]
    else:
        dynamo_response = candidate_table.query(KeyConditionExpression=Key('id').eq(receiverId))
        if (dynamo_response is not None and dynamo_response['Count'] > 0):
            candidate = dynamo_response['Items'][0]
        
        dynamo_response = job_table.query(KeyConditionExpression=Key('id').eq(initiatorId))
        if (dynamo_response is not None and dynamo_response['Count'] > 0):
            job = dynamo_response['Items'][0]
            if (job is not None):
                recruiter_dynamo_response = recruiter_table.query(KeyConditionExpression=Key('id').eq(job['recruiterId']))
                if (recruiter_dynamo_response is not None and recruiter_dynamo_response['Count'] > 0):
                    recruiter = recruiter_dynamo_response['Items'][0]
                company_dynamo_response = comapny_table.query(KeyConditionExpression=Key('id').eq(job['companyId']))
                if (company_dynamo_response is not None and company_dynamo_response['Count'] > 0):
                    company = company_dynamo_response['Items'][0]
                    
    print('sqs', candidate, recruiter, company, job)
    
    # if (candidate is not None and recruiter is not None and company is not None and job is not None):
    #     try:
    #         print('sending email')
    #         response = sesClient.send_email(
    #             Destination={
    #                 'ToAddresses': [
    #                     candidate['email'],
    #                 ],
    #             },
    #             Message={
    #                 'Body': {
    #                     'Text': {
    #                         'Charset': 'UTF-8',
    #                         'Data': 'Match with ' + (company['name'] if company['name'] is not None else '') + ' for job: ' + (job['title'] if job['title'] is not None else '') + '. Connect with ' + (recruiter['firstname'] if recruiter['firstname'] is not None else '') + ' ' + (recruiter['lastname'] if recruiter['lastname'] is not None else '') + ' on their email: ' + (recruiter['email'] if recruiter['email'] is not None else '') + ' or chat with them on Swipply!',
    #                     },
    #                 },
    #                 'Subject': {
    #                     'Charset': 'UTF-8',
    #                     'Data': 'You have been matched with ' + (company['name'] if company['name'] is not None else ''),
    #                 },
    #             },
    #             Source='siddharth.bhardwaj2051999@gmail.com',
    #         )
    #         print(response)
                

    #     except ClientError as e:
    #         print(e.response['Error']['Message'])
    #     else:
    #         print("Email sent! Message ID:"),
    #         print(response['MessageId'])
    # else:
    #     print('data not found in the DB')
        
    
    sqsResponse = sqsClient.send_message(
        QueueUrl='https://sqs.us-east-1.amazonaws.com/992382771028/email',
        DelaySeconds=0,
        MessageAttributes={
            'from': {
                'DataType': 'String',
                'StringValue': 'siddharth.bhardwaj2051999@gmail.com'
            },
            'to': {
                'DataType': 'String',
                'StringValue': candidate['email']
            },
            'message': {
                'DataType': 'String',
                'StringValue': 'Match with ' + (company['name'] if company['name'] is not None else '') + ' for job: ' + (job['title'] if job['title'] is not None else '') + '. Connect with ' + (recruiter['firstname'] if recruiter['firstname'] is not None else '') + ' ' + (recruiter['lastname'] if recruiter['lastname'] is not None else '') + ' on their email: ' + (recruiter['email'] if recruiter['email'] is not None else '') + ' or chat with them on Swipply!'
            },
            'subject': {
                'DataType': 'String',
                'StringValue': 'You have been matched with ' + (company['name'] if company['name'] is not None else '')
            }
            # 'metadata': {
            #     'matchPercentage': '', CALC IN BACKEND FOR SECURITY
            #     'region': '',
            # }
        },
        MessageBody=(
            'emailData'
        )
    )
    
    print('sqs email message 1 sent', sqsResponse)
    
    sqsResponse = sqsClient.send_message(
        QueueUrl='https://sqs.us-east-1.amazonaws.com/992382771028/email',
        DelaySeconds=0,
        MessageAttributes={
            'from': {
                'DataType': 'String',
                'StringValue': 'siddharth.bhardwaj2051999@gmail.com'
            },
            'to': {
                'DataType': 'String',
                'StringValue': recruiter['email']
            },
            'message': {
                'DataType': 'String',
                'StringValue': 'Match with ' + (candidate['firstname'] if candidate['firstname'] is not None else '') + ' ' + (candidate['lastname'] if candidate['lastname'] is not None else '') + ' for job: ' + (job['title'] if job['title'] is not None else '') + 'at ' + (company['name'] if company['name'] is not None else '') + '. Connect with them' + ' on their email: ' + (candidate['email'] if candidate['email'] is not None else '') + ' or chat with them on Swipply!'
            },
            'subject': {
                'DataType': 'String',
                'StringValue': 'You have been matched with ' + (candidate['firstname'] if candidate['firstname'] is not None else '') + ' ' + (candidate['lastname'] if candidate['lastname'] is not None else '')
            }
            # 'metadata': {
            #     'matchPercentage': '', CALC IN BACKEND FOR SECURITY
            #     'region': '',
            # }
        },
        MessageBody=(
            'emailData'
        )
    )
    
    print('sqs email message 2 sent', sqsResponse)

def lambda_handler(event, context):
    sqsClient = boto3.client('sqs', endpoint_url='https://sqs.us-east-1.amazonaws.com')
    sesClient = boto3.client('ses', region_name = 'us-east-1')
    dynamodb = boto3.resource(
                    'dynamodb',
                    region_name=region,
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY
                )
    table = dynamodb.Table('matches')
    candidate_table = dynamodb.Table('candidates')
    job_table = dynamodb.Table('jobPosting')
    recruiter_table = dynamodb.Table('recruiters')
    comapny_table = dynamodb.Table('company')
    
    print('Starting redis connection')
    redis = Redis(host=redis_endpoint, port=redis_port, decode_responses=True)
    
    for message in event['Records']:
        matchInitiator = message['messageAttributes']['matchInitiator']['stringValue']
        matchReceiver = message['messageAttributes']['matchReceiver']['stringValue']
        initiatorType = message['messageAttributes']['initiatorType']['stringValue'] #candidate
        receiverType = message['messageAttributes']['receiverType']['stringValue'] #job
        # receiptHandle = message['receiptHandle']
        # sqsClient.delete_message(QueueUrl='https://sqs.us-east-1.amazonaws.com/992382771028/matching-queue', ReceiptHandle=receiptHandle)
        
        #TODO: store extra info in redis key to avoid redundant call to dynamoDB
        redis_entry = redis.exists(matchReceiver + "_" + matchInitiator)
        print('redis', redis_entry)
        if (redis.exists(matchReceiver + "_" + matchInitiator)):
            print('found entry in redis');
            # SNS/SES push
            send_email(sqsClient, table, candidate_table, job_table, recruiter_table, comapny_table, matchInitiator, matchReceiver, initiatorType, receiverType)
            redis.delete(matchReceiver + "_" + matchInitiator)
            
            dynamodb_existing = table.query(IndexName='initiator-receiver-index', KeyConditionExpression=Key('initiator').eq(matchInitiator) & Key('receiver').eq(matchReceiver), Select='ALL_ATTRIBUTES')
            dynamodb_entry = None
            if (dynamodb_existing['Count'] > 0):
                for dynamodb_item in dynamodb_existing['Items']:
                    dynamodb_entry = dynamodb_item
                        
            if (dynamodb_entry is not None):
                dynamodb_entry['fulfilled'] = 1
                dynamodb_entry['updatedAt'] = str(datetime.datetime.now().astimezone().isoformat())
                dbresponse = table.put_item(Item = dynamodb_entry)
            else:
                match = {
                    'id': str(uuid.uuid4()),
                    'initiator': matchInitiator,
                    'receiver': matchReceiver,
                    'fulfilled': 1,
                    # 'matchScore': 'HIGH', # TODO: get matchScore
                    'createdAt': str(datetime.datetime.now().astimezone().isoformat()),
                    'updatedAt': str(datetime.datetime.now().astimezone().isoformat())
                }
                dbresponse = table.put_item(Item = match)
                print('dynamodb redis put success')
                
            dynamodb_response = table.query(IndexName='initiator-receiver-index', KeyConditionExpression=Key('initiator').eq(matchReceiver) & Key('receiver').eq(matchInitiator), Select='ALL_ATTRIBUTES')
            dynamodb_entry = None
            if (dynamodb_response['Count'] > 0):
                for dynamodb_item in dynamodb_response['Items']:
                    print('TEST', dynamodb_item)
                    if (dynamodb_item['fulfilled'] is not None and dynamodb_item['fulfilled'] == 0):
                        dynamodb_entry = dynamodb_item
                        
            if (dynamodb_entry is not None):
                dynamodb_entry['fulfilled'] = 1
                dynamodb_entry['updatedAt'] = str(datetime.datetime.now().astimezone().isoformat())
                dbresponse = table.put_item(Item = dynamodb_entry)

            print('dynamodb redis fulfilled success')
            return {
                'statusCode': 200,
                'body': json.dumps('Hello from Lambda!')
            }
            
        print('initiator', matchInitiator, 'receiver', matchReceiver)
        dynamodb_response = table.query(IndexName='initiator-receiver-index', KeyConditionExpression=Key('initiator').eq(matchReceiver) & Key('receiver').eq(matchInitiator), Select='ALL_ATTRIBUTES')
        print('dbresponse', dynamodb_response)
        if (dynamodb_response is not None and dynamodb_response['Count'] > 0):
            print(dynamodb_response);
            
            # SNS/SES push
            send_email(sqsClient, table, candidate_table, job_table, recruiter_table, comapny_table, matchInitiator, matchReceiver, initiatorType, receiverType)
            
            dynamodb_existing = table.query(IndexName='initiator-receiver-index', KeyConditionExpression=Key('initiator').eq(matchInitiator) & Key('receiver').eq(matchReceiver), Select='ALL_ATTRIBUTES')
            dynamodb_entry = None
            if (dynamodb_existing['Count'] > 0):
                for dynamodb_item in dynamodb_existing['Items']:
                    dynamodb_entry = dynamodb_item
                        
            if (dynamodb_entry is not None):
                dynamodb_entry['fulfilled'] = 1
                dynamodb_entry['updatedAt'] = str(datetime.datetime.now().astimezone().isoformat())
                dbresponse = table.put_item(Item = dynamodb_entry)
            else:
                match = {
                    'id': str(uuid.uuid4()),
                    'initiator': matchInitiator,
                    'receiver': matchReceiver,
                    'fulfilled': 1,
                    # 'matchScore': 'HIGH', # TODO: get matchScore
                    'createdAt': str(datetime.datetime.now().astimezone().isoformat()),
                    'updatedAt': str(datetime.datetime.now().astimezone().isoformat())
                }
                dbresponse = table.put_item(Item = match)
                print('dynamodb put success')
            
            dynamodb_entry = None
            if (dynamodb_response['Count'] > 0):
                for dynamodb_item in dynamodb_response['Items']:
                    if (dynamodb_item['fulfilled'] == 0):
                        dynamodb_entry = dynamodb_item
            
            if (dynamodb_entry is not None):
                dynamodb_entry['fulfilled'] = 1
                dbresponse = table.put_item(Item = dynamodb_entry)
                
            print('dynamodb fulfilled success')
            return {
                'statusCode': 200,
                'body': json.dumps('Hello from Lambda!')
            }
            
        # TODO: get matchScore
        # add match to dynamodb
        dynamodb_existing = table.query(IndexName='initiator-receiver-index', KeyConditionExpression=Key('initiator').eq(matchInitiator) & Key('receiver').eq(matchReceiver), Select='ALL_ATTRIBUTES')
        dynamodb_entry = None
        if (dynamodb_existing['Count'] > 0):
            for dynamodb_item in dynamodb_existing['Items']:
                dynamodb_entry = dynamodb_item
                
        if (dynamodb_entry is not None):
            dynamodb_entry['fulfilled'] = 0
            dynamodb_entry['updatedAt'] = str(datetime.datetime.now().astimezone().isoformat())
            dbresponse = table.put_item(Item = dynamodb_entry)
        else:     
            dbresponse = table.put_item(Item = {
                'id': str(uuid.uuid4()),
                'initiator': matchInitiator,
                'receiver': matchReceiver,
                'fulfilled': 0,
                # 'matchScore': 'HIGH',
                'createdAt': str(datetime.datetime.now().astimezone().isoformat()),
                'updatedAt': str(datetime.datetime.now().astimezone().isoformat())
            })
            print('NE: dynamodb put success')
        # TODO: get matchScore
        redis.set(name=matchInitiator + "_" + matchReceiver, value="HIGH", ex=60, nx=True) # TTL = 3 days
        print('NE: redis put success')
        return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
        }
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
