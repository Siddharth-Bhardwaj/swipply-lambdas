import json
import boto3

def pushToSqs(event):
    sqsClient = boto3.client('sqs')
    
    sqsResponse = sqsClient.send_message(
        QueueUrl='https://sqs.us-east-1.amazonaws.com/992382771028/matching-queue',
        DelaySeconds=0,
        MessageAttributes={
            'matchInitiator': {
                'DataType': 'String',
                'StringValue': event['queryStringParameters']['matchInitiator']
            },
            'matchReceiver': {
                'DataType': 'String',
                'StringValue': event['queryStringParameters']['matchReceiver']
            },
            'initiatorType': {
                'DataType': 'String',
                'StringValue': event['queryStringParameters']['initiatorType']
            },
            'receiverType': {
                'DataType': 'String',
                'StringValue': event['queryStringParameters']['receiverType']
            }
            # 'metadata': {
            #     'matchPercentage': '', CALC IN BACKEND FOR SECURITY
            #     'region': '',
            # }
        },
        MessageBody=(
            'matchData'
        )
    )
    
    print(sqsResponse)
    
    if sqsResponse is not None:
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': True,
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
                'Content-Type': 'application/json'
            },
            'body': json.dumps('match pushed to sqs')
        }
    
    return {
        'statusCode': 500,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': True,
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
            'Content-Type': 'application/json'
        },
        'body': json.dumps('failed to push match to sqs')
    }

def lambda_handler(event, context):
    # TODO geo-sharding (but too expensive)
    # so we can maybe do userId based sharding (odd-even)?

    # maybe keep the producer as a service in ec2 instance
    # and use threadpool to maintain connections with DB and redis
    # NO - dynamodb queries are HTTP request response and not 
    # persistent connections like in relational databases
    return pushToSqs(event)
