import json
import boto3
import os

region = 'us-east-1'
ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')

def send_email(sesClient, match_table, candidate_table, job_table, recruiter_table, comapny_table, fromEmail, toEmail, message, subject):
    if (fromEmail is not None and toEmail is not None and message is not None and subject is not None):
        try:
            print('sending email')
            response = sesClient.send_email(
                Destination={
                    'ToAddresses': [
                        toEmail,
                    ],
                },
                Message={
                    'Body': {
                        'Text': {
                            'Charset': 'UTF-8',
                            'Data': message
                        },
                    },
                    'Subject': {
                        'Charset': 'UTF-8',
                        'Data': subject
                    },
                },
                Source=fromEmail,
            )
            print(response)
                
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            print("Email sent! Message ID:"),
            print(response['MessageId'])
    else:
        print('missing data', fromEmail, toEmail, message, subject)
    
def lambda_handler(event, context):
    # TODO implement
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
    
    for message in event['Records']:
        print(json.dumps(message))
        fromEmail = message['messageAttributes']['from']['stringValue']
        toEmail = message['messageAttributes']['to']['stringValue']
        emailMessage = message['messageAttributes']['message']['stringValue']
        subject = message['messageAttributes']['subject']['stringValue']
        
        send_email(sesClient, table, candidate_table, job_table, recruiter_table, comapny_table, fromEmail, toEmail, emailMessage, subject)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
