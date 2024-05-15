import os
import json
import base64
import boto3
from boto3.dynamodb.conditions import Key
import datetime

region = 'us-east-1'
ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')

def lambda_handler(event, context):
    user_id = event["queryParams"]["userId"]
    type = event["queryParams"]["type"]
    s3 = boto3.client("s3")
    filename = type + "/" + user_id + "_" + str(datetime.datetime.now().astimezone().isoformat()) + ".jpeg"
    get_file_content = event["content"]
    decode_content = base64.b64decode(get_file_content)
    s3_upload = s3.put_object(Bucket="swipply-profile-pictures",Key=filename,Body=decode_content)
    
    dynamodb = boto3.resource("dynamodb", region_name=region, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    table = dynamodb.Table(type)
    dynamo_response = table.query(KeyConditionExpression=Key("id").eq(user_id))
    candidate = dynamo_response['Items'][0]
    candidate['profilePictureS3Key'] = filename
    table.put_item(Item = candidate)

    return {
        'statusCode': 200,
        'Access-Control-Allow-Credentials': true,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps(filename)
    }