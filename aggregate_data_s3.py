import boto3
import os
import sys
import logging
import pandas as pd
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_MESSAGE_USER_BUCKET = os.getenv('S3_MESSAGE_USER_BUCKET')
S3_PIPELINE_BUCKET = os.getenv('S3_PIPELINE_BUCKET')
SAMPLES_PATH = os.getenv('SAMPLES_PATH')
PIPELINE_CSV_PATH = os.getenv('PIPELINE_CSV_PATH')

if AWS_ACCESS_KEY_ID is None or AWS_SECRET_ACCESS_KEY is None:
    print('AWS crendentials are missing...')
    sys.exit(-1)
    
s3client = boto3.client(
    's3',
    aws_access_key_id = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY
)
    
message_file = s3client.list_objects(Bucket=S3_MESSAGE_USER_BUCKET)['Contents'][0]['Key']
user_file = s3client.list_objects(Bucket=S3_MESSAGE_USER_BUCKET)['Contents'][1]['Key']
pipeline_file = s3client.list_objects(Bucket=S3_PIPELINE_BUCKET)['Contents'][0]['Key']

def getFileFromBucket(file_name, bucket_name):
    # Get the file
    s3_client = s3client
    try:
        message_obj = s3_client.get_object(Bucket = bucket_name, Key = file_name)
        df = pd.read_csv(message_obj['Body'], sep = ",")
        return df
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise
    
    return False

message_df = getFileFromBucket(message_file, S3_MESSAGE_USER_BUCKET)
user_df = getFileFromBucket(user_file, S3_MESSAGE_USER_BUCKET)

def create_pipeline(message_df, user_df, output_path):
    messages_df = message_df
    user_df = user_df
    
    user_id_list = [index for index in user_df['user_id']]
    author_id_list = [index for index in messages_df['author_id']]
    user_first_name_list = [name for name in user_df['first_name']]
    user_last_name_list = [name for name in user_df['last_name']]
    number_of_messages_list = [author_id_list.count(index) for index in user_id_list]
    
    dict_out = {
        "user_id": user_id_list,
        "first_name": user_first_name_list,
        "last_name": user_last_name_list,
        "number_of_messages": number_of_messages_list
    }
    
    pipeline_df = pd.DataFrame(dict_out).sort_values("number_of_messages")
    
    return pipeline_df.to_csv(f"{output_path}\pipeline_result.csv", index=False)

os_path = create_pipeline(message_df, user_df, SAMPLES_PATH)

def uploadFileToBucket(file_name, bucket_name, key):
    # If S3 key was not specified, use file_name
    if key is None:
        key = os.path.basename(file_name)
        
    # Upload the file
    s3_client = s3client
    try:
        return s3_client.upload_file(file_name, bucket_name, key)
    except ClientError as e:
        logging.error(e)
        return False

uploadFileToBucket(PIPELINE_CSV_PATH, S3_PIPELINE_BUCKET, pipeline_file)