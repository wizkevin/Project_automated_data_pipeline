import os
import csv
import sys
import boto3
import pandas as pd
from dotenv import load_dotenv
from flask import Flask, jsonify
from botocore.exceptions import ClientError

from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_PIPELINE_BUCKET = os.getenv('S3_PIPELINE_BUCKET')

if AWS_ACCESS_KEY_ID is None or AWS_SECRET_ACCESS_KEY is None:
    print('AWS crendentials are missing...')
    sys.exit(-1)
    
s3client = boto3.client(
    's3',
    aws_access_key_id = AWS_ACCESS_KEY_ID,
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY
)

# create a base class for our models
Base = declarative_base()

# define a Leaderboard model
class Leaderboard(Base):
    __tablename__ = 'leaderboard'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer)
    first_name = Column(String(120))
    last_name = Column(String(120))
    number_of_messages = Column(Integer) 

json_payload = {
    "data_path": os.getenv('PIPELINE_CSV_PATH'), # Path to your pipeline_result.csv file
    "s3_bucket": os.getenv('S3_PIPELINE_BUCKET'), # Path to pipeline bucket name
    "samples_path": os.getenv('SAMPLES_PATH')
}

DB_URI = os.getenv('DB_URI') # Your DB_URI path

# Get the data_path field from the JSON payload
data_path = json_payload['data_path']
samples_path = json_payload['samples_path']

# create a database engine
engine = create_engine(DB_URI, pool_pre_ping=True)

# create the table
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)

def getFileFromBucket(file_name, bucket_name, output_path):
    # Get the file
    s3_client = s3client
    try:
        pipeline_obj = s3_client.get_object(Bucket = bucket_name, Key = file_name)
        df = pd.read_csv(pipeline_obj['Body'], sep = ",")
        return df.to_csv(f"{output_path}\pipeline_result.csv", index=False)
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise
    
    return False

app = Flask(__name__)

@app.post('/feed')
def feed_data():
    session = Session()
    
    # Open the CSV file and read the data
    with open(data_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # skip the header row
        
        # Store the data in the database
        for row in csv_reader:
            new_leaderboard = Leaderboard(user_id=row[0], first_name=row[1], last_name=row[2], number_of_messages=row[3])
            session.add(new_leaderboard)
            session.commit()
        
    session.close()
    
    return jsonify({'message': 'Data successfully fed into the database'}), 200


@app.post('/feed/s3')
def feed_data_s3():
    session = Session()
    
    file_to_find = "pipeline_result.csv"
    
    pipeline_object = s3client.list_objects(Bucket=S3_PIPELINE_BUCKET)['Contents']
    
    for index in range(len(pipeline_object)):
        if file_to_find == pipeline_object[index]['Key']:
            getFileFromBucket(pipeline_object[index]['Key'], S3_PIPELINE_BUCKET, samples_path)
            
            # Open the CSV file and read the data
            with open(data_path, 'r') as csv_file:
                csv_reader = csv.reader(csv_file)
                next(csv_reader)  # skip the header row
                
                # Store the data in the database
                for row in csv_reader:
                    new_leaderboard = Leaderboard(user_id=row[0], first_name=row[1], last_name=row[2], number_of_messages=row[3])
                    session.add(new_leaderboard)
                    session.commit()
        else:
            return jsonify({'message': 'File not found !'}), 404
        
    session.close()
    
    return jsonify({'message': 'Data successfully fed into the database'}), 200


@app.get("/leaderboard")
def get_leaderboard():
    session = Session()
    
    response = session.query(Leaderboard).all()
    
    data = {
        "leaderboard": []
    }
    
    for result in response:
        data['leaderboard'].append({
            'user_id': result.user_id,
            'first_name': result.first_name,
            'last_name': result.last_name,
            'number_of_messages': result.number_of_messages
            })
        
    session.close()
    
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=3000)