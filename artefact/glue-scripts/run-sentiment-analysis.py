"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
import json
import boto3
import gzip
import io
import botocore
import tarfile
import csv
from tarfile import TarInfo
import pandas as pd
import ast
import pandas as pd
import re
import sys
import time
from awsglue.utils import getResolvedOptions



# Get the arguments that are passed to the script
args = getResolvedOptions(sys.argv, ['sourceloc','comprehendrole'])


# Get s3 client and resource
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
client_comp = boto3.client('comprehend')
client_glue = boto3.client('glue')


# Arguments related to the metadata
input_bucket_name, key_obj  = args['sourceloc'].split('/', 1)

filename = key_obj.rsplit('/', 1)

# read the source csv data
obj_client = s3_client.get_object(Bucket=input_bucket_name, Key=key_obj)
df_input = pd.read_csv(io.BytesIO(obj_client['Body'].read()))
print(df_input.head(5))

new_file_input_loc = 'staging/input/movie_review.csv'
new_file_output_loc = 'staging/output/'

#create a file with text data only on which sentiment analysis will run
csv_buffer = io.StringIO()
df_input['review_text'].to_csv(csv_buffer,index=False)
s3_resource.Object(input_bucket_name, new_file_input_loc).put(Body=csv_buffer.getvalue())

#create sentiment analysis job using aws comprehend
response_start = client_comp.start_sentiment_detection_job(
InputDataConfig={
    'S3Uri': 's3://'+ input_bucket_name + '/' + new_file_input_loc,
    'InputFormat': 'ONE_DOC_PER_LINE'
},
OutputDataConfig={
    'S3Uri': 's3://'+ input_bucket_name + '/' + new_file_output_loc,
},
DataAccessRoleArn=args['comprehendrole'],
JobName='sentiment-analysis',
LanguageCode='en'
)

job_id = response_start['JobId']
status_job = response_start['JobStatus']

#check the the job to complete
while status_job != 'COMPLETED':
    time.sleep(30)
    response_desc = client_comp.describe_sentiment_detection_job(
        JobId=job_id
    )
    status_job = response_desc['SentimentDetectionJobProperties']['JobStatus']
    if status_job == 'FAILED' :
        raise ValueError('Comprehend Job Failed')
    print(response_desc['SentimentDetectionJobProperties']['JobStatus'])





input_bucket = s3_resource.Bucket(input_bucket_name)

#the output file is a tar file , so need to unzip it
for obj in input_bucket.objects.filter(Prefix=new_file_output_loc):
    if obj.key.endswith('.tar.gz'):
        key_obj = obj.key

new_key =  new_file_output_loc + 'results.txt'


s3_client.download_file(input_bucket_name, key_obj, '/tmp/file')
if(tarfile.is_tarfile('/tmp/file')):
    tar = tarfile.open('/tmp/file', "r:gz")
    for TarInfo in tar:
        tar.extract(TarInfo.name, path='/tmp/extract/')
s3_client.upload_file('/tmp/extract/'+TarInfo.name,input_bucket_name, new_key)
tar.close()


#read the initial raw file and merge it with sentiment data generated from comprehend
obj = s3_resource.Object(input_bucket_name, new_key)
obj_text = obj.get()['Body'].read().decode('utf8')


df = pd.DataFrame()
new_dict = dict()

#convert sentiment data in json to columnar format
for line in obj_text.rstrip('\n').split('\n'):
    obj_dict = json.loads(line)

    new_dict.update({'File': obj_dict['File']})
    new_dict.update({'Line': obj_dict['Line']})
    new_dict.update({'Sentiment': obj_dict['Sentiment']})
    new_dict.update({'Mixed_Score': obj_dict['SentimentScore']['Mixed']})
    new_dict.update({'Negative_Score': obj_dict['SentimentScore']['Negative']})
    new_dict.update({'Neutral_Score': obj_dict['SentimentScore']['Neutral']})
    new_dict.update({'Positive_Score': obj_dict['SentimentScore']['Positive']})
    df = df.append(pd.DataFrame([new_dict], columns=new_dict.keys()))




df_input['index1'] = df_input.index
df_output = pd.merge(df_input, df, left_on='index1', right_on='Line', how='inner').drop('index1', axis=1)

#write final output to s3
csv_buffer = io.StringIO()
df_output.to_csv(csv_buffer,index=False)
s3_resource.Object(input_bucket_name, 'staging/movie_review/sentiment_data.csv').put(Body=csv_buffer.getvalue())


