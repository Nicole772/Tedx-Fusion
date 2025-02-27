import boto3
import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pymongo import MongoClient

# Parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

transcripts_dataset_path = "s3://tedx-2025-data/transcripts.csv"

# Spark Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load transcripts dataset
transcripts_dataset = spark.read.option("header", "true").csv(transcripts_dataset_path)

# MongoDB Connection
mongo_client = MongoClient("mongodb://localhost:27017/")  # Update with actual connection string
db = mongo_client["unibg_tedx_2024"]
highlights_collection = db["video_highlights"]

# AWS Comprehend client
comprehend = boto3.client('comprehend', region_name='us-east-1')

def analyze_sentiment(text):
    """
    Uses AWS Comprehend to analyze sentiment in the given text.
    """
    response = comprehend.detect_sentiment(Text=text, LanguageCode='en')
    return response['Sentiment']

def extract_key_phrases(text):
    """
    Uses AWS Comprehend to extract key phrases from the given text.
    """
    response = comprehend.detect_key_phrases(Text=text, LanguageCode='en')
    return [phrase['Text'] for phrase in response['KeyPhrases']]

# Process each transcript
for row in transcripts_dataset.collect():
    video_id = row['id']
    transcript_text = row['transcript']
    
    sentiment = analyze_sentiment(transcript_text)
    key_phrases = extract_key_phrases(transcript_text)
    
    highlights_collection.update_one(
        {"video_id": video_id},
        {"$set": {"sentiment": sentiment, "highlights": key_phrases}},
        upsert=True
    )

# Finalize job
job.commit()
