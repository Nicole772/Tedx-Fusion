import requests
import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, count
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pymongo import MongoClient

# Parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

tags_dataset_path = "s3://tedx-2025-data/tags.csv"
user_activity_path = "s3://tedx-2025-data/user_activity.csv"  # Example user activity dataset

# Spark Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load datasets
tags_dataset = spark.read.option("header", "true").csv(tags_dataset_path)
user_activity = spark.read.option("header", "true").csv(user_activity_path)

# Aggregate tags per video
tags_dataset_agg = tags_dataset.groupBy(col("id").alias("id_ref")).agg(collect_list("tag").alias("tags"))

# Aggregate user preferences
user_preferences = user_activity.groupBy("user_id").agg(
    collect_list("video_id").alias("watched_videos"),
    collect_list("tag").alias("watched_tags"),
    count("tag").alias("tag_count")
)

# MongoDB Atlas Connection
mongo_uri = "mongodb+srv://admin:admin@cluster0.mongodb.net/unibg_tedx_2024?retryWrites=true&w=majority"
mongo_client = MongoClient(mongo_uri)
db = mongo_client["unibg_tedx_2024"]
user_preferences_collection = db["user_preferences"]
recommended_courses_collection = db["recommended_courses"]

# Update user preferences in MongoDB
for row in user_preferences.collect():
    user_preferences_collection.update_one(
        {"user_id": row["user_id"]},
        {"$set": {"watched_videos": row["watched_videos"], "watched_tags": row["watched_tags"]}},
        upsert=True
    )

def fetch_courses_from_api(tag):
    """
    Fetches courses related to a given tag from multiple e-learning platforms.
    """
    platforms = {
        "coursera": f"https://api.coursera.org/api/courses.v1?q=search&query={tag}",
        "udemy": f"https://www.udemy.com/api-2.0/courses/?search={tag}",
        "edx": f"https://www.edx.org/api/v1/catalog/search?q={tag}"
    }
    
    courses = []
    
    for platform, url in platforms.items():
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                for course in data.get("elements", []):
                    courses.append({
                        "title": course.get("name"),
                        "url": course.get("slug"),
                        "platform": platform.capitalize()
                    })
        except Exception as e:
            print(f"Error fetching courses for tag {tag} on {platform}: {str(e)}")
    
    return courses

def recommend_courses_for_users():
    """
    Fetches most watched tags for each user and suggests courses based on those tags.
    """
    users = user_preferences_collection.find()
    for user in users:
        user_id = user["user_id"]
        watched_tags = user.get("watched_tags", [])
        recommended_courses = []
        
        for tag in watched_tags:
            recommended_courses.extend(fetch_courses_from_api(tag))
        
        recommended_courses_collection.update_one(
            {"user_id": user_id},
            {"$set": {"courses": recommended_courses}},
            upsert=True
        )

# Execute recommendations
recommend_courses_for_users()

job.commit()
