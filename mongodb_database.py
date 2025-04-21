#this is used to bulk upload csv file into mongodb database

import csv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# MongoDB Atlas URI
uri = "mongodb+srv://kghungralekar1234:7NHqKqVhiLCvzwiP@cluster0.vs3p1dd.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Connect to MongoDB
client = MongoClient(uri, server_api=ServerApi('1'))

# Ping the server
try:
    client.admin.command('ping')
    print("Connected to MongoDB Atlas")
except Exception as e:
    print("Connection failed:", e)
    exit()

# Choose your database and collection
db = client['mydatabase']  # replace with your DB name
collection = db['mycollection']  # replace with your collection name

# CSV file path
csv_file_path = 'student_course_grades.csv'  # replace with your CSV file path

# Read and insert data
with open(csv_file_path, mode='r', encoding='utf-8-sig') as file:
    reader = csv.DictReader(file)
    data = list(reader)  # convert to list of dictionaries

    if data:
        result = collection.insert_many(data)
        print(f" Inserted {len(result.inserted_ids)} records into MongoDB")
    else:
        print(" CSV is empty or not readable")