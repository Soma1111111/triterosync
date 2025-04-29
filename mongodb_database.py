#this is used to bulk upload csv file into mongodb database

import csv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://krishdave011:1234567890@cluster0.epj2ivh.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

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