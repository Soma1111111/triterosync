#this is the file which is used to do crud operations for mongo db.

import csv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

class MongoDB:
    def __init__(self, db_name='mydatabase', collection_name='mycollection'):
        uri = "mongodb+srv://kghungralekar1234:7NHqKqVhiLCvzwiP@cluster0.vs3p1dd.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
        self.client = MongoClient(uri, server_api=ServerApi('1'))
        try:
            self.client.admin.command('ping')
            print("Connected to MongoDB Atlas")
        except Exception as e:
            print("Connection failed:", e)
            exit()
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def insert_data(self, studentId, courseId, rollNo, emailId, grade):
        doc = {
        "student-ID": studentId,
        "course-id": courseId,
        "roll no": rollNo,
        "email ID": emailId,
        "grade": grade
        }
        #Check for exact match
        existing = self.collection.find_one(doc)
        if existing:
            print("Document already exists in the database. Duplicate not inserted.")
        else:
            result = self.collection.insert_one(doc)
            print(f"Inserted document with ID: {result.inserted_id}")


    def bulk_insert_from_csv(self, csv_file_path):
        with open(csv_file_path, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            data = list(reader)

            if data:
                result = self.collection.insert_many(data)
                print(f"Inserted {len(result.inserted_ids)} records from CSV")
            else:
                print("CSV is empty or not readable")

    def select_data(self, studentId, courseId):
        print(f"Looking for: student-ID='{studentId}', course-id='{courseId}'")
        query = {"student-ID": studentId, "course-id": courseId}
        results = self.collection.find(query)

        found_any = False
        for doc in results:
            found_any = True
            print(doc)

        if not found_any:
            print("No matching documents found.")

    def update_data(self, studentId, courseId, new_grade):
        print(f"Looking for: student-ID='{studentId}', course-id='{courseId}'")
        found = self.collection.find_one({"student-ID": studentId, "course-id": courseId})
        if found:
            print("Found document:", found)
            result = self.collection.update_one(
                {"student-ID": studentId, "course-id": courseId},
                {"$set": {"grade": new_grade}}
            )
            if result.modified_count > 0:
                print(f"Updated grade to '{new_grade}' for student '{studentId}', course '{courseId}'")
            else:
                print("Document found but no changes made (grade might be the same)")
        else:
            print("No matching document found for update")

    def delete_data(self, studentId, courseId):
        print(f"Attempting delete for: student-ID='{studentId}', course-id='{courseId}'")
        result = self.collection.delete_one(
            {"student-ID": studentId, "course-id": courseId}
        )
        if result.deleted_count > 0:
            print(f"Deleted document for student '{studentId}', course '{courseId}'")
        else:
            print("No matching document found to delete")

    def destroy(self):
        self.client.close()
        print("MongoDB connection closed")

# --------------------------
# Main Test Block
# --------------------------
if __name__ == "__main__":
    mongo = MongoDB("mydatabase", "mycollection")
    # Test inserts
    mongo.insert_data("SID1036", "CSE016", "CRPC2ZW9", "crpc2zw9@university.edu", "A")
    # Update a grade
    mongo.update_data("SID1035", "CSE016", "A+")
    # Delete a document
    mongo.delete_data("SID1035", "CSE016")

    # View all documents
    mongo.select_all()

    # 🔌 Close connection
    mongo.destroy()