import csv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

class MongoDB:
    def __init__(self, db_name='mydatabase', collection_name='mycollection'):
        self.uri = "mongodb+srv://krishdave011:1234567890@cluster0.z0tarda.mongodb.net/?retryWrites=true&w=majority&tls=true&tlsInsecure=true"
        self.db_name = db_name
        self.collection_name = collection_name
        self.reconnect()

    def reconnect(self):
        print("Connecting to MongoDB Atlas...")
        self.client = MongoClient(self.uri, server_api=ServerApi('1'))
        try:
            self.client.admin.command('ping')
            print("MongoDB: Connection successful.")
        except Exception as e:
            print("MongoDB: Connection failed on ping:", e)
            exit()

        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]

    def ensure_connection(self):
        try:
            self.client.admin.command('ping')
        except Exception as e:
            print("MongoDB: Lost connection, reconnecting...")
            self.reconnect()

    def insert_data(self, studentId, courseId, rollNo, emailId, grade):
        self.ensure_connection()
        doc = {
            "student-ID": studentId,
            "course-id": courseId,
            "roll no": rollNo,
            "email ID": emailId,
            "grade": grade
        }
        existing = self.collection.find_one({"student-ID": studentId, "course-id": courseId})
        if existing:
            print("Document already exists based on studentID and courseID.")
        else:
            result = self.collection.insert_one(doc)
            print(f"Inserted document with ID: {result.inserted_id}")

    def bulk_insert_from_csv(self, csv_file_path):
        self.ensure_connection()
        with open(csv_file_path, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            data = list(reader)
            if data:
                result = self.collection.insert_many(data)
                print(f"Inserted {len(result.inserted_ids)} records from CSV")
            else:
                print("CSV is empty or unreadable.")

    def select_data(self, studentId, courseId):
        self.ensure_connection()
        query = {"student-ID": studentId, "course-id": courseId}
        result = self.collection.find_one(query)
        if result:
            return result.get("grade", None)
        else:
            return None

    def update_data(self, studentId, courseId, new_grade):
        self.ensure_connection()
        print(f"Updating/inserting: student-ID='{studentId}', course-id='{courseId}'")
        result = self.collection.update_one(
            {"student-ID": studentId, "course-id": courseId},
            {"$set": {"grade": new_grade}},
            upsert=True
        )
        if result.matched_count > 0:
            print(f"Updated grade to '{new_grade}' for ({studentId}, {courseId})")
        elif result.upserted_id:
            print(f"Inserted new document with upsert_id: {result.upserted_id}")

    def delete_data(self, studentId, courseId):
        self.ensure_connection()
        result = self.collection.delete_one({"student-ID": studentId, "course-id": courseId})
        if result.deleted_count > 0:
            print(f"Deleted document for ({studentId}, {courseId})")
        else:
            print("No matching document found to delete.")

    def destroy(self):
        if self.client:
            self.client.close()
            print("MongoDB connection closed.")

if __name__ == "__main__":
    mongo = MongoDB("mydatabase", "mycollection")
    mongo.destroy()
