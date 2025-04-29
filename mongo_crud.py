# import csv
# from pymongo.mongo_client import MongoClient
# from pymongo.server_api import ServerApi
# from datetime import datetime, timezone
# import re

# class MongoDB:
#     def __init__(self, db_name='mydatabase', collection_name='mycollection', oplog_path='oplog.mongo'):
#         self.uri = "mongodb+srv://krishdave011:1234567890@cluster0.epj2ivh.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
#         self.db_name=db_name; self.collection_name=collection_name
#         self.oplog_path=oplog_path
#         self.reconnect()

#     def reconnect(self):
#         self.client=MongoClient(self.uri, server_api=ServerApi('1'))
#         self.client.admin.command('ping')
#         self.db=self.client[self.db_name]; self.collection=self.db[self.collection_name]

#     def insert_data(self,studentId,courseId,rollNo,emailId,grade):
#         self.collection.insert_one({
#             'student-ID':studentId,'course-id':courseId,'roll no':rollNo,'email ID':emailId,'grade':grade
#         })

#     def select_data(self,studentId,courseId):
#         doc=self.collection.find_one({'student-ID':studentId,'course-id':courseId})
#         return doc.get('grade') if doc else None

#     def update_data(self,studentId,courseId,grade):
#         self.collection.update_one({
#             'student-ID':studentId,'course-id':courseId
#         },{'$set':{'grade':grade}}, upsert=True)

#     def _read_oplog(self):
#         entries=[]
#         with open(self.oplog_path) as f:
#             for L in f:
#                 ts,prefix,body=L.strip().split(',',2)
#                 entries.append((datetime.fromisoformat(ts),prefix,body))
#         return entries

#     def merge_from(self, source):
#         src=sorted(source._read_oplog(), key=lambda x:x[0])
#         tgt=self._read_oplog()
#         latest={}
#         for ts,_,body in tgt:
#             m=re.match(rf"MONGODB\.SET\(\(([^,]+),([^\)]+)\)",body)
#             if m: latest[(m.group(1),m.group(2))]=max(latest.get((m.group(1),m.group(2)),ts),ts)
#         for ts,prefix,body in src:
#             m=re.match(r"POSTGRESQL\.SET\(\(([^,]+),([^\)]+)\),([A-Za-z0-9+\-]+)\)",body)
#             if not m: m=re.match(r"HIVE\.SET\(\(([^,]+),([^\)]+)\),([A-Za-z0-9+\-]+)\)",body)
#             if not m: continue
#             sid,cid,grade=m.groups(); key=(sid,cid)
#             if latest.get(key) is None or ts>latest[key]:
#                 self.update_data(sid,cid,grade)
#                 with open(self.oplog_path,'a') as f:
#                     f.write(f"{ts.isoformat()},{prefix},MONGODB.SET(({sid},{cid}),{grade})\n")
#                 latest[key]=ts
#         print(f"Mongo: merged from {source.__class__.__name__}")

#     def destroy(self):
#         self.client.close()
#         print("Mongo closed.")
import csv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime, timezone
import re

class MongoDB:
    def __init__(self, db_name='mydatabase', collection_name='mycollection', oplog_path='oplog.mongo'):
        self.uri = "mongodb+srv://krishdave011:1234567890@cluster0.epj2ivh.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
        self.db_name=db_name; self.collection_name=collection_name
        self.oplog_path=oplog_path
        self.reconnect()

    def reconnect(self):
        self.client=MongoClient(self.uri, server_api=ServerApi('1'))
        self.client.admin.command('ping')
        self.db=self.client[self.db_name]; self.collection=self.db[self.collection_name]

    def insert_data(self,studentId,courseId,rollNo,emailId,grade):
        self.collection.insert_one({
            'student-ID':studentId,'course-id':courseId,'roll no':rollNo,'email ID':emailId,'grade':grade
        })

    def select_data(self,studentId,courseId):
        doc=self.collection.find_one({'student-ID':studentId,'course-id':courseId})
        return doc.get('grade') if doc else None

    def update_data(self,studentId,courseId,grade):
        self.collection.update_one({
            'student-ID':studentId,'course-id':courseId
        },{'$set':{'grade':grade}}, upsert=True)

    def _read_oplog(self):
        entries=[]
        with open(self.oplog_path) as f:
            for L in f:
                ts,prefix,body=L.strip().split(',',2)
                entries.append((datetime.fromisoformat(ts),prefix,body))
        return entries

    def merge_from(self, source):
        src=sorted(source._read_oplog(), key=lambda x:x[0])
        tgt=self._read_oplog()
        latest={}
        for ts,_,body in tgt:
            m=re.match(rf"MONGODB\.SET\(\(([^,]+),([^\)]+)\)",body)
            if m: latest[(m.group(1),m.group(2))]=max(latest.get((m.group(1),m.group(2)),ts),ts)
        for ts,prefix,body in src:
            m=re.match(r"POSTGRESQL\.SET\(\(([^,]+),([^\)]+)\),([A-Za-z0-9+\-]+)\)",body)
            if not m: m=re.match(r"HIVE\.SET\(\(([^,]+),([^\)]+)\),([A-Za-z0-9+\-]+)\)",body)
            if not m: continue
            sid,cid,grade=m.groups(); key=(sid,cid)
            if latest.get(key) is None or ts>latest[key]:
                self.update_data(sid,cid,grade)
                with open(self.oplog_path,'a') as f:
                    f.write(f"{ts.isoformat()},{prefix},MONGODB.SET(({sid},{cid}),{grade})\n")
                latest[key]=ts
        print(f"Mongo: merged from {source.__class__.__name__}")
    def _read_oplog2(self):
        entries = []
        with open(self.oplog_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue  # skip empty lines
                prefix, _, body = line.partition(',')
                entries.append((prefix, body))
        return entries

    def merge_from2(self, source):
        src = source._read_oplog2()
        tgt = self._read_oplog2()
        latest = {}

        # Helper to map grades to numbers for comparison
        grade_order = {'F': 0, 'D': 1, 'C': 2, 'B': 3, 'A': 4} 

        # process target entries
        for counter, body in tgt:
            m = re.match(r"MONGODB\.SET\(\(([^,]+),([^\)]+)\),([A-Za-z0-9+\-]+)\)", body)
            if m:
                sid, cid, grade = m.groups()
                key = (counter, sid, cid)
                if key not in latest or grade_order.get(grade, -1) > grade_order.get(latest[key], -1):
                    latest[key] = grade

        # process source entries
        for counter, body in src:
            m = re.match(r"POSTGRESQL\.SET\(\(([^,]+),([^\)]+)\),([A-Za-z0-9+\-]+)\)", body)
            if not m:
                m = re.match(r"HIVE\.SET\(\(([^,]+),([^\)]+)\),([A-Za-z0-9+\-]+)\)", body)
            if not m:
                continue
            sid, cid, grade = m.groups()
            key = (counter, sid, cid)
            if key not in latest or grade_order.get(grade, -1) > grade_order.get(latest[key], -1):
                self.update_data(sid, cid, grade)
                with open(self.oplog_path, 'a') as f:
                    f.write(f"{counter},MONGODB.SET(({sid},{cid}),{grade})\n")
                latest[key] = grade

        print(f"Mongo: merged from {source.__class__.__name__}")
    def merge_from3(self, source):
        src = source._read_oplog2()
        tgt = self._read_oplog2()

        # Priority order: lower value = higher priority
        server_priority = {'MONGODB': 1, 'POSTGRESQL': 2, 'HIVE': 3}

        # key = (sid, cid), value = (grade, server)
        latest = {}

        # Helper function to extract info from SET statements
        def extract_info(body):
            m = re.match(r"([A-Z]+)\.SET\(\(([^,]+),([^\)]+)\),([A-Za-z0-9+\-]+)\)", body)
            if m:
                return m.groups()  # (server, sid, cid, grade)
            return None

        # Process target (Mongo's own) entries
        for counter, body in tgt:
            info = extract_info(body)
            if info:
                server, sid, cid, grade = info
                key = (sid, cid)
                latest[key] = (grade, server)

        # Process source entries
        for counter, body in src:
            info = extract_info(body)
            if not info:
                continue
            server, sid, cid, grade = info
            key = (sid, cid)

            # Update only if current server has higher priority
            if key not in latest or server_priority.get(server, float('inf')) < server_priority.get(latest[key][1], float('inf')):
                self.update_data(sid, cid, grade)
                with open(self.oplog_path, 'a') as f:
                    f.write(f"{counter},MONGODB.SET(({sid},{cid}),{grade})\n")
                latest[key] = (grade, 'MONGODB')

        print(f"Mongo: merged from {source.__class__.__name__}")


    def destroy(self):
        self.client.close()
        print("Mongo closed.")