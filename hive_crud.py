import os
import subprocess
import time
import uuid
import csv
import re
from datetime import datetime, timezone
from pyhive import hive

class HiveCRUD:
    def __init__(self, table_name="student_course", oplog_path="oplog.hiveql"):
        self.table_name = table_name
        self.oplog_path = oplog_path
        self._start_hiveserver()
        self._open_connection()

    def _start_hiveserver(self):
        hive_home = os.environ.get('HIVE_HOME')
        if not hive_home:
            raise RuntimeError("HIVE_HOME is not set.")
        cmd = os.path.join(hive_home, "bin", "hiveserver2")
        print("Starting HiveServer2…")
        self.hive_server = subprocess.Popen(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        time.sleep(10)

    def _open_connection(self):
        self.conn = hive.Connection(host="localhost", port=10000,
                                    username="ketan")
        self.cursor = self.conn.cursor()

    def create_table(self):
        self.cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
        self.cursor.execute(f"""
            CREATE TABLE {self.table_name} (
                student_id STRING,
                course_id  STRING,
                roll_no    STRING,
                email_id   STRING,
                grade      STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
            TBLPROPERTIES ("skip.header.line.count"="1")
        """)
        print(f"Table '{self.table_name}' created.")

    def select_data(self, studentId, courseId):
        qry = f"""
            SELECT grade
              FROM {self.table_name}
             WHERE student_id = '{studentId}'
               AND course_id  = '{courseId}'
        """
        self.cursor.execute(qry)
        res = self.cursor.fetchone()
        return res[0] if res else None

    def update_data(self, studentId, courseId, new_grade):
        # overwrite via CSV trick
        self.cursor.execute(f"SELECT student_id,course_id,roll_no,email_id,grade FROM {self.table_name}")
        rows = self.cursor.fetchall()
        tmp = f"/tmp/hive_update_{uuid.uuid4().hex}.csv"
        with open(tmp,'w',newline='') as f:
            w = csv.writer(f)
            w.writerow(['student_id','course_id','roll_no','email_id','grade'])
            for sid,cid,roll,email,grade in rows:
                if sid==studentId and cid==courseId:
                    grade=new_grade
                w.writerow([sid,cid,roll,email,grade])
        self.cursor.execute(f"LOAD DATA LOCAL INPATH '{tmp}' OVERWRITE INTO TABLE {self.table_name}")
        os.remove(tmp)
        print(f"Hive: updated ({studentId},{courseId})→{new_grade}")

    def _read_oplog(self):
        entries=[]
        with open(self.oplog_path) as f:
            for L in f:
                ts, prefix, body = L.strip().split(',',2)
                dt = datetime.fromisoformat(ts)
                entries.append((dt,prefix,body))
        return entries

    def merge_from(self, source):
        # Last-Writer-Wins based on timestamps
        src_entries = sorted(source._read_oplog(), key=lambda x:x[0])
        tgt_entries = self._read_oplog()
        # build dict of latest ts per key in target
        latest={}  # (sid,cid)->ts
        for ts,p,body in tgt_entries:
            m = re.match(rf"{re.escape(self.__class__.__name__.replace('CRUD','').upper())}\.SET\(\(([^,]+),([^\)]+)\)", body)
            if m:
                key=(m.group(1),m.group(2))
                latest[key]=max(latest.get(key,ts),ts)
        # apply
        for ts,prefix,body in src_entries:
            m = re.match(rf"{source.__class__.__name__.replace('CRUD','').upper()}\.SET\(\(([^{''}]+),([^\)]+)\),([A-Za-z0-9+\-]+)\)", body)
            # note: adjust regex if needed
            m = re.match(rf"{source.__class__.__name__.replace('CRUD','').upper()}\.SET\(\(([^,]+),([^\)]+)\),([A-Za-z0-9+\-]+)\)", body)
            if not m: continue
            sid,cid,grade = m.groups()
            key=(sid,cid)
            if latest.get(key) is None or ts>latest[key]:
                # apply
                self.update_data(sid,cid,grade)
                # log
                with open(self.oplog_path,'a') as f:
                    f.write(f"{ts.isoformat()},{prefix},{self.__class__.__name__.replace('CRUD','').upper()}.SET(({sid},{cid}),{grade})\n")
                latest[key]=ts
        print(f"Hive: merged from {source.__class__.__name__}")

    def destroy(self):
        print("Closing Hive…")
        try:
            self.cursor.close(); self.conn.close()
        except: pass
        self.hive_server.terminate()