import psycopg2
from psycopg2 import sql
from datetime import datetime, timezone
import re

class PostgreSQL:
    def __init__(self, dbname, user, password, host='localhost', port=5432,
                 table_name='grades', oplog_path='oplog.sql'):
        self.table_name=table_name; self.oplog_path=oplog_path
        self.conn=psycopg2.connect(dbname=dbname,user=user,password=password,host=host,port=port)
        self.cursor=self.conn.cursor()

    def insert_data(self, sid, cid, grade):
        self.cursor.execute(sql.SQL(
            "INSERT INTO {t}(student_id,course_id,roll_no,email_id,grade)"
            " VALUES(%s,%s,NULL,NULL,%s) ON CONFLICT (student_id,course_id)"
            " DO UPDATE SET grade=EXCLUDED.grade"
        ).format(t=sql.Identifier(self.table_name)), (sid,cid,grade))
        self.conn.commit()

    def select_data(self, sid, cid):
        self.cursor.execute(sql.SQL(
            "SELECT grade FROM {t} WHERE student_id=%s AND course_id=%s"
        ).format(t=sql.Identifier(self.table_name)),(sid,cid))
        r=self.cursor.fetchone(); return r[0] if r else None

    def update_data(self, sid, cid, grade):
        # reuse insert_data for upsert
        self.insert_data(sid,cid,grade)

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
            m=re.match(r"POSTGRESQL\.SET\(\(([^,]+),([^\)]+)\)",body)
            if m: latest[(m.group(1),m.group(2))]=max(latest.get((m.group(1),m.group(2)),ts),ts)
        for ts,prefix,body in src:
            m=re.match(r"(HIVE|MONGODB)\.SET\(\(([^,]+),([^\)]+)\),([A-Za-z0-9+\-]+)\)",body)
            if not m: continue
            _,sid,cid,grade=m.groups(); key=(sid,cid)
            if latest.get(key) is None or ts>latest[key]:
                self.insert_data(sid,cid,grade)
                with open(self.oplog_path,'a') as f:
                    f.write(f"{ts.isoformat()},{prefix},POSTGRESQL.SET(({sid},{cid}),{grade})\n")
                latest[key]=ts
        print(f"Postgres: merged from {source.__class__.__name__}")

    def destroy(self): self.cursor.close(); self.conn.close()