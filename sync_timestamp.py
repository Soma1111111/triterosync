import re, sys
from hive_crud import HiveCRUD
from mongo_crud import MongoDB
from postgre_crud import PostgreSQL

# instantiate connectors with their oplog paths
hive   = HiveCRUD(oplog_path='oplog.hiveql')
postgre= PostgreSQL(dbname='trifectanosql', user='ketan1', password='1234567890', table_name='grades', oplog_path='oplog.sql')
mongo  = MongoDB(db_name='mydatabase', collection_name='mycollection', oplog_path='oplog.mongo')

CONNECTORS = {'HIVE':hive,'POSTGRESQL':postgre,'MONGODB':mongo}

# clear logs
for c in CONNECTORS.values(): open(c.oplog_path,'w').close()

# common log writer
from datetime import datetime, timezone

def now_iso(): return datetime.now(timezone.utc).isoformat()

def write_log(conn_name,prefix,op,ts=None):
    if ts is None: ts=now_iso()
    path=CONNECTORS[conn_name].oplog_path
    with open(path,'a') as f: f.write(f"{ts},{prefix},{op}\n")

# dispatch SET/GET

def process_set(prefix,dbn,sid,cid,grade):
    c=CONNECTORS[dbn]; c.update_data(sid,cid,grade)
    write_log(dbn,prefix,f"{dbn}.SET(({sid},{cid}),{grade})")


def process_get(prefix,dbn,sid,cid):
    c=CONNECTORS[dbn]; g=c.select_data(sid,cid)
    print(f"{prefix}, {dbn}.GET({sid},{cid}) → {g}")
    write_log(dbn,prefix,f"{dbn}.GET({sid},{cid})")

# merge simply calls connector.merge_from()

def perform_merge(src_name,tgt_name):
    src=CONNECTORS[src_name]; tgt=CONNECTORS[tgt_name]
    tgt.merge_from(src)

# main parser

def main(path):
    for L in open(path):
        line=L.strip()
        if not line or line.startswith('#'): continue
        m=re.match(r"^(\w+)\.MERGE\((\w+)\)$",line)
        if m:
            tgt,src=m.groups(); perform_merge(src,tgt)
            continue
        if ',' in line:
            pre,cmd=[x.strip() for x in line.split(',',1)]
            sm=re.match(r"^(\w+)\.SET\(\(([^,]+),([^\)]+)\),([A-Za-z0-9+\-]+)\)$",cmd)
            if sm: dbn,sid,cid,gr=sm.groups(); process_set(pre,dbn,sid,cid,gr); continue
            gm=re.match(r"^(\w+)\.GET\(([^,]+),([^\)]+)\)$",cmd)
            if gm: dbn,sid,cid=gm.groups(); process_get(pre,dbn,sid,cid); continue
        print(f"Unrecognized: {line}")

if __name__=='__main__':
    if len(sys.argv)!=2:
        print("Usage: sync_timestamp.py <cmdfile>"); sys.exit(1)
    main(sys.argv[1])

