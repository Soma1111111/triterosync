#!/usr/bin/env python3
import re, sys
from datetime import datetime, timezone

# 1) import your CRUD modules
import hive_crud as hc
import mongo_crud as mc
import postgre_crud as pc

# 2) initialize connectors
hive = hc.HiveCRUD(table_name="student_course")

postgre = pc.PostgreSQL(
    dbname="trifectanosql",
    user="ketan1",
    password="1234567890",
    table_name="grades"
)

mongo = mc.MongoDB(db_name="mydatabase", collection_name="mycollection")

# 3) map DB name → (connector, oplog path)
DB_MAP = {
    "HIVE":       (hive,   "oplog.hiveql"),
    "POSTGRESQL": (postgre,"oplog.sql"),
    "MONGODB":    (mongo,  "oplog.mongo"),
}

# 4) clear old logs so each run starts fresh
for _, path in DB_MAP.values():
    open(path, "w").close()

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def write_log(dbn: str, prefix: str, op_text: str, ts: str=None):
    """
    Append one entry to dbn’s oplog in the form:
      <ISO timestamp>,<prefix>,<operation>
    If ts is provided, use it; otherwise use current time.
    """
    if ts is None:
        ts = now_iso()
    _, path = DB_MAP[dbn]
    with open(path, "a") as f:
        f.write(f"{ts},{prefix},{op_text}\n")

def parse_log(path):
    """
    Read a log file, returning a list of tuples:
      (timestamp:datetime, prefix:str, body:str)
    """
    entries = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line: continue
            ts_str, prefix, body = line.split(",", 2)
            ts = datetime.fromisoformat(ts_str)
            entries.append((ts, prefix, body))
    return entries

def latest_target_ts(dbn, sid, cid):
    """
    Scan dbn’s oplog for the latest SET((sid,cid),*) entry and return its timestamp,
    or None if no such entry exists.
    """
    _, path = DB_MAP[dbn]
    last = None
    for ts, _, body in parse_log(path):
        # look only at SETs
        if body.startswith(f"{dbn}.SET(("):
            m = re.match(rf"{dbn}\.SET\(\(\s*{re.escape(sid)}\s*,\s*{re.escape(cid)}\s*\)\s*,", body)
            if m:
                if last is None or ts > last:
                    last = ts
    return last

# 5) SET: upsert-aware
def process_set(prefix, dbn, sid, cid, grade):
    db, _ = DB_MAP[dbn]
    # always upsert at source
    if dbn == "HIVE":
        db.update_data(sid, cid, grade)
    elif dbn == "POSTGRESQL":
        db.insert_data(sid, cid, grade)
    else:  # MONGODB
        db.update_data(sid, cid, grade)
    # log with current timestamp
    write_log(dbn, prefix, f"{dbn}.SET(({sid},{cid}),{grade})")

# 6) GET: purely for user visibility (no effect on merge)
def process_get(prefix, dbn, sid, cid):
    db, _ = DB_MAP[dbn]
    grade = db.select_data(sid, cid)
    print(f"{prefix}, {dbn}.GET({sid},{cid}) → {grade}")
    write_log(dbn, prefix, f"{dbn}.GET({sid},{cid})")

# 7) MERGE: LWW from source → target
def perform_merge(source, target):
    src_db, src_log = DB_MAP[source]
    _, _            = DB_MAP[target]

    # 1) load and sort source entries by timestamp ascending
    entries = parse_log(src_log)
    entries.sort(key=lambda x: x[0])

    for ts, prefix, body in entries:
        # only care about SET ops
        m = re.match(
            rf"{source}\.SET\(\(\s*([^,]+)\s*,\s*([^)]+)\s*\)\s*,\s*([A-Za-z0-9\+\-]+)\)",
            body
        )
        if not m:
            continue
        sid, cid, grade = m.groups()

        # 2) compare with target’s last timestamp for this key
        last_ts = latest_target_ts(target, sid, cid)
        if last_ts is None or ts > last_ts:
            # apply this SET to the target
            tgt_db, _ = DB_MAP[target]
            if target == "HIVE":
                tgt_db.update_data(sid, cid, grade)
            elif target == "POSTGRESQL":
                tgt_db.insert_data(sid, cid, grade)
            else:  # MONGODB
                tgt_db.update_data(sid, cid, grade)
            # 3) record in target log *with the original source timestamp*
            write_log(target, prefix, f"{target}.SET(({sid},{cid}),{grade})", ts=ts)

    print(f"[MERGE] {source} → {target} (LWW by timestamp) done.")

# 8) parse & dispatch commands
def main(cmdfile):
    with open(cmdfile) as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue

            # MERGE(target,source)
            mm = re.match(r"^(\w+)\.MERGE\(\s*(\w+)\s*\)$", line)
            if mm:
                tgt, src = mm.groups()
                tgt, src = tgt.upper(), src.upper()
                if src in DB_MAP and tgt in DB_MAP:
                    perform_merge(src, tgt)
                else:
                    print(f"[ERROR] Unknown MERGE: {line}")
                continue

            # <prefix>, DB.SET or DB.GET
            if "," in line:
                prefix, cmd = [p.strip() for p in line.split(",",1)]

                sm = re.match(
                    r"^(\w+)\.SET\(\(\s*([^,]+)\s*,\s*([^)]+)\s*\)\s*,\s*([A-Za-z0-9\+\-]+)\)$",
                    cmd
                )
                if sm:
                    dbn, sid, cid, grade = sm.groups()
                    process_set(prefix, dbn.upper(), sid, cid, grade)
                    continue

                gm = re.match(r"^(\w+)\.GET\(\s*([^,]+)\s*,\s*([^)]+)\s*\)$", cmd)
                if gm:
                    dbn, sid, cid = gm.groups()
                    process_get(prefix, dbn.upper(), sid, cid)
                    continue

            print(f"[WARN] couldn’t parse line: {line}")

if __name__ == "__main__":
    if len(sys.argv)!=2:
        print("Usage: python sync_krish.py <commands_file>")
        sys.exit(1)
    main(sys.argv[1])
