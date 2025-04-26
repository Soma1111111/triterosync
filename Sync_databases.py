import mongo_crud as mc
import postgre_crud as pc
import hive_crud as hc
import re

# Initialize database connections
mongo = mc.MongoDB("mydatabase", "mycollection")
hive = hc.Hive("student_course")
hive.create_table()
postgre = pc.PostgreSQL(
    dbname="mydb",
    user="ketan1",
    password="Arti@1982",
    host="localhost",
    port=5432,
    table_name="grades"
)

# Mapping from database name to object and log file path
DB_MAP = {
    'MONGODB': (mongo, 'oplog.mongo'),
    'HIVE': (hive, 'oplog.hiveql'),
    'POSTGRESQL': (postgre, 'oplog.sql')
}

# Clear or create the output log files
for _, log_file in DB_MAP.values():
    open(log_file, 'w').close()

# Global counter for operations
global_counter = 1


def write_log(db_name, prefix_id, op_text):
    """
    Append a log entry to the given database's log file.
    """
    global global_counter
    _, log_file = DB_MAP[db_name]
    entry = f"{prefix_id}, {op_text}, {global_counter}"
    with open(log_file, 'a') as f:
        f.write(entry + "\n")
    global_counter += 1


def process_set(prefix_id, db_name, sid, course, grade):
    """
    Handle a SET operation: update the database and write to log.
    """
    db_obj, _ = DB_MAP[db_name]
    db_obj.update_data(sid, course, grade)
    write_log(db_name, prefix_id, f"{db_name}.SET(({sid},{course}), {grade})")


def process_get(prefix_id, db_name, sid, course):
    """
    Handle a GET operation: select from the database and print the result.
    """
    db_obj, _ = DB_MAP[db_name]
    grade = db_obj.select_data(sid, course)
    print(f"{prefix_id}, {db_name}.GET({sid},{course}) -> {grade}")


def perform_merge(target_name, source_name):
    """
    Merge all SET operations from source_name's log into target_name.
    """
    target_obj, target_log = DB_MAP[target_name]
    source_obj, source_log = DB_MAP[source_name]

    with open(source_log, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # Expect format: prefix_id, DB.SET((sid,course), grade), old_counter
            parts = [p.strip() for p in line.split(',', 2)]
            prefix_id = parts[0]
            op_text = parts[1]
            # Extract sid, course, grade
            m = re.search(r"SET\(\(([^,]+),([^\)]+)\),\s*([A-Za-z])\)", op_text)
            if m:
                sid = m.group(1)
                course = m.group(2)
                grade = m.group(3)
                # Apply to target DB
                target_obj.update_data(sid, course, grade)
                # Log in target
                write_log(target_name, prefix_id, f"{target_name}.SET(({sid},{course}), {grade})")


def main(input_file):
    with open(input_file, 'r') as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue

            # Check for MERGE without prefix
            merge_match = re.match(r"^(\w+)\.MERGE\((\w+)\)$", line)
            if merge_match:
                src, dst = merge_match.groups()
                # e.g., MONGODB.MERGE(POSTGRESQL) means src= MONGODB, dst= POSTGRESQL
                perform_merge(src, dst)
                continue

            # Otherwise, expect prefix_id, COMMAND
            if ',' in line:
                prefix, rest = [p.strip() for p in line.split(',', 1)]
                # SET
                set_match = re.match(r"(\w+)\.SET\(\(([^,]+),([^\)]+)\),\s*([A-Za-z])\)", rest)
                if set_match:
                    db_name, sid, course, grade = set_match.groups()
                    process_set(prefix, db_name.upper(), sid, course, grade)
                    continue
                # GET
                get_match = re.match(r"(\w+)\.GET\(\s*([^,]+),([^\)]+)\)", rest)
                if get_match:
                    db_name, sid, course = get_match.groups()
                    process_get(prefix, db_name.upper(), sid, course)
                    continue

            print(f"Unrecognized command: {line}")


if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print("Usage: python sync_databases.py <input_file>")
    else:
        main(sys.argv[1])
