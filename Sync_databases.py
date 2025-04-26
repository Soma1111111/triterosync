import mongo_crud as mc
import postgre_crud as pc
import hive_crud as hc
import re

# Initialize database connections
mongo = mc.MongoDB("mydatabase", "mycollection")
hive = hc.HiveCRUD("student_course")
# postgre = pc.PostgreSQL(
#     dbname="mydb",
#     user="ketan1",
#     password="Arti@1982",
#     host="localhost",
#     port=5432,
#     table_name="grades"
# )

postgre = pc.PostgreSQL(
    dbname="trifectanosql",
    user="ketan1",
    password="1234567890",
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
    Ensures consistent formatting for parsing later.
    """
    global global_counter
    _, log_file = DB_MAP[db_name]
    # Remove any extra whitespace in the operation text
    clean_op = re.sub(r'\s+', '', op_text.replace(' ', ''))
    entry = f"{prefix_id}, {clean_op}, {global_counter}"
    with open(log_file, 'a') as f:
        f.write(entry + "\n")
    global_counter += 1


def process_set(prefix_id, db_name, sid, course, grade):
    """
    Handle a SET operation: update the database and write to log.
    """
    db_obj, _ = DB_MAP[db_name]
    db_obj.update_data(sid, course, grade)
    write_log(db_name, prefix_id, f"{db_name}.SET(({sid},{course}),{grade})")


def process_get(prefix_id, db_name, sid, course):
    """
    Handle a GET operation: select from the database and print the result.
    """
    db_obj, _ = DB_MAP[db_name]
    grade = db_obj.select_data(sid, course)
    print(f"{prefix_id}, {db_name}.GET({sid},{course}) -> {grade}")

def perform_merge(target_name, source_name):
    """
    Merge source_name's operations into target_name,
    by combining and sorting logs before applying.
    """
    global global_counter
    target_obj, target_log = DB_MAP[target_name]
    _, source_log = DB_MAP[source_name]

    combined_entries = []

    # Function to parse log entries with more flexible splitting
    def parse_log_file(log_file):
        entries = []
        with open(log_file, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                # Split into components, handling variable whitespace
                parts = [part.strip() for part in line.split(',')]
                if len(parts) >= 3:
                    prefix_id = parts[0]
                    # Reconstruct operation text (might contain commas)
                    op_text = ','.join(parts[1:-1]).strip()
                    counter = parts[-1]
                    try:
                        entries.append((int(counter), prefix_id, op_text))
                    except ValueError:
                        print(f"Warning: Could not parse counter '{counter}' from line: {line}")
        return entries

    # Parse both log files
    combined_entries.extend(parse_log_file(target_log))
    combined_entries.extend(parse_log_file(source_log))

    # Sort by counter
    combined_entries.sort()

    print(f"Entries to merge: {combined_entries}")  # Debug output

    # Clear target's log (because we'll rewrite it)
    open(target_log, 'w').close()

    # Apply operations in sorted order
    for counter, prefix_id, op_text in combined_entries:
        m = re.match(
            r'^\w+\.SET\(\(\s*([^,]+)\s*,\s*([^)]+)\s*\)\s*,\s*([A-Za-z])\)$',
            op_text
        )
        if not m:
            print(f"Skipping non-SET operation: {op_text}")
            continue

        sid = m.group(1).strip()
        course = m.group(2).strip()
        grade = m.group(3).strip()

        print(f"Merging operation: {op_text}")  # Debug output

        # Update in target DB
        target_obj.update_data(sid, course, grade)

        # Write new log entry with new global_counter
        write_log(
            target_name,
            prefix_id,
            f"{target_name}.SET(({sid},{course}),{grade})"
        )

def main(input_file):
    with open(input_file, 'r') as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue

            # Debug: Print the line being processed
            print(f"Processing line: {line}")

            # Check for MERGE operations (format: DB.MERGE(DB_NAME))
            merge_match = re.match(r"^(\w+)\.MERGE\((\w+)\)$", line)
            if merge_match:
                target_db, source_db = merge_match.groups()
                print(f"Performing merge: {target_db} <- {source_db}")
                perform_merge(target_db.upper(), source_db.upper())
                continue

            # Otherwise, prefix_id, COMMAND
            if ',' in line:
                # Split into prefix and command parts
                parts = re.split(r',\s+(?=\w+\.)', line, maxsplit=1)
                if len(parts) != 2:
                    print(f"Invalid command format: {line}")
                    continue
                
                prefix_id = parts[0].strip()
                command = parts[1].strip()

                # Check for SET operation (format: DB.SET((sid,course), grade))
                set_match = re.match(r"^(\w+)\.SET\(\(\s*([^,]+)\s*,\s*([^)]+)\s*\)\s*,\s*([A-Za-z])\)$", command)
                if set_match:
                    db_name, sid, course, grade = set_match.groups()
                    process_set(prefix_id, db_name.upper(), sid, course, grade)
                    continue

                # Check for GET operation (format: DB.GET(sid, course))
                get_match = re.match(r"^(\w+)\.GET\(\s*([^,]+)\s*,\s*([^)]+)\s*\)$", command)
                if get_match:
                    db_name, sid, course = get_match.groups()
                    process_get(prefix_id, db_name.upper(), sid, course)
                    continue

            print(f"Unrecognized command: {line}")


if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print("Usage: python sync_databases.py <input_file>")
    else:
        main(sys.argv[1])