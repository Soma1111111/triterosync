# will change into main file

import re
import json

def parse_testcase_file(file_path):
    instructions = []

    with open(file_path, 'r') as file:
        for idx, line in enumerate(file):
            line = line.strip()
            if not line:
                continue

            timestamp = idx + 1
            db_timestamp = None
            operation = None
            db1 = db2 = student_id = course_id = grade = None

            # Check for leading timestamp (db_timestamp)
            if ',' in line:
                parts = line.split(',', 1)
                if parts[0].strip().isdigit():
                    db_timestamp = int(parts[0].strip())
                    line = parts[1].strip()

            # Remove extra spaces to simplify parsing
            line = re.sub(r'\s+', '', line)

            # MERGE operation
            if 'MERGE' in line:
                match = re.match(r'(\w+)\.MERGE\((\w+)\)', line)
                if match:
                    db1, db2 = match.groups()
                    operation = 'MERGE'

            # SET operation
            elif 'SET' in line:
                match = re.match(r'(\w+)\.SET\(\(\s*(\w+)\s*,\s*(\w+)\s*\),\s*(\w+)\)', line)
                if match:
                    db1, student_id, course_id, grade = match.groups()
                    operation = 'SET'

            # GET operation
            elif 'GET' in line:
                match = re.match(r'(\w+)\.GET\(\s*(\w+)\s*,\s*(\w+)\s*\)', line)
                if match:
                    db1, student_id, course_id = match.groups()
                    operation = 'GET'

            instructions.append({
                'timestamp': timestamp,
                'db_timestamp': db_timestamp,
                'operation': operation,
                'db1': db1,
                'db2': db2,
                'student_id': student_id,
                'course_id': course_id,
                'grade': grade
            })

    return instructions

# Parse instructions
parsed_instructions = parse_testcase_file("example_testcase.in")

# Save to JSON
with open("parsed_instructions.json", "w") as json_file:
    json.dump(parsed_instructions, json_file, indent=4)

print("Instructions saved to parsed_instructions.json")
