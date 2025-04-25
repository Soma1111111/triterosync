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
def distribute_lines_correctly():
    # Step 1: Create/clear output files
    open("oplog.hiveql", "w").close()
    open("oplog.mongo", "w").close()
    open("oplog.sql", "w").close()

    # Step 2: Read input and open output files for appending
    with open("example_testcase2.in", "r") as infile, \
         open("oplog.hiveql", "a") as hive_out, \
         open("oplog.mongo", "a") as mongo_out, \
         open("oplog.sql", "a") as sql_out:
        # Step 3: Process each line
        cnt=0
        for line in infile:
            if "HIVE" in line:
                if("SET" in line):
                    hive_out.write(line)
                    match = re.search(r"SET\(\(([^,]+),([^)]+)\),\s*([A-F][+-]?)\)", line)
                    if match:
                        sid,cid,grade = match.groups()
                        hive.update_data(sid,cid,grade)
                        print(f"HIVE SET: studentID={sid}, Course-ID={cid}, grades={grade}")
                elif("GET" in line):
                    hive_out.write(line)
                    match = re.search(r"GET\(\(([^,]+),([^)]+)\),\s*([A-F][+-]?)\)", line)
                    if match:
                        sid,cid = match.groups()
                        hive.select_data("student_course",sid,cid)
                else:
                    pass
            elif "MONGODB" in line:
                if("SET" in line):
                    mongo_out.write(line)
                    match = re.search(r"SET\(\(([^,]+),([^)]+)\),\s*([A-F][+-]?)\)", line)
                    if match:
                        sid,cid,grade = match.groups()
                        mongo.update_data(sid,cid,grade)
                elif("GET" in line):
                    mongo_out.write(line)
                    match = re.search(r"GET\(\(([^,]+),([^)]+)\),\s*([A-F][+-]?)\)", line)
                    if match:
                        sid,cid = match.groups()
                        mongo.select_data(sid,cid)
                else:
                    pass
        
            elif "POSTGRESQL" in line:
                if("SET" in line):
                    sql_out.write(line)
                    match = re.search(r"SET\(\(([^,]+),([^)]+)\),\s*([A-F][+-]?)\)", line)
                    if match:
                        sid,cid,grade = match.groups()
                        postgre.update_data(sid,cid,grade)
                elif("GET" in line):
                    sql_out.write(line)
                    match = re.search(r"GET\(\(([^,]+),([^)]+)\),\s*([A-F][+-]?)\)", line)
                    if match:
                        sid,cid = match.groups()
                        mongo.select_data(sid,cid)
                else:
                    pass

# Run the script
if __name__ == "__main__":
    distribute_lines_correctly()
    print("Lines distributed to oplog.hiveql, oplog.mongo, and oplog.sql as required (excluding MERGE).")