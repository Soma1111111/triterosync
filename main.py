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
        for line in infile:
            # Skip lines containing 'MERGE'
            if "MERGE" in line:
                continue

            # Write to appropriate oplog file based on DB type
            if "HIVE" in line:
                hive_out.write(line)
            elif "MONGODB" in line:
                mongo_out.write(line)
            elif "POSTGRESQL" in line:
                sql_out.write(line)

# Run the script
if __name__ == "__main__":
    distribute_lines_correctly()
    print("Lines distributed to oplog.hiveql, oplog.mongo, and oplog.sql as required (excluding MERGE).")
