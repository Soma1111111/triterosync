import os
import subprocess
import time
import uuid
import csv
from pyhive import hive
import uuid, csv, os

class HiveCRUD:
    def __init__(self, table_name="student_course"):
        self.table_name = table_name
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
        # Connect to HiveServer2 (default port 10000)
        self.conn = hive.Connection(host="localhost", port=10000,
                                    username="ketan")
        self.cursor = self.conn.cursor()

    def create_table(self):
        # Drop any existing table
        self.cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
        # Create a flat table (no partitions)
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

    def bulk_insert_from_csv(self, csv_file_path):
        # Load entire CSV into Hive table (overwriting any existing data)
        self.cursor.execute(f"""
            LOAD DATA LOCAL INPATH '{csv_file_path}'
            OVERWRITE INTO TABLE {self.table_name}
        """)
        print(f"Bulk data loaded from '{csv_file_path}' into '{self.table_name}'.")

    def insert_data(self, studentId, courseId, rollNo, emailId, grade):
        # Use INSERT INTO VALUES to add a single row directly
        insert_query = f"""
            INSERT INTO TABLE {self.table_name}
            (student_id, course_id, roll_no, email_id, grade)
            VALUES (
              '{studentId}',
              '{courseId}',
              '{rollNo}',
              '{emailId}',
              '{grade}'
            )
        """
        self.cursor.execute(insert_query)
        print(f"Inserted row for ({studentId}, {courseId}) into '{self.table_name}'.")

    def select_data(self, studentId, courseId):
        qry = f"""
            SELECT grade
            FROM {self.table_name}
            WHERE student_id = '{studentId}'
            AND course_id  = '{courseId}'
        """
        self.cursor.execute(qry)
        result = self.cursor.fetchone()

        if result:
            return result[0]  # grade is the first (and only) column selected
        else:
            return None


    def update_data(self, studentId, courseId, new_grade):
        # 1) Pull every row out of Hive
        self.cursor.execute(f"""
            SELECT student_id, course_id, roll_no, email_id, grade
            FROM {self.table_name}
        """)
        all_rows = self.cursor.fetchall()
        if not all_rows:
            print("Table is empty—nothing to update.")
            return

        # 2) Write them (with the one grade changed) to a temp CSV
        tmp_csv = f"/tmp/hive_update_{uuid.uuid4().hex}.csv"
        with open(tmp_csv, 'w', newline='') as f:
            writer = csv.writer(f)
            # If you’ve set TBLPROPERTIES("skip.header.line.count"="1"),
            # you can write a header row so Hive will skip it:
            writer.writerow(['student_id','course_id','roll_no','email_id','grade'])
            for sid, cid, roll, email, grade in all_rows:
                if sid == studentId and cid == courseId:
                    grade = new_grade
                writer.writerow([sid, cid, roll, email, grade])

        # 3) OVERWRITE the Hive table with the new CSV (fast metadata op)
        self.cursor.execute(f"""
            LOAD DATA LOCAL INPATH '{tmp_csv}'
            OVERWRITE INTO TABLE {self.table_name}
        """)
        os.remove(tmp_csv)

        print(f"Updated grade for ({studentId}, {courseId}) → '{new_grade}'.")

    def destroy(self):
        print("Closing Hive connection and stopping server…")
        try:
            self.cursor.close()
            self.conn.close()
        except:
            pass
        self.hive_server.terminate()


if __name__ == "__main__":
    h = HiveCRUD("student_course")
    h.create_table()
    # # Bulk load CSV
    h.bulk_insert_from_csv(
        # "/home/ketan/Desktop/NoSQL_Project1/triterosync/student_course_grades.csv"
        "/mnt/c/Users/DELL/Documents/KrishWork/NOSQL/Project/triterosync/student_course_grades.csv"
    )
    # # Select
    # h.select_data("SID1033", "CSE016")
    # # Update
    # h.update_data("SID1033", "CSE016", "A-")
    # # Read back
    # h.select_data("SID1033", "CSE016")