#this does bulk upload as well as crud operations for hive

import os
import subprocess
import time
from pyhive import hive
import csv
import uuid

class Hive:
    table_name = ""
    temp_table = ""
    def __init__(self, table_name):
        self.table_name = table_name
        self.temp_table = table_name + "_temp"
        hive_home = os.environ.get('HIVE_HOME')
        if not hive_home:
            print("HIVE_HOME is not set.")
            exit(1)
        print(f"HIVE_HOME is set to: {hive_home}")
        hive_server_cmd = os.path.join(hive_home, "bin", "hiveserver2")
        print("Starting HiveServer2...")
        self.hive_server_process = subprocess.Popen(
            hive_server_cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        time.sleep(10)

        try:
            self.conn = hive.Connection(host="localhost", port=10000, username="ketan")
            self.cursor = self.conn.cursor()
            self.cursor.execute("SET hive.exec.dynamic.partition=true")
            self.cursor.execute("SET hive.exec.dynamic.partition.mode=nonstrict")
        except Exception as e:
            print(f"Error connecting to Hive: {e}")
            self.destroy()

    def create_table(self):
        self.cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
        self.cursor.execute(f"""
            CREATE TABLE {self.table_name} (
                grade STRING
            )
            PARTITIONED BY (student_id STRING, course_id STRING)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """)
        print(f"Partitioned table '{self.table_name}' created.")

    def load_data(self, studentId, courseId, grade):
        temp_filename = f"temp_{uuid.uuid4().hex}.csv"
        try:
            with open(temp_filename, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow([grade])  
            load_query = f"""
                LOAD DATA LOCAL INPATH '{os.path.abspath(temp_filename)}' 
                OVERWRITE INTO TABLE {self.table_name}
                PARTITION (student_id='{studentId}', course_id='{courseId}')
            """
            self.cursor.execute(load_query)
        finally:
            if os.path.exists(temp_filename):
                os.remove(temp_filename)

        print(f"Data loaded into table '{self.table_name}'.")


    def insert_data(self, studentId, courseId, grade):
        self.load_data(studentId, courseId, grade)

    def delete_data(self, studentId, courseId):
        temp_filename = f"temp_{uuid.uuid4().hex}.csv"

        try:
            with open(temp_filename, mode='w', newline='') as file:
                pass
            
            load_query = f"""
                LOAD DATA LOCAL INPATH '{os.path.abspath(temp_filename)}' 
                OVERWRITE INTO TABLE {self.table_name}
                PARTITION (student_id='{studentId}', course_id='{courseId}')
            """
            self.cursor.execute(load_query)
        
        finally:
            if os.path.exists(temp_filename):
                os.remove(temp_filename)

        print(f"Data loaded into table '{self.table_name}'.")

    def select_data(self, student_id, course_id, table_name='student_course'):
        query = f"SELECT student_id, course_id, grade FROM {table_name} WHERE student_id = %s AND course_id = %s"
        self.cursor.execute(query, (student_id, course_id))
        # rows = self.cursor.fetchall()
        print(f"Data from table '{table_name}' for student_id='{student_id}' and course_id='{course_id}':")
        # for row in rows:
        #     print(row)

    def update_data(self, studentId, courseId, grade):
        self.load_data(studentId, courseId, grade)

    def destroy(self):
        print("Cleaning up Hive connection and process...")
        try:
            if hasattr(self, 'cursor'):
                self.cursor.close()
            if hasattr(self, 'conn'):
                self.conn.close()
        except:
            pass
        if hasattr(self, 'hive_server_process') and self.hive_server_process:
            self.hive_server_process.terminate()
            print("HiveServer2 stopped.")
    def __del__(self):
        self.destroy()

if __name__ == "__main__":
    hive_instance = Hive("student_course")
    hive_instance.create_table()
    hive_instance.insert_data("IMT2023001", "CSC101", "A")
    hive_instance.select_data()
    hive_instance.insert_data("IMT2023001", "CSC102", "B")
    hive_instance.select_data()
    hive_instance.insert_data("IMT2023002", "CSC101", "C")
    hive_instance.select_data()
    hive_instance.update_data("IMT2023001", "CSC101", "B")
    hive_instance.select_data()
    hive_instance.delete_data("IMT2023001", "CSC101")
    hive_instance.select_data()
    hive_instance.__del__()