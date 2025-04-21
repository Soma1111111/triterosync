#this is used to do crud operations in postgresql

import psycopg2
from psycopg2 import sql

class PostgreSQL:
    def __init__(self, dbname, user, password, host="localhost", port=5432, table_name="grades"):
        self.table_name = table_name
        try:
            self.conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )
            self.cursor = self.conn.cursor()
            print("Connected to PostgreSQL.")
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            exit(1)
    def insert_data(self, studentId, courseId, grade):
        try:
            self.cursor.execute(sql.SQL("""
                INSERT INTO {table} (student_id, course_id, roll_no, email_id, grade)
                VALUES (%s, %s, NULL, NULL, %s)
            """).format(table=sql.Identifier(self.table_name)), 
            (studentId, courseId, grade))
            self.conn.commit()
            print(f"Inserted/Updated ({studentId}, {courseId}) → {grade}")
        except Exception as e:
            print(f"Insert failed: {e}")
            self.conn.rollback()

    def select_data(self):
        try:
            self.cursor.execute(sql.SQL("SELECT * FROM {table};")
                                .format(table=sql.Identifier(self.table_name)))
            rows = self.cursor.fetchall()
            print(f"\n Data from table '{self.table_name}':")
            for row in rows:
                print(f"| {row[0]:<10} | {row[1]:<8} | {row[2] or 'NULL':<8} | {row[3] or 'NULL':<20} | {row[4]:<5} |")
            print()
        except Exception as e:
            print(f" Select failed: {e}")

    def update_data(self, studentId, courseId, grade):
        try:
            self.cursor.execute(sql.SQL("""
                UPDATE {table}
                SET grade = %s
                WHERE student_id = %s AND course_id = %s;
            """).format(table=sql.Identifier(self.table_name)), 
            (grade, studentId, courseId))
            self.conn.commit()
            print(f" Updated ({studentId}, {courseId}) to grade {grade}")
        except Exception as e:
            print(f" Update failed: {e}")
            self.conn.rollback()

    def delete_data(self, studentId, courseId):
        try:
            self.cursor.execute(sql.SQL("""
                DELETE FROM {table}
                WHERE student_id = %s AND course_id = %s;
            """).format(table=sql.Identifier(self.table_name)), 
            (studentId, courseId))
            self.conn.commit()
            print(f" Deleted ({studentId}, {courseId})")
        except Exception as e:
            print(f" Delete failed: {e}")
            self.conn.rollback()

    def destroy(self):
        print(" Cleaning up PostgreSQL connection...")
        try:
            self.cursor.close()
            self.conn.close()
            print(" Connection closed.")
        except:
            pass

    def __del__(self):
        self.destroy()

if __name__ == "__main__":
    pg = PostgreSQL(
        dbname="nosql",
        user="soma",
        password="soma1",
        host="localhost",
        port=5432,
        table_name="grades"
    )
    # pg.create_table()  # Now uncommented to ensure fresh table creation
    pg.insert_data("IMT2023001", "CSC101", "A")
    # pg.insert_data("IMT2023001", "CSC102", "B")
    # pg.insert_data("IMT2023002", "CSC101", "C")
    # pg.select_data()
    
    # Example updates and deletions
    # pg.update_data("SID1033", "CSE016", "A+")
    # pg.delete_data("SID1033", "CSE016")
    # pg.select_data()