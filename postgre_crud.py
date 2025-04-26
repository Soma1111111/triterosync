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
        """Insert or update grade if (student_id, course_id) conflict exists."""
        try:
            self.cursor.execute(sql.SQL("""
                INSERT INTO {table} (student_id, course_id, roll_no, email_id, grade)
                VALUES (%s, %s, NULL, NULL, %s)
                ON CONFLICT (student_id, course_id)
                DO UPDATE SET grade = EXCLUDED.grade
            """).format(table=sql.Identifier(self.table_name)),
            (studentId, courseId, grade))
            self.conn.commit()
            print(f"Inserted/Upserted ({studentId}, {courseId}) → {grade}")
        except Exception as e:
            print(f"Insert/Upsert failed: {e}")
            self.conn.rollback()

    def select_data(self, student_id, course_id):
        try:
            query = sql.SQL(
                "SELECT grade FROM {table} WHERE student_id = %s AND course_id = %s;"
            ).format(table=sql.Identifier(self.table_name))
            self.cursor.execute(query, (student_id, course_id))
            result = self.cursor.fetchone()
            if result:
                return result[0]
            else:
                return None
        except Exception as e:
            print(f"Select failed: {e}")
            return None

    def update_data(self, studentId, courseId, grade):
        try:
            self.cursor.execute(sql.SQL("""
                UPDATE {table}
                SET grade = %s
                WHERE student_id = %s AND course_id = %s;
            """).format(table=sql.Identifier(self.table_name)),
            (grade, studentId, courseId))
            self.conn.commit()
            print(f"Updated ({studentId}, {courseId}) to grade {grade}")
        except Exception as e:
            print(f"Update failed: {e}")
            self.conn.rollback()

    def delete_data(self, studentId, courseId):
        try:
            self.cursor.execute(sql.SQL("""
                DELETE FROM {table}
                WHERE student_id = %s AND course_id = %s;
            """).format(table=sql.Identifier(self.table_name)),
            (studentId, courseId))
            self.conn.commit()
            print(f"Deleted ({studentId}, {courseId})")
        except Exception as e:
            print(f"Delete failed: {e}")
            self.conn.rollback()

    def destroy(self):
        print("Cleaning up PostgreSQL connection...")
        try:
            self.cursor.close()
            self.conn.close()
            print("Connection closed.")
        except:
            pass

    def __del__(self):
        self.destroy()
