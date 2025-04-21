#this is used to create and bulk upload the data into postgresql

import psycopg2
import os

def create_grades_table():
    """Connects to the nosql database and creates the 'grades' table if it does not exist."""
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="nosql",
            user="soma",
            password="soma1",
            port="5432"
        )
        cursor = conn.cursor()
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS grades (
                student_id VARCHAR(20),
                course_id VARCHAR(20),
                roll_no VARCHAR(20),
                email_id VARCHAR(100),
                grade VARCHAR(2)
            );
        """
        cursor.execute(create_table_sql)
        conn.commit()
        print("Table 'grades' created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def fast_bulk_upload_csv(csv_path, table_name):
    """Performs a fast bulk upload of a CSV file to the specified table using PostgreSQL's COPY command."""
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="nosql",
            user="soma",
            password="soma1",
            port="5432"
        )
        cursor = conn.cursor()
        
        # Open the CSV and skip the header row
        with open(csv_path, 'r') as f:
            next(f)  # Skip header
            cursor.copy_from(
                file=f,
                table=table_name,
                sep=',',
                columns=('student_id', 'course_id', 'roll_no', 'email_id', 'grade'),
                null=''
            )

        conn.commit()
        print(f"COPY command completed for {csv_path} and data inserted into table '{table_name}'.")
    except Exception as e:
        print(f"Error uploading CSV: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def list_tables():
    """Lists all tables in the 'public' schema of the nosql database."""
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="nosql",
            user="soma",
            password="soma1",
            port="5432"
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public';
        """)
        tables = cursor.fetchall()
        print("Tables in the 'public' schema:")
        for table in tables:
            print(table[0])
    except Exception as e:
        print(f"Error listing tables: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    # Step 1: Create the 'grades' table
    create_grades_table()

    # Step 2: Bulk upload CSV data into the 'grades' table.
    csv_file = "student_course_grades.csv"  # Change this path if your CSV is located elsewhere.
    if os.path.exists(csv_file):
        fast_bulk_upload_csv(csv_file, "grades")
    else:
        print(f"CSV file '{csv_file}' not found.")

    # Step 3: List tables in the 'nosql' database to confirm the table exists.
    list_tables()