

import csv
import psycopg2

# Database connection parameters
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="1234",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# SQL to create a table
create_table_query = """
CREATE TABLE if not exists Work_rates (
    ID INTEGER,
    AttackingWorkRate text,
    DefensiveWorkRate text
);
"""
cur.execute(create_table_query)  # Create the table if it doesn't exist
conn.commit()  # Commit the creation of the table

# Path to your CSV file
csv_file_path = 'Work_rates.csv'

# Open the CSV file and read data
with open(csv_file_path, newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # Skip the header row

    # Prepare your SQL query for inserting data
    query = """
    INSERT INTO Work_rates (ID, AttackingWorkRate, DefensiveWorkRate) 
    VALUES (%s, %s, %s)
    """

    # Insert each row
    for row in reader:
        try:
            # Execute SQL query for each row
            cur.execute(query, row)
            conn.commit()  # Commit each successful insert
        except psycopg2.Error as e:
            print(f"Error inserting data: {e}")
            conn.rollback()  # Rollback transaction in case of error

# Close the cursor and connection
cur.close()
conn.close()
