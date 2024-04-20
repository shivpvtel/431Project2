import csv
import psycopg2

# Establish database connection
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="1234",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Create the table if it doesn't exist
create_table_query = """
CREATE TABLE if not exists Player_body_component (
    ID INTEGER,
    PreferredFoot VARCHAR(255),
    Height INTEGER,
    Weight INTEGER
);
"""
cur.execute(create_table_query)
conn.commit()

# Assuming correct CSV file path now
csv_file_path = 'Player_body_component.csv'

# Open the CSV file and read data
with open(csv_file_path, newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # Skip the header row

    # Prepare your SQL query for inserting data
    query = """
    INSERT INTO Player_body_component (ID, PreferredFoot, Height, Weight) 
    VALUES (%s, %s, %s, %s)
    """

    # Insert each row
    for row in reader:
        if len(row) == 4:  # Ensure each row has exactly 4 elements
            try:
                # Execute SQL query for each row
                cur.execute(query, row)
                conn.commit()  # Commit each successful insert
            except psycopg2.Error as e:
                print(f"Error inserting data: {e}")
                conn.rollback()  # Rollback transaction in case of error
        else:
            print(f"Skipped row due to incorrect number of columns: {row}")

# Close the cursor and connection
cur.close()
conn.close()
