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
CREATE TABLE if not exists Physical_Attributes(	
    ID INTEGER,								
    Acceleration INTEGER, 
    SprintSpeed INTEGER, 
    Agility INTEGER, 
    Reactions INTEGER, 
    Balance INTEGER, 
    ShotPower INTEGER,
    Jumping INTEGER,
    Stamina INTEGER, 
    Strength INTEGER, 
    Aggression INTEGER, 
    Vision INTEGER, 
    Composure INTEGER
);
"""
cur.execute(create_table_query)  # Create the table if it doesn't exist
conn.commit()  # Commit the creation of the table

# Path to your CSV file
csv_file_path = 'Physical_attributes.csv'

# Open the CSV file and read data
with open(csv_file_path, newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # Skip the header row

    # Prepare your SQL query for inserting data
    query = """
    INSERT INTO Physical_Attributes (ID, Acceleration, SprintSpeed, Agility, Reactions, Balance, ShotPower, Jumping, Stamina, Strength, Aggression, Vision, Composure) 
    VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s, %s)
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
