    
import csv
import psycopg2

def safe_int(value, default=None):
    """Convert value to integer or return default if conversion fails."""
    try:
        return int(value)
    except ValueError:
        return default
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
CREATE TABLE if not exists National (
    ID INTEGER,
    NationalTeam VARCHAR(255),
    IntReputation INTEGER,
    NationalNumber INTEGER
);
"""
cur.execute(create_table_query)
conn.commit()

# Assuming correct CSV file path now
csv_file_path = 'National.csv'

# Open the CSV file and read data
with open(csv_file_path, newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # Skip the header row

    # Prepare your SQL query for inserting data
    query = """
    INSERT INTO National (ID, NationalTeam, IntReputation, NationalNumber) 
    VALUES (%s, %s, %s, %s)
    """

     # Insert each row
    for row in reader:
        # Preprocess integer fields to handle potential input errors
        row[2] = safe_int(row[2], default=None)  # Convert ClubNumber, use None if conversion fails
        row[3] = safe_int(row[3], default=None)  # Convert ClubJoined, use None if conversion fails

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
