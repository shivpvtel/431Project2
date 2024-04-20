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
CREATE TABLE if not exists Contracts (
    ID INTEGER,
    ValueEUR INTEGER,
    WageEUR INTEGER,
    ContractUntil INTEGER,
    ReleaseClause INTEGER
);
"""
cur.execute(create_table_query)
conn.commit()

# Assuming correct CSV file path now
csv_file_path = 'Contracts.csv'  # Adjusted to the correct CSV

# Open the CSV file and read data
with open(csv_file_path, newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # Skip the header row

    query = """
    INSERT INTO Contracts (ID, ValueEUR, WageEUR, ContractUntil, ReleaseClause) 
    VALUES (%s, %s, %s, %s, %s)
    """

    # Insert each row
    for row in reader:
        # Preprocess integer fields to handle potential input errors
        row[1] = safe_int(row[1], default=None)  # Assuming ValueEUR is at index 1
        row[2] = safe_int(row[2], default=None)  # Assuming WageEUR is at index 2
        row[3] = safe_int(row[3], default=None)  # Assuming ContractUntil is at index 3
        row[4] = safe_int(row[4], default=None)  # Assuming ReleaseClause is at index 4

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
