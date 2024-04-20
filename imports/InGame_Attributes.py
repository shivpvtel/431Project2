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
CREATE TABLE if not exists InGame_Attributes(
    ID INTEGER,																						
    WeakFoot INTEGER,
    SkillMoves INTEGER,
    Crossing INTEGER,
    Finishing INTEGER,
    HeadingAccuracy INTEGER,
    ShortPassing INTEGER,
    Volleys	 INTEGER,
    Dribbling INTEGER,
    Curve INTEGER,
    FKAccuracy INTEGER,
    LongPassing INTEGER,
    BallControl INTEGER,
    LongShots INTEGER,
    Interceptions INTEGER,
    Positioning	 INTEGER,
    Penalties INTEGER,
    Marking INTEGER,
    StandingTackle INTEGER,
    SlidingTackle INTEGER,
    GKDiving INTEGER,
    GKHandling INTEGER,
    GKKicking INTEGER,
    GKPositioning INTEGER,
    GKReflexes INTEGER
);
"""
cur.execute(create_table_query)  # Create the table if it doesn't exist
conn.commit()  # Commit the creation of the table

# Path to your CSV file
csv_file_path = 'In_game_attributes.csv'

# Open the CSV file and read data
with open(csv_file_path, newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # Skip the header row

    # Prepare your SQL query for inserting data
    query = """
    INSERT INTO InGame_Attributes (ID, WeakFoot, SkillMoves, Crossing, Finishing, HeadingAccuracy, ShortPassing, Volleys, Dribbling, Curve, FKAccuracy,
    LongPassing, BallControl, LongShots, Interceptions, Positioning, Penalties, Marking, StandingTackle, SlidingTackle,
    GKDiving, GKHandling, GKKicking, GKPositioning, GKReflexes) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
