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
# Adjust column definitions according to the content of your CSV file
create_table_query = """
CREATE TABLE if not exists Player_info (
    ID INTEGER,
    Name VARCHAR(255),
    FullName VARCHAR(255),
    Age INTEGER,
    PhotoUrl TEXT,
    Nationality VARCHAR(100),
    Primary Key (ID)
);

CREATE TABLE if not exists Player_body_component (
    ID INTEGER,
    PreferredFoot VARCHAR(255),
    Height INTEGER,
    Weight INTEGER,
    FOREIGN KEY (ID) REFERENCES Player_info(ID)
);

CREATE TABLE if not exists Positions (
    ID INTEGER,
    Positions Varchar(100),
    BestPosition Varchar(20),
    ClubPosition Varchar(20),
    NationalPosition Varchar(20),
    FOREIGN KEY (ID) REFERENCES Player_info(ID)
);

CREATE TABLE if not exists Player_stats (
    ID INTEGER,
    TotalStats INTEGER,
    BaseStats INTEGER,
    Overall INTEGER,
    Potential INTEGER,
    Growth INTEGER,
    FOREIGN KEY (ID) REFERENCES Player_info(ID)
);

CREATE TABLE if not exists Club (
    ID INTEGER,
    Club VARCHAR(255),
    ClubNumber INTEGER,
    ClubJoined INTEGER,
    FOREIGN KEY (ID) REFERENCES Player_info(ID)
);

CREATE TABLE if not exists National (
    ID INTEGER,
    NationalTeam VARCHAR(255),
    IntReputation INTEGER,
    NationalNumber INTEGER,
    FOREIGN KEY (ID) REFERENCES Player_info(ID)
);

CREATE TABLE if not exists Contracts (
    ID INTEGER,
    ValueEUR INTEGER,
    WageEUR INTEGER,
    ContractUntil INTEGER,
    ReleaseClause INTEGER,
    FOREIGN KEY (ID) REFERENCES Player_info(ID)
);

CREATE TABLE if not exists Work_rates (
    ID INTEGER,
    AttackingWorkRate INTEGER,
    DefensiveWorkRate INTEGER,
    FOREIGN KEY (ID) REFERENCES Player_info(ID)
);

CREATE TABLE if not exists Totals (	
    ID INTEGER,			
    PaceTotal INTEGER,	
    ShootingTotal INTEGER,	
    PassingTotal INTEGER,	
    DribblingTotal INTEGER, 
    DefendingTotal INTEGER, 
    PhysicalityTotal INTEGER,
    FOREIGN KEY (ID) REFERENCES Player_info(ID)
);

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
    Composure INTEGER,
    FOREIGN KEY (ID) REFERENCES Player_info(ID)
);

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
    GKReflexes INTEGER,
    FOREIGN KEY (ID) REFERENCES Player_info(ID)
);

CREATE TABLE if not exists Position_ratings(	
    ID INTEGER,													
    STRating INTEGER,
    LWRating INTEGER,
    LFRating INTEGER,
    CFRating INTEGER,
    RFRating INTEGER,
    RWRating INTEGER,
    CAMRating INTEGER,
    LMRating INTEGER,
    CMRating INTEGER,
    RMRating INTEGER,
    LWBRating INTEGER,
    CDMRating INTEGER,
    RWBRating INTEGER,
    LBRating INTEGER,
    CBRating INTEGER,
    RBRating INTEGER,
    GKRating INTEGER,
    FOREIGN KEY (ID) REFERENCES Player_info(ID)
);

"""
cur.execute(create_table_query)
conn.commit()  # Commit the creation of the table

# Path to your CSV file
csv_file_path = 'Player_info.csv'
csv_file_path2 = 'Player_body_component.csv'
csv_file_path3 = 'Positions.csv'
csv_file_path4 = 'Player_stats.csv'
csv_file_path5 = 'Club.csv'
csv_file_path6 = 'National.csv'
csv_file_path7 = 'Contracts.csv'
csv_file_path8 = 'Work_rates.csv'
csv_file_path9 = 'Totals.csv'
csv_file_path10 = 'Physical_attributes.csv'
csv_file_path11 = 'In_game_attributes.csv'
csv_file_path12 = 'Position_rating.csv'



# Player-info
with open(csv_file_path, newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)
    query = """
    INSERT INTO player_info (ID, Name, FullName, Age, PhotoUrl, Nationality) 
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    cur.execute(query)

# Player_body_component
with open(csv_file_path2, newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)
    query2 = """
    IINSERT INTO Player_body_component (ID, PreferredFoot, Height, Weight) VALUES (%s, %s, %s, %s)
    """
    cur.execute(query2)
    

# Position
with open(csv_file_path3, newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)
    query3 = """
    INSERT INTO Positions (ID, Positions, BestPosition, ClubPosition, NationalPosition) 
    VALUES (%s, %s, %s, %s, %s)
    """
    cur.execute(query3)

# Player_stats
with open(csv_file_path4, newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader) 
    query4 = """
    INSERT INTO Player_stats (ID, TotalStats, BaseStats, Overall, Potential, Growth) 
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    cur.execute(query4)

# Club
with open(csv_file_path5, newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)
    query5 = """
    INSERT INTO Club (ID, Club, ClubNumber, ClubJoined) 
    VALUES (%s, %s, %s, %s)
    """
    cur.execute(query5)
# National
with open(csv_file_path6, newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)
    query6 = """
    INSERT INTO National (ID, NationalTeam, IntReputation, NationalNumber) 
    VALUES (%s, %s, %s, %s)
    """
    cur.execute(query6)

# Contracts
with open(csv_file_path7, newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader) 
    query7 = """
    INSERT INTO Contracts (ID, ValueEUR, WageEUR, ContractUntil, ReleaseClause) 
    VALUES (%s, %s, %s, %s, %s)
    """
    cur.execute(query7)

# Work_rates
with open(csv_file_path8, newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader) 
    query8 = """
    INSERT INTO Work_rates (ID, AttackingWorkRate, DefensiveWorkRate) 
    VALUES (%s, %s, %s)
    """
    cur.execute(query8)
# Total
with open(csv_file_path9, newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader) 
    query9 = """
    INSERT INTO Totals (ID, PaceTotal, ShootingTotal, PassingTotal, DribblingTotal, DefendingTotal, PhysicalityTotal) 
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    cur.execute(query9)

# Physical_Attributes
with open(csv_file_path10, newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)
    query10 = """
    INSERT INTO Physical_Attributes (ID, Acceleration, SprintSpeed, Agility, Reactions, Balance, ShotPower, Jumping, Stamina, Strength, Aggression, Vision, Composure) 
    VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s, %s)
    """
    cur.execute(query10)
# InGame_Attributes
with open(csv_file_path11, newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader) 
    query11 = """
    INSERT INTO InGame_Attributes (ID, WeakFoot, SkillMoves, Crossing, Finishing, HeadingAccuracy, ShortPassing, Volleys, Dribbling, Curve, FKAccuracy,
    LongPassing, BallControl, LongShots, Interceptions, Positioning, Penalties, Marking, StandingTackle, SlidingTackle,
    GKDiving, GKHandling, GKKicking, GKPositioning, GKReflexes) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.execute(query9)
# Position_ratings
with open(csv_file_path12, newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader) 
    query12 = """
    INSERT INTO Position_ratings (ID, STRating, LWRating, LFRating, CFRating, RFRating, RWRating, CAMRating, LMRating, CMRating, RMRating, LWBRating, CDMRating, RWBRating, LBRating, CBRating, RBRating, GKRating) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.execute(query12)

conn.commit()
cur.close()
conn.close()
