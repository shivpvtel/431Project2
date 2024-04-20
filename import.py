import csv
import psycopg2

def insert_data_from_csv(csv_file_path, query, db_connection, db_cursor):
    with open(csv_file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip the header row
        for row in reader:
            try:
                db_cursor.execute(query, row)
            except psycopg2.Error as e:
                print(f"Error inserting data: {e}")
                db_connection.rollback()  # Rollback in case of any error
            else:
                db_connection.commit()  # Commit each successful insert

def main():
    # Database connection parameters
    conn = psycopg2.connect(
        dbname="Fifa",
        user="postgres",
        password="1234",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    # Dictionary of CSV paths and corresponding SQL insert queries
    operations = {
        'Player_info.csv': "INSERT INTO player_info (ID, Name, FullName, Age, PhotoUrl, Nationality) VALUES (%s, %s, %s, %s, %s, %s)",
        'Player_body_component.csv': "INSERT INTO Player_body_component (ID, PreferredFoot, Height, Weight) VALUES (%s, %s, %s, %s)",
        'Positions.csv': "INSERT INTO Positions (ID, Positions, BestPosition, ClubPosition, NationalPosition) VALUES (%s, %s, %s, %s, %s)",
        'Player_stats.csv': "INSERT INTO Player_stats (ID, TotalStats, BaseStats, Overall, Potential, Growth) VALUES (%s, %s, %s, %s, %s, %s)",
        'Club.csv': "INSERT INTO Club (ID, Club, ClubNumber, ClubJoined)  VALUES (%s, %s, %s, %s)",
        'National.csv': "INSERT INTO National (ID, NationalTeam, IntReputation, NationalNumber) VALUES (%s, %s, %s, %s)",
        'Contracts.csv':"INSERT INTO Contracts (ID, ValueEUR, WageEUR, ContractUntil, ReleaseClause) VALUES (%s, %s, %s, %s, %s)",
        'Work_rates.csv': "INSERT INTO Work_rates (ID, AttackingWorkRate, DefensiveWorkRate) ",
        'Totals.csv': "INSERT INTO Totals (ID, PaceTotal, ShootingTotal, PassingTotal, DribblingTotal, DefendingTotal, PhysicalityTotal) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        'Physical_attributes.csv': "INSERT INTO Physical_Attributes (ID, Acceleration, SprintSpeed, Agility, Reactions, Balance, ShotPower, Jumping, Stamina, Strength, Aggression, Vision, Composure) VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s, %s)",
        'In_game_attributes.csv':"INSERT INTO InGame_Attributes (ID, WeakFoot, SkillMoves, Crossing, Finishing, HeadingAccuracy, ShortPassing, Volleys, Dribbling, Curve, FKAccuracy,LongPassing, BallControl, LongShots, Interceptions, Positioning, Penalties, Marking, StandingTackle, SlidingTackle, GKDiving, GKHandling, GKKicking, GKPositioning, GKReflexes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        'Position_rating.csv': "INSERT INTO Position_ratings (ID, STRating, LWRating, LFRating, CFRating, RFRating, RWRating, CAMRating, LMRating, CMRating, RMRating, LWBRating, CDMRating, RWBRating, LBRating, CBRating, RBRating, GKRating) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    }

    # Execute insert operations for each CSV file
    for csv_file, insert_query in operations.items():
        insert_data_from_csv(csv_file, insert_query, conn, cur)

    # Close database connection
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()