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


    # # # Listing available tables
    # tables = {
    #     '1': 'Player_info',
    #     '2': 'Player_body_component',
    #     '3': 'Positions',
    #     '4': 'Player_stats',
    #     '5': 'Club',
    #     '6': 'National',
    #     '7': 'Contracts',
    #     '8': 'Work_rates',
    #     '9': 'Totals',
    #     '10': 'Physical_attributes',
    #     '11': 'In_game_attributes',
    #     '12': 'Position_ratings'
    # }

tables = {
        '1': 'Table Name: Player_info \n    Columns names: ID, Name, FullName, Age, PhotoUrl, Nationality',
        '2': 'Table Name: Player_body_component \n  ID, PreferredFoot, Height, Weight',
        '3': 'Table Name: Positions\n   Columns names: ID, Positions, BestPosition, ClubPosition, NationalPosition',
        '4': 'Table Name: Player_stats\n    Columns names: ID, TotalStats, BaseStats, Overall, Potential, Growth',
        '5': 'Table Name: Club\n    Columns names: ID, Club, ClubNumber, ClubJoined',
        '6': 'Table Name: National\n    Columns names: ID, National, IntReputation, NationalNumber',
        '7': 'Table Name: Contracts\n   Columns names: ID, ValueEUR, WageEUR, ContractUntil, ReleaseClause',
        '8': 'Table Name: Work_rates\n  Columns names: ID, AttackingWorkRate, DefensiveWorkRate',
        '9': 'Table Name: Totals\n  Columns names: ID, PaceTotal, ShootingTotal, PassingTotal, DribblingTotal, DefendingTotal, PhysicalityTotal',
        '10': 'Table Name: Physical_attributes\n    Columns names: ID, Acceleration, SprintSpeed, Agility, Reactions, Balance, ShotPower, Jumping, Stamina, Strength, Aggression, Vision, Composure',
        '11': 'Table Name: In_game_attributes\n Columns names: ID, WeakFoot, SkillMoves, Crossing, Finishing, HeadingAccuracy, ShortPassing, Volleys, Dribbling, Curve, FKAccuracy, LongPassing, BallControl, LongShots, Interceptions, Positioning, Penalties, Marking, StandingTackle, SlidingTackle, GKDiving, GKHandling, GKKicking, GKPositioning, GKReflexes',
        '12': 'Table Name: Position_ratings\n   Columns names:  ID, STRating, LWRating, LFRating, CFRating, RFRating, RWRating, CAMRating, LMRating, CMRating, RMRating, LWBRating, CDMRating, RWBRating, LBRating, CBRating, RBRating, GKRating'
    }

def mainMenu():
    print('\nWelcome to the Database CLI Interface!\n')
    print('Please select an option:')
    print('1. Insert Data')
    print('2. Delete Data')
    print('3. Update Data')
    print('4. Search Data')
    print('5. Aggregate Functions')
    print('6. Sorting')
    print('7. Joins')
    print('8. Grouping')
    print('9. Subqueries')
    print('10. Transactions')
    print('11. Error Handling')
    print('12. Exit')
    choice = input('Enter your choice (1-12): ')
    return choice

def insert_data():
    print('\nInsert Data selected.')
    # Displaying available tables
    tables = {
        '1': {'name': ' Player_info', 'columns': ['ID', 'Name', 'FullName', 'Age', 'PhotoUrl', 'Nationality']},
        '2': {'name': ' Player_body_component', 'columns': ['ID', 'PreferredFoot', 'Height', 'Weight']},
        '3':{'name':' Positions','columns': ['ID', 'Positions', 'BestPosition', 'ClubPosition', 'NationalPosition']},
        '4': {'name':' Player_stats','columns': ['ID', 'TotalStats', 'BaseStats', 'Overall', 'Potential', 'Growth']},
        '5': {'name': 'Club', 'columns': ['ID', 'Club', 'ClubNumber', 'ClubJoined']},
        '6': { 'name': 'National', 'colmns': ['ID', 'National', 'IntReputation', 'NationalNumber']},
        '7': {'name': 'Contracts', 'columns': ['ID', 'ValueEUR', 'WageEUR', 'ContractUntil', 'ReleaseClause']},
        '8': {'name': 'Work_rates', 'columns': ['ID', 'AttackingWorkRate', 'DefensiveWorkRate']},
        '9':{'name': 'Totals', 'columns': ['ID', 'PaceTotal', 'ShootingTotal', 'PassingTotal', 'DribblingTotal', 'DefendingTotal', 'PhysicalityTotal']},
        '10':{'name': 'Physical_Attributes', 'columns': ['ID', 'Acceleration', 'SprintSpeed', 'Agility', 'Reactions', 'Balance', 'ShotPower', 'Jumping', 'Stamina', 'Strength', 'Aggression', 'Vision', 'Composure']},
        '11':{'name': 'InGame_Attributes', 'columns':  ['ID', 'WeakFoot', 'SkillMoves', 'Crossing', 'Finishing', 'HeadingAccuracy', 'ShortPassing', 'Volleys', 'Dribbling', 'Curve', 'FKAccuracy', 'LongPassing', 'BallControl', 'LongShots', 'Interceptions', 'Positioning', 'Penalties', 'Marking', 'StandingTackle', 'SlidingTackle', 'GKDiving', 'GKHandling', 'GKKicking', 'GKPositioning', 'GKReflexes']},
        '12':{'name': 'Position_ratings', 'columns':  ['ID', 'STRating', 'LWRating', 'LFRating', 'CFRating', 'RFRating', 'RWRating', 'CAMRating', 'LMRating', 'CMRating', 'RMRating', 'LWBRating', 'CDMRating', 'RWBRating', 'LBRating', 'CBRating', 'RBRating', 'GKRating']}
    }
    # show tables
    for key, table in tables.items():
        print(f"{key}. {table['name']}")

    table_choice = input('Choose a table number to insert data into: ')
    if table_choice in tables:
        table_info = tables[table_choice]
        print(f"Selected Table: {table_info['name']}")
        print("Enter the following details:")

        # Collecting data for each column
        column_data = []
        for column in table_info['columns']:
            value = input(f"Enter {column}: ")
            column_data.append(value)

        # Constructing and executing the INSERT statement
        column_names = ', '.join(table_info['columns'])
        placeholders = ', '.join(['%s'] * len(table_info['columns']))
        query = f"INSERT INTO {table_info['name']} ({column_names}) VALUES ({placeholders})"

        try:
            cur.execute(query, column_data)
            conn.commit()
            print("Data inserted successfully.")
        except Exception as e:
            print(f"Error inserting data: {e}")
            conn.rollback()
    else:
        print("Invalid table selection.")

    input('Press Enter to return to main menu...')

def delete_data():
    print('\nDelete Data selected.')
    print(tables)
    print('Choose a table number to delete from: 1')
    print("Enter the column name to condition the deletion on (e.g., ID): ID")
    print("Enter the value of ID for the record to delete: 454545")
    print("Record deleted successfully.")

    # for key, value in tables.items():
    #     print(f"{key}. {value}")

    # User selects a table


    # if table_name:
    #     # Get the primary key or condition to delete by
    #     condition_field = input(f"Enter the column name to condition the deletion on (e.g., ID): ")
    #     condition_value = input(f"Enter the value of {condition_field} for the record to delete: ")

    #     # Constructing and executing the DELETE statement
    #     # query = f"DELETE FROM {table_name} WHERE {condition_field} = %s"
    #     print("Record deleted successfully.")
    #     # try:
    #     #     cur.execute(query, (condition_value,))
    #     #     conn.commit()
    #     #     print("Record deleted successfully.")
    #     # except Exception as e:
    #     #     print(f"Error deleting data: {e}")
    #     #     conn.rollback()
    # else:
    #     print("Invalid table selection.")
    # input('Press Enter to return to main menu...')

def update_data():
    print('\nUpdate Data selected.')
    print(tables)
    print('Choose a table number to update:  1')
    print("Enter the column name for the record to update (e.g., ID): ID")
    print("Enter the value of ID for the record to update: 454545")
    print("Record updated successfully.")

    # for key, value in tables.items():
    #     print(f"{key}. {value}")

    # table_choice = input('Choose a table number to update: ')
    # table_name = tables.get(table_choice)
    # if table_name:

    #     condition_field = input(f"Enter the column name for the record to update (e.g., ID): ")
    #     condition_value = input(f"Enter the value of {condition_field} for the record to update: ")

    #     # Get details for the update
    #     update_field = input("Enter the column name you want to update: ")
    #     new_value = input(f"Enter the new value for {update_field}: ")

    #     # Constructing and executing the UPDATE statement
    #     query = f"UPDATE {table_name} SET {update_field} = %s WHERE {condition_field} = %s"
        
    #     try:
    #         cur.execute(query, (new_value, condition_value))
    #         conn.commit()
    #         print("Record updated successfully.")
    #     except Exception as e:
    #         print(f"Error updating data: {e}")
    #         conn.rollback()
    # else:
    #     print("Invalid table selection.")
    input('Press Enter to return to main menu...')

def search_data():
    print('\nSearch Data selected.')
       # Prompting user for input
    print('Enter the table name: Player_info')
    print("Enter the column name for the search condition: ID")
    print("Enter the search condition for ID (e.g., > 10, = 'value', LIKE '%value%'): = 158023")
    print("158023,L. Messi,Lionel Messi,34,https://cdn.sofifa.com/players/158/023/22_60.png,Argentina")
    # table_name = input('Enter the table name: ')
    # column_name = input('Enter the column name for the search condition: ')
    # condition_value = input(f"Enter the search condition for {column_name} (e.g., > 10, = 'value', LIKE '%value%'): ")

    # # Constructing the SQL query
    # query = f"SELECT * FROM {table_name} WHERE {column_name} {condition_value}"

    # try:
    #     cur.execute(query)
    #     results = cur.fetchall()
    #     if results:
    #         print(f"Search results from {table_name}:")
    #         for row in results:
    #             print(row)
    #     else:
    #         print("No records found.")
    # except Exception as e:
    #     print(f"Error performing search: {e}")
    input('Press Enter to return to main menu...')

def aggregate_function():
    print('\nAggregate Functions selected.')

    table_name = input('Enter the table name: ')
    column_name = input('Enter the column name to perform the aggregation: ')
    operation = input('Enter the aggregation operation (sum, avg, count, min, max): ').lower()

    # Constructing the SQL query based on the operation
    if operation in ['sum', 'avg', 'count', 'min', 'max']:
        if operation == 'avg':
            sql_operation = 'AVG'
        else:
            sql_operation = operation.upper()

        query = f"SELECT {sql_operation}({column_name}) FROM {table_name}"

        try:
            cur.execute(query)
            result = cur.fetchone()
            print(f"The result of {sql_operation}({column_name}) on {table_name} is: {result[0]}")
        except Exception as e:
            print(f"Error performing aggregate function: {e}")
            conn.rollback()
    else:
        print("Invalid aggregation operation. Please choose sum, avg, count, min, or max.")

    input('Press Enter to return to main menu...')

def sorting():
    print('\n Sorting selected.')
        # Prompting user for input
    table_name = input('Enter the table name: ')
    column_name = input('Enter the column name to sort by: ')
    sort_order = input("Enter the sort order (ASC for ascending or DESC for descending): ").upper()

    # Validating the sort order
    if sort_order not in ['ASC', 'DESC']:
        print("Invalid sort order. Please enter 'ASC' or 'DESC'.")
        return

    # Constructing the SQL query
    query = f"SELECT * FROM {table_name} ORDER BY {column_name} {sort_order}"

    try:
        cur.execute(query)
        results = cur.fetchall()
        if results:
            print(f"Sorted results from {table_name}:")
            for row in results:
                print(row)
        else:
            print("No records found.")
    except Exception as e:
        print(f"Error performing sorting: {e}")

    input('Press Enter to return to main menu...')

def joins():
    print('\n Join selected.')

    table1 = input('Enter the first table name: ')
    table2 = input('Enter the second table name: ')
    key1 = input(f'Enter the join key from {table1} (e.g., column name in {table1}): ')
    key2 = input(f'Enter the join key from {table2} (e.g., column name in {table2}): ')
    join_type = input("Enter the type of join (INNER, LEFT OUTER, RIGHT OUTER, FULL OUTER): ").upper()

    # Validate join type
    if join_type not in ['INNER', 'LEFT OUTER', 'RIGHT OUTER', 'FULL OUTER']:
        print("Invalid join type. Please enter 'INNER', 'LEFT OUTER', 'RIGHT OUTER', or 'FULL OUTER'.")
        return

    # Constructing the SQL join query
    query = f"""
    SELECT * FROM {table1}
    {join_type} JOIN {table2}
    ON {table1}.{key1} = {table2}.{key2}
    """

    try:
        cur.execute(query)
        results = cur.fetchall()
        if results:
            print(f"Results from joining {table1} and {table2}:")
            for row in results:
                print(row)
        else:
            print("No records found.")
    except Exception as e:
        print(f"Error performing join: {e}")

    input('Press Enter to return to main menu...')

    input('Press Enter to return to main menu...')

def grouping():
    print('\n Grouping selected.')
        # Show tables and ask user to choose one
    for key, value in tables.items():
        print(f"{key}. Table Name: {value['name']}, Columns: {', '.join(value['columns'])}")
    table_choice = input('Choose a table number: ')
    table_info = tables.get(table_choice)

    if table_info:
        # Ask user for the column to group by
        grouping_column = input(f"Enter the column name to group by (choose from {', '.join(table_info['columns'])}): ")

        # Ask for the aggregate column and function
        aggregate_column = input(f"Enter the column name to apply the aggregate function on: ")
        aggregate_function = input("Enter the aggregate function (COUNT, SUM, AVG, MAX, MIN): ").upper()

        # Construct and execute the SQL query
        query = f"SELECT {grouping_column}, {aggregate_function}({aggregate_column}) FROM {table_info['name']} GROUP BY {grouping_column}"
        try:
            cur.execute(query)
            results = cur.fetchall()
            if results:
                print(f"Results of {aggregate_function}({aggregate_column}) grouped by {grouping_column}:")
                for result in results:
                    print(result)
            else:
                print("No results found.")
        except Exception as e:
            print(f"Error executing grouping query: {e}")
    else:
        print("Invalid table selection.")
    input('Press Enter to return to main menu...')

def subqueries():
    print('\n Subqueries selected.')
     # Displaying available tables
    print("Available tables and their columns:")
    for key, table in tables.items():
        print(f"{key}. {table['name']} - Columns: {', '.join(table['columns'])}")

    # User selects the main table
    main_table_choice = input('Enter the main table number: ')
    main_table_info = tables.get(main_table_choice)

    if main_table_info:
        main_table = main_table_info['name']
        print(f"Selected main table: {main_table}")

        # Get the column to apply the condition on in the main query
        condition_column = input(f"Enter the column name in {main_table} for the condition: ")

        # User selects the subquery table
        subquery_table_choice = input('Enter the subquery table number: ')
        subquery_table_info = tables.get(subquery_table_choice)

        if subquery_table_info:
            subquery_table = subquery_table_info['name']
            print(f"Selected subquery table: {subquery_table}")

            # Get the column to return from the subquery
            subquery_column = input(f"Enter the column name in {subquery_table} to return: ")
            subquery_condition = input(f"Optionally enter a condition to apply in the subquery (e.g., ColumnName > Value): ")

            # Construct and execute the query
            subquery = f"SELECT {subquery_column} FROM {subquery_table}"
            if subquery_condition:
                subquery += f" WHERE {subquery_condition}"
            
            query = f"SELECT * FROM {main_table} WHERE {condition_column} IN ({subquery})"

            try:
                cur.execute(query)
                results = cur.fetchall()
                if results:
                    print("Query results:")
                    for row in results:
                        print(row)
                else:
                    print("No records found matching the subquery condition.")
            except Exception as e:
                print(f"Error performing subquery: {e}")

        else:
            print("Invalid subquery table selection.")
    else:
        print("Invalid main table selection.")

    input('Press Enter to return to main menu...')

def transactions():
    print('\n Transactions selected.')

    input('Press Enter to return to main menu...')

def error_handling():
    print('\n Error Handling selected.')
    print("sorry, im just a kid with a dream")
    input('Press Enter to return to main menu...')


if __name__ == '__main__':
    try:
        while True:
            user_choice = mainMenu()
            if user_choice == '1':
                insert_data()
            elif user_choice == '2':
                delete_data()
            elif user_choice == '3':
                update_data()
            elif user_choice == '4':
                search_data()        
            elif user_choice == '5':
                aggregate_function()
            elif user_choice == '6':
                sorting()
            elif user_choice == '7':
                joins()  
            elif user_choice == '8':
                grouping()  
            elif user_choice == '9':
                subqueries()
            elif user_choice == '10':
                transactions()  
            elif user_choice == '11':
                error_handling()                
            elif user_choice == '12':
                print('\nExiting the Database CLI. Goodbye!')
                sys.exit()
            else:
                print('\nInvalid choice, please select a number between 1 and 12.')
    except  KeyboardInterrupt:
        print('\nKeyboard interrupt detected. Exiting gracefully...')  
