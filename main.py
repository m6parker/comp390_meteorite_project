import requests
import sqlite3


def print_red(text: str):
    """ this function changes lines printed to terminal red """
    print(f'\033[91m{text}')


def print_green(text: str):
    """ this function changes lines printed to terminal green """
    print(f'\033[92m{text}')


def get_request(target_url: str):
    """ this function issues a get request to the target url passed in as its parameter
    a response object is returned
    the status code of the request is also reported """

    response_obj = requests.get(target_url)
    if response_obj.status_code != 200:
        print_red(f'get request was unsuccessful :(\n{response_obj.status_code} [{response_obj.reason}]')
        return response_obj
    else:
        print_green(f'get request is successful :)\n{response_obj.status_code} [{response_obj.reason}]')
        return response_obj


def convert_content_to_json(response_obj: requests.Response):
    """ this function accepts a request response object as its parameter and tries to
    convert the response object content to a JSON data object
    'None' is returned if the conversion is no successful """
    json_data_obj = None
    try:
        json_data_obj = response_obj.json()
        print_green(f'response object content converted to json object.\n')
    except requests.exceptions.JSONDecodeError as json_decode_error:
        print_red(f'An error has occurred while trying to convert the request content to a JSON object:\n'
                  f'{json_decode_error}')
    finally:
        return json_data_obj


def connect_to_db(meteorite_db: str):
    """ this function tries to establish a connection to the database using the parameter
    and incoming string called meteorite_db and returns none if unsuccessful and returns
    a database connection object if successful """
    db_connection = None
    try:
        db_connection = sqlite3.connect(meteorite_db)
        print_green('connection to database was successful :)')
    except sqlite3.Error as db_error:
        print_red(f'{db_error}: connection to database was unsuccessful :(')
    finally:
        return db_connection


def create_db_cursor(db_connection_obj: sqlite3.Connection):
    """ this function creates a database cursor object using the incoming sqlite
     connection object parameter given. It returns none if unsuccessful and a cursor
     object if successful. Appropriate messages are displayed. """
    cursor_obj = None
    try:
        cursor_obj = db_connection_obj.cursor()
        print_green(f'cursor object created successfully on {db_connection_obj}\n'
                    f'cursor object: {cursor_obj}')
    except sqlite3.Error as db_error:
        print_red(f'cursor object could not be created: {db_error}')
    finally:
        return cursor_obj


def create_sql_tables(db_cursor):
    """ this function tries to create the 7 sqlite tables, one for each region we're working with
     an exception is caught if code fails"""
    try:
        # create a table in the database to store ALL the meteorite data (if it does not already exist)
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS Africa_MiddleEast(
                                    name TEXT,
                                    mass TEXT,
                                    reclat TEXT,
                                    reclong TEXT);''')
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS Europe(
                                            name TEXT,
                                            mass TEXT,
                                            reclat TEXT,
                                            reclong TEXT);''')
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS Upper_Asia(
                                            name TEXT,
                                            mass TEXT,
                                            reclat TEXT,
                                            reclong TEXT);''')
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS Lower_Asia(
                                            name TEXT,
                                            mass TEXT,
                                            reclat TEXT,
                                            reclong TEXT);''')
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS Australia(
                                            name TEXT,
                                            mass TEXT,
                                            reclat TEXT,
                                            reclong TEXT);''')
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS North_America(
                                            name TEXT,
                                            mass TEXT,
                                            reclat TEXT,
                                            reclong TEXT);''')
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS South_America(
                                            name TEXT,
                                            mass TEXT,
                                            reclat TEXT,
                                            reclong TEXT);''')
        print_green('SQLite tables created successfully')
    except sqlite3.Error as error:
        print_red('an error has occurred creating the tables :(')
        print_red(f'{error}')
    finally:
        return db_cursor


def loop_thru_data(db_cursor, json_data_obj):
    """ this function goes through the data and sorts it into tables based off the reclat and reclong
    numbers if they have any. It tries to insert them into the table after clearing the table of old stuff"""
    # geolocation bounding box -- (left,bottom,right,top)
    bound_box_dict = {
        'Africa_MiddleEast_Meteorites': (-17.8, -35.2, 62.2, 37.6),
        'Europe_Meteorites': (-24.1, 36, 32, 71.1),
        'Upper_Asia_Meteorites': (32.2, 35.8, 190.4, 72.7),
        'Lower_Asia_Meteorites': (58.2, -9.9, 154, 38.6),
        'Australia_Meteorites': (112.9, -43.8, 154.3, -11.1),
        'North_America_Meteorites': (-168.2, 12.8, -52, 71.5),
        'South_America_Meteorites': (-81.2, -55.8, -34.4, 12.6)
    }
    for element in json_data_obj:
        if json_data_obj.reclat is not None:
            if -35.2 <= float(reclat) <= 37.6 and -17.8 <= float(reclong) <= 62.2:
                try:
                    db_cursor.execute('''DELETE FROM Africa_MiddleEast''')
                    db_cursor.execute('''Insert into Africa_MiddleEast VALUES(?, ?, ?, ?)''',
                              (json_data_obj.get('name', None),
                               json_data_obj.get('mass', None),
                               json_data_obj.get('reclat', None),
                               json_data_obj.get('reclong', None)))
                    print_green('new row inserted!')
                except sqlite3.Error as error:
                    print_red(f'{error}: error inserting into table...')
            elif 36 <= float(reclat) <= 71.1 and -24.1 <= float(reclong) <= 32:
                try:
                    db_cursor.execute('''DELETE FROM Europe''')
                    db_cursor.execute('''Insert into Europe VALUES(?, ?, ?, ?)''',
                              (json_data_obj.get('name', None),
                               json_data_obj.get('mass', None),
                               json_data_obj.get('reclat', None),
                               json_data_obj.get('reclong', None)))
                    print_green('new row inserted!')
                except sqlite3.Error as error:
                    print_red(f'{error}: error inserting into table...')
            elif 35.8 <= float(reclat) <= 72.7 and 32.2 <= float(reclong) <= 190.4:
                try:
                    db_cursor.execute('''DELETE FROM Upper_Asia''')
                    db_cursor.execute('''Insert into Upper_Asia VALUES(?, ?, ?, ?)''',
                              (json_data_obj.get('name', None),
                               json_data_obj.get('mass', None),
                               json_data_obj.get('reclat', None),
                               json_data_obj.get('reclong', None)))
                    print_green('new row inserted!')
                except sqlite3.Error as error:
                    print_red(f'{error}: error inserting into table...')
            elif -9.9 <= float(reclat) <= 38.6 and 58.2 <= float(reclong) <= 154:
                try:
                    db_cursor.execute('''DELETE FROM Lower_Asia''')
                    db_cursor.execute('''Insert into Lower_Asia VALUES(?, ?, ?, ?)''',
                              (json_data_obj.get('name', None),
                               json_data_obj.get('mass', None),
                               json_data_obj.get('reclat', None),
                               json_data_obj.get('reclong', None)))
                    print_green('new row inserted!')
                except sqlite3.Error as error:
                    print_red(f'{error}: error inserting into table...')
            elif -43.8 <= float(reclat) <= -11.1 and 112.9 <= float(reclong) <= 154.3:
                try:
                    db_cursor.execute('''DELETE FROM Australia''')
                    db_cursor.execute('''Insert into Australia VALUES(?, ?, ?, ?)''',
                              (json_data_obj.get('name', None),
                               json_data_obj.get('mass', None),
                               json_data_obj.get('reclat', None),
                               json_data_obj.get('reclong', None)))
                    print_green('new row inserted!')
                except sqlite3.Error as error:
                    print_red(f'{error}: error inserting into table...')
            elif 12.8 <= float(reclat) <= 71.5 and -168.2 <= float(reclong) <= -52:
                try:
                    db_cursor.execute('''DELETE FROM North_America''')
                    db_cursor.execute('''Insert into North_America VALUES(?, ?, ?, ?)''',
                              (json_data_obj.get('name', None),
                               json_data_obj.get('mass', None),
                               json_data_obj.get('reclat', None),
                               json_data_obj.get('reclong', None)))
                    print_green('new row inserted!')
                except sqlite3.Error as error:
                    print_red(f'{error}: error inserting into table...')
            elif -55.8 <= float(reclat) <= 12.6 and -81.2 <= float(reclong) <= -34.4:
                try:
                    db_cursor.execute('''DELETE FROM South_America''')
                    db_cursor.execute('''Insert into South_America VALUES(?, ?, ?, ?)''',
                              (json_data_obj.get('name', None),
                               json_data_obj.get('mass', None),
                               json_data_obj.get('reclat', None),
                               json_data_obj.get('reclong', None)))
                    print_green('new row inserted!')
                except sqlite3.Error as error:
                    print_red(f'{error}: error inserting into table...')
            else:
                pass



def main():
    """main function calls the get_request function passing the nasa url as the target url to access the data
    then it calls another function to create database and establish a connection and insert the data onto the database
    in different tables. the connection is commited and closed. """
    # calls the function checking the status code above then puts in resp obj
    target_url = 'https://data.nasa.gov/resource/gh4g-9sfh.json'
    response_obj = get_request(target_url)
    json_data_object = convert_content_to_json(response_obj)
    # connect to a sqlite database - create it if it does not exist
    db_connection = connect_to_db('meteorite_db.db')
    db_cursor = create_db_cursor(db_connection)
    db_tables = create_sql_tables(db_cursor)
    loop_thru_data(db_cursor, json_data_object)
    db_connection.commit()
    db_cursor.close()


if __name__ == '__main__':
    main()
