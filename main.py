import requests
import sqlite3


# 7 tables for different regions, loop through and find which table it belongs
# if meteor falls in middle of the ocean it doesn't appear on the database
# use get request to remotely get data from nasa website and put in appropriate table
# name, mass, lat, long -- columns not 12
# project 3 including mass
# 7 tables
# one function to connect to database, pass database name, simple try except to try to connect to database
#   return if successful, return none if not.
# function to create tables call it 7 times.
# decode json dictionary with json decoder
# looping after tables made, through json data list, each dict correspons to meteor, only looping thru ONE TIME
# is geodict not there? does it have reclat, reclong? do nothing if not - first thing have to check.
# loop thru once: first ask if has geolocation? no... skip to next in list
# geolocation = yes - extract geolocation from that dictionary. extract = get values and put into tuple/variable.
# then loop thru bounding boxes (in form of a dictionary)
# need commit to see the tables filled

# target_url = 'https://data.nasa.gov/resource/gh4g-9sfh.json'
# geolocation bounding box -- (left,bottom,right,top)


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


def loop_thru_data(db_cursor):
    bound_box_dict = {
        'Africa_MiddleEast_Meteorites': (-17.8, -35.2, 62.2, 37.6),
        'Europe_Meteorites': (-24.1, 36, 32, 71.1),
        'Upper_Asia_Meteorites': (32.2, 35.8, 190.4, 72.7),
        'Lower_Asia_Meteorites': (58.2, -9.9, 154, 38.6),
        'Australia_Meteorites': (112.9, -43.8, 154.3, -11.1),
        'North_America_Meteorites': (-168.2, 12.8, -52, 71.5),
        'South_America_Meteorites': (-81.2, -55.8, -34.4, 12.6)
    }
    for element in db_cursor:
        print(element)
    # try:
    #     db_cursor.execute('''Insert into ''')
    #     print_green('new row inserted!')
    # except sqlite3.Error as error:
    #     print_red(f'{error}: error inserting into table...')

#
# # this function takes two parameters: a dictionary object and a target key
# def convert_obj_to_string(dict_record, key):
#     # <dict>.get() gets the value associated with a key in a dictionary
#     # the second parameter for <dict>.get() tells the get function to return 'None' if the key does not exist
#     if dict_record.get(key, None) is None:
#         return None
#     # return a string version of the <dict>[key] object if a key is present
#     return json.dumps(dict_record[key])


def main():
    # calls the function checking the status code above then puts in resp obj
    target_url = 'https://data.nasa.gov/resource/gh4g-9sfh.json'
    response_obj = get_request(target_url)
    json_data_object = convert_content_to_json(response_obj)
    # connect to a sqlite database - create it if it does not exist
    db_connection = connect_to_db('meteorite_db.db')
    db_cursor = create_db_cursor(db_connection)
    db_tables = create_sql_tables(db_cursor)
    loop_thru_data(db_cursor)

    #
    # # run a GET request
    # response = requests.get('https://data.nasa.gov/resource/gh4g-9sfh.json')
    #
    # # convert response text to json format (i.e. list of dictionaries)
    # # (the json() decoder function only works if the text is formatted correctly)
    # json_data = response.json()
    #
    # # connect to database
    # db_connection = None
    # try:
    #     # connect to a sqlite database - create it if it does not exist
    #     db_connection = sqlite3.connect('meteorite_db.db')
    #     # create a cursor object - this cursor object will be used for all operations pertaining to the database
    #     db_cursor = db_connection.cursor()
    #
    #     # create a table in the database to store ALL the meteorite data (if it does not already exist)
    #     # the parentheses following the table name ("meteorite_data") contains a list of column names
    #     # and the data type of values that will be inserted into those columns
    #     db_cursor.execute('''CREATE TABLE IF NOT EXISTS meteorite_data(
    #                             name TEXT,
    #                             id INTEGER,
    #                             nametype TEXT,
    #                             recclass TEXT,
    #                             mass TEXT,
    #                             fall TEXT,
    #                             year TEXT,
    #                             reclat TEXT,
    #                             reclong TEXT,
    #                             geolocation TEXT,
    #                             states TEXT,
    #                             counties TEXT);''')
    #
    #     # clear the 'meteorite_data' table if it already contains data from last time the program was run
    #     db_cursor.execute('DELETE FROM meteorite_data')
    #
    #     # read all the data from the specified json URL and insert it into the database:
    #     # loop through each dictionary entry in the JSON list
    #     for record in json_data:
    #         # some keys may not exist because there is no value for that record - use <dict>.get()
    #         # to INSERT 'None' if the key is not found
    #         db_cursor.execute('''INSERT INTO meteorite_data VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
    #                           (record.get('name', None),
    #                            int(record.get('id', None)),
    #                            record.get('nametype', None),
    #                            record.get('recclass', None),
    #                            record.get('mass', None),
    #                            record.get('fall', None),
    #                            record.get('year', None),
    #                            record.get('reclat', None),
    #                            record.get('reclong', None),
    #                            convert_obj_to_string(record, 'geolocation'),    # convert geolocation <dict> to string
    #                            record.get(':@computed_region_cbhk_fwbd', None),
    #                            record.get(':@computed_region_nnqa_25f4', None)))
    #
    #     # run a SELECT query on the table that holds all meteorite data
    #     db_cursor.execute('SELECT * FROM meteorite_data WHERE id <= 1000')
    #     # get the result of the query as a list of tuples
    #     q1_result = db_cursor.fetchall()
    #
    #     # create a table in the database to store the filtered data (if it does not already exist)
    #     # this table will hold the resulting rows of our SELECT query
    #     db_cursor.execute('''CREATE TABLE IF NOT EXISTS filtered_data(
    #                                            name TEXT,
    #                                            id INTEGER,
    #                                            nametype TEXT,
    #                                            recclass TEXT,
    #                                            mass TEXT,
    #                                            fall TEXT,
    #                                            year TEXT,
    #                                            reclat TEXT,
    #                                            reclong TEXT,
    #                                            geolocation TEXT,
    #                                            states TEXT,
    #                                            counties TEXT);''')
    #
    #     # clear the 'filtered_data' table if it already contains data from last time the program was run
    #     db_cursor.execute('DELETE FROM filtered_data')
    #
    #     # fill the filtered table
    #     for tuple_entry in q1_result:
    #         db_cursor.execute('''INSERT INTO filtered_data VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', tuple_entry)
    #
    #     # commit all changes made to the database
    #     db_connection.commit()
    #     db_cursor.close()
    #
    #     # print(q1_result)
    #
    # # catch any database errors
    # except sqlite3.Error as db_error:
    #     # print the error description
    #     print(f'A Database Error has occurred: {db_error}')
    #
    # # 'finally' blocks are useful when behavior in the try/except blocks is not predictable
    # # The 'finally' block will run regardless of what happens in the try/except blocks.
    # finally:
    #     # close the database connection whether an error happened or not (if a connection exists)
    #     if db_connection:
    #         db_connection.close()
    #         print('Database connection closed.')
    #


if __name__ == '__main__':
    main()
