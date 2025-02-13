import requests
import sqlite3
import pandas as pd
import urllib3
import json

def create_university_tables(db_name):
    #Open connection with given database and create tables
    #universities, web_pages and domains if they dont exist
    #using 3NF normalized form for designing schema

    connection = sqlite3.connect(db_name)
    cursor = connection.cursor()
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_weather (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                maximum_daily_temperature TEXT,
                state_province TEXT,
                alpha_two_code TEXT,
                country TEXT
            )
        ''')

    cursor.execute('''
             CREATE TABLE IF NOT EXISTS web_pages (
                 web_page_id INTEGER PRIMARY KEY AUTOINCREMENT,
                 university_id INTEGER,
                 web_page TEXT,
                 FOREIGN KEY (university_id) REFERENCES universities(id)
             )
         ''')

    cursor.execute('''
                 CREATE TABLE IF NOT EXISTS domains (
                     domain_id INTEGER PRIMARY KEY AUTOINCREMENT,
                     university_id INTEGER,
                     domain_name TEXT,
                     FOREIGN KEY (university_id) REFERENCES universities(id)
                 )
             ''')

    connection.commit()
    return connection

def fetch_universities_data_partial(url):
    http = urllib3.PoolManager()

    response = http.request('GET', url, preload_content=False, timeout=120)
    buffer = ''
    try:
        if response.status == 200:
            for chunk in response.stream(8192):  # 8 KB chunks
                buffer = buffer + chunk.decode('utf-8')
        else:
                print(f"Request failed with status code {response.status}")
    except Exception as e:
        print('Ignoring server error '+str(e))

    #make the json string valid in case it is malformed
    valid_json_str = buffer.rsplit('}, {', 1)[0] + '}]'

    return valid_json_str

def fetch_universities_data(url):
    try:
        response_obj = requests.get(url)
        response = response_obj.json()
    except Exception as e:
        print('Trying to fetch partial data')
        response_str = fetch_universities_data_partial(url)
        response = json.loads(response_str)

    df = pd.DataFrame(response)
    return df


def insert_universities_info(conn, university_info_df):
    cursor = conn.cursor()
    # Insert university_info DataFrame into the university_info table
    for index, row in university_info_df.iterrows():
        cursor.execute('''
            INSERT INTO university_info (name, state_province, alpha_two_code, country)
            VALUES (?, ?, ?, ?)
        ''', (row['name'], row['state-province'], row['alpha_two_code'], row['country']))

        # Get the university_id of the last inserted row
        university_id = cursor.lastrowid

        # Insert the web pages
        for web_page in row['web_pages']:
            cursor.execute('''
                INSERT INTO web_pages (university_id, web_page)
                VALUES (?, ?)
            ''', (university_id, web_page))

        # Insert the domains
        for domain in row['domains']:
            cursor.execute('''
                INSERT INTO domains (university_id, domain_name)
                VALUES (?, ?)
            ''', (university_id, domain))

    # Commit the transaction to make sure all changes are saved
    conn.commit()

def main():
    print("Setting up the database...")
    print("Creating universities table...")
    conn = create_university_tables("reup.db")
    print("Fetching universities data...")
    data_frame = fetch_universities_data("http://universities.hipolabs.com/search?country=China")
    print("Inserting universities data...")
    insert_universities_info(conn, data_frame)
    conn.close()
    print("Done!")

if __name__ == "__main__":
    main()