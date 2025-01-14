import pandas as pd
import mysql.connector;
from dotenv import load_dotenv
import os

load_dotenv()

source_db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': os.getenv('DB_PASSWORD'),
    'database': 'source_db'
}

destination_db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': os.getenv('DB_PASSWORD'),
    'database': 'destination_db'
}

def extract():
    connection = mysql.connector.connect(**source_db_config);
    cursor = connection.cursor();
    cursor.execute('SELECT * FROM flight_data')
    rows = cursor.fetchall();
    cursor.close();
    connection.close();
    return rows;

def transform(rows):
    df = pd.DataFrame(rows , columns=['id' , 'airline' , 'departure' , 'arrival' , 'price'])
    df.dropna(inplace=True)
    df.drop_duplicates()
    df = df.loc[df['price'] > 0  , ['id' , 'airline' , 'departure' , 'arrival' , 'price']]

    df['flight_duration'] = (df['arrival'] - df['departure']).dt.total_seconds() / 3600 #in hours
    df['airline'] = df['airline'].replace('Airline 1' , 'AirIndia')

    transformed_data   = df.to_dict(orient='records')
    return transformed_data;

def load(transformed_data):
    connection = mysql.connector.connect(**destination_db_config)
    cursor = connection.cursor()

    for data in transformed_data:
        departure_str = data['departure'].strftime('%Y-%m-%d %H:%M:%S')
        arrival_str = data['arrival'].strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute(
            'INSERT INTO flight_data_transformed(airline , departure , arrival , price , flight_duration)'
            'VALUES(%s , %s, %s , %s , %s )',
            (data['airline'] , departure_str , arrival_str , data['price'] , data['flight_duration'])
        )
    
    connection.commit()
    connection.close()

def run_etl():
    rows = extract()
    transformed_data = transform(rows)
    load(transformed_data)


if __name__ == "__main__":
    run_etl()