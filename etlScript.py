import pandas as pd
import mysql.connector;
from dotenv import load_dotenv
import os
import logging 

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


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
    try:
        with mysql.connector.connect(**source_db_config) as connection:
            cursor = connection.cursor()
            query = 'SELECT * FROM flight_data'
            logging.info(f'extracting data : {query}')
            cursor.execute(query)
            rows = cursor.fetchall()
            logging.info(f'succesfully extracted {len(rows)} rows')
            return rows;
    except mysql.connector.Error as err:
        logging.error(f"something is wrong while extracting data :{err} ")
        return []

def transform(rows):

    try:
        df = pd.DataFrame(rows , columns=['id' , 'airline' , 'departure' , 'arrival' , 'price'])
        df.dropna(inplace=True)
        df = df.drop_duplicates()
        df = df.loc[df['price'] > 0  , ['id' , 'airline' , 'departure' , 'arrival' , 'price']]

        #type conversion if not already in dt
        df['departure'] = pd.to_datetime(df['departure'] , errors='coerce')
        df['arrival'] = pd.to_datetime(df['arrival'] , errors='coerce')

        df.dropna(subset=['arrival' , 'departure'] , inplace=True)

        df['flight_duration'] = (df['arrival'] - df['departure']).dt.total_seconds() / 3600 #in hours

        #doing dt to str conversion so insertion dont throw any error
        df['departure']  = df['departure'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df['arrival']  = df['arrival'].dt.strftime('%Y-%m-%d %H:%M:%S')

        df['airline'] = df['airline'].replace('Airline 1' , 'AirIndia')
        
        transformed_data   = df.to_dict(orient='records')
        logging.info(f'transformation complete')
        return transformed_data;
    except Exception as e:
        logging.error(f'error while transforming data: {e}')
        return []

def load(transformed_data):
    try:
        with mysql.connector.connect(**destination_db_config) as connection:
            cursor = connection.cursor()

            check_table = '''
                select count(*) from information_schema.tables where table_schema = %s AND table_name = %s
                '''
            cursor.execute(check_table , (destination_db_config['database'] , 'flight_data_transformed'))
            table_exists = cursor.fetchone()[0]
            if not table_exists:
                create_table = '''
                    create table flight_data_transformed(
                        id int auto_increment primary key,
                        airline varchar(255),
                        departure datetime,
                        arrival datetime,
                        price float,
                        flight_duration float
                    )
                '''
                cursor.execute(create_table)
                logging.info(f"new table created : flight_data_tranformed")
            
            insert_query = '''
                    INSERT INTO flight_data_transformed(airline , departure , arrival , price , flight_duration)
                    VALUES(%s , %s, %s , %s , %s )
                '''
                    
            data_to_insert = [(data['airline'] , data['departure'] , data['arrival'] , data['price'] , data['flight_duration'])
                for data in transformed_data ]
            
            cursor.executemany(insert_query , data_to_insert)
            
            connection.commit() 
            logging.info(f'successfully inserted {len(transformed_data)} rows')
    except mysql.connector.Error as e:
        logging.error(f'error while inserting data {e}')
        

def run_etl():
    rows = extract()
    transformed_data = transform(rows)
    load(transformed_data)


if __name__ == "__main__":
    run_etl()