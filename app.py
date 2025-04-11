from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import pandas as pd
import json
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, select
from datetime import datetime
from tqdm import tqdm
import threading
import time
import os

app = Flask(__name__)
CORS(app)

# URLs to fetch data from
urls = [
    "https://applegreenstores.com/fuel-prices/data.json",
    "https://fuelprices.asconagroup.co.uk/newfuel.json",
    "https://storelocator.asda.com/fuel_prices_data.json",
    "https://fuelprices.esso.co.uk/latestdata.json",
    "https://jetlocal.co.uk/fuel_prices_data.json",
    "https://www.morrisons.com/fuel-prices/fuel.json",
    "https://moto-way.com/fuel-price/fuel_prices.json",
    "https://fuel.motorfuelgroup.com/fuel_prices_data.json",
    "https://www.rontec-servicestations.co.uk/fuel-prices/data/fuel_prices_data.json",
    "https://api.sainsburys.co.uk/v1/exports/latest/fuel_prices_data.json",
    "https://www.sgnretail.uk/files/data/SGN_daily_fuel_prices.json",
    "https://www.tesco.com/fuel_prices/fuel_prices_data.json",
]

# Database setup
engine = create_engine(os.getenv("DATABASE_URL").strip())
metadata = MetaData()

fuel_prices = Table('fuel_prices', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('site_id', String),
    Column('brand', String),
    Column('address', String),
    Column('postcode', String),
    Column('location', String),
    Column('prices', String),
    Column('date', String),
)

fuel_prices_metadata = Table('fuel_prices_metadata', metadata,
    Column('id', Integer, primary_key=True),
    Column('last_updated', String),
)

metadata.create_all(engine)

def fetch_and_process_data(url):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "application/json",
            "Connection": "keep-alive"
        }
        response = requests.get(url, headers=headers)
        if 'application/json' in response.headers.get('Content-Type', '') or 'text/plain' in response.headers.get('Content-Type', ''):
            data = response.json() if response.headers.get('Content-Type').startswith('application/json') else json.loads(response.text)
            stations = data.get('stations', data)  # Fallback to data if 'stations' key is missing
            if not stations:  # Checking if 'stations' key exists in JSON
                print(f"No 'stations' key found in data from {url}")
            df = pd.DataFrame(stations)
            df['location'] = df['location'].apply(json.dumps)
            df['prices'] = df['prices'].apply(json.dumps)
            df['date'] = str(datetime.now().date())  # Add current date to data
            print(f"Fetched and processed data from {url}")
            return df
        else:
            print(f"Unexpected content type from {url}: {response.headers.get('Content-Type')}")
    except Exception as e:
        print(f"Error fetching data from {url}: {e}")
    return pd.DataFrame()  # Return an empty DataFrame if any error occurs

def check_existing_data(conn, url):
    query = select(fuel_prices.c.date).where(fuel_prices.c.date == str(datetime.now().date()))
    result = conn.execute(query).fetchall()
    return len(result) > 0

def main():
    data_frames = []
    with engine.connect() as conn:
        print("Checking stations: ", end="")  # Checking stations
        for i, url in enumerate(tqdm(urls, ncols=100, desc="Checking stations")):
            if check_existing_data(conn, url):
                print(f"Data for {url} already exists in the database. Skipping fetch.")
                continue
            df = fetch_and_process_data(url)
            if not df.empty:
                data_frames.append(df)
        if data_frames:
            combined_df = pd.concat(data_frames, ignore_index=True)
            combined_df.to_sql('fuel_prices', engine, if_exists='append', index=False)
            conn.execute(fuel_prices_metadata.delete())
            conn.execute(fuel_prices_metadata.insert().values(last_updated=str(datetime.now())))
        else:
            print("No new data to insert into the database.")

@app.route('/api/prices', methods=['GET'])
def get_prices():
    postcode = request.args.get('postcode', '')
    fuel_types = request.args.get('filters', '{}')
    filters = json.loads(fuel_types)

    query = "SELECT * FROM fuel_prices WHERE date = %s"
    df = pd.read_sql(query, engine, params=(str(datetime.now().date()),))
    df['location'] = df['location'].apply(json.loads)
    df['prices'] = df['prices'].apply(json.loads)

    if postcode:
        df = df[df['postcode'].str.contains(postcode, case=False)]

    filtered_data = []
    for _, row in df.iterrows():
        prices = row['prices']
        station_data = {
            'brand': row['brand'],
            'address': row['address'],
            'postcode': row['postcode'],
            'unleaded': prices.get('E10') if filters.get('unleaded', True) else None,
            'superUnleaded': prices.get('E5') if filters.get('superUnleaded', True) else None,
            'diesel': prices.get('B7') if filters.get('diesel', True) else None,
        }
        filtered_data.append(station_data)

    return jsonify(filtered_data)

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

if __name__ == "__main__":
    def run_main_in_background():
        time.sleep(5)  # Optional delay to ensure server is stable
        try:
            main()
        except Exception as e:
            print(f"Error in main(): {e}")
    
    # Run main() in a background thread so it doesn't block app startup
    threading.Thread(target=run_main_in_background).start()

    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
