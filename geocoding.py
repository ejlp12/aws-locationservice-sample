import csv
import json
import logging
import os
import requests
import datetime
from urllib3.util import Retry

# AWS Location Service API endpoint
AWS_REGION = os.environ.get('AWS_REGION', 'ap-southeast-1')
PLACE_INDEX_NAME = os.environ.get('PLACE_INDEX_NAME', 'change_to_your_place_index_name')
API_ENDPOINT = f"https://places.geo.{AWS_REGION}.amazonaws.com/places/v0/indexes/{PLACE_INDEX_NAME}/search/text"

# API key
API_KEY = os.environ.get('LOCATION_SERVICE_API_KEY', 'change_to_your_ALS_API_key_start_with_v1.public')

# Filter countries (default to "ID" for Indonesia)
FILTER_COUNTRIES = os.environ.get('FILTER_COUNTRIES', 'IDN').split(',')

# Number of lines to skip from the beginning of the CSV file
LINES_TO_SKIP = int(os.environ.get('LINES_TO_SKIP', '1'))

# Number of lines to read from the CSV file
LINES_TO_READ = int(os.environ.get('LINES_TO_READ', '12650'))

# Get the current date and time
now = datetime.datetime.now()

# Format the date and time as a string
date_time_str = now.strftime("%Y-%m-%d_%H-%M-%S")

# Configure logging
logging.basicConfig(filename=f"geocoding{date_time_str}.log", level=logging.INFO)

# Function to send request to AWS Location Service
def get_location(address_components):
    payload = {
        #"Text": f"{address_components[6]} {address_components[7]}, {address_components[5]}, {address_components[4]}, {address_components[3]}, {address_components[2]}, {address_components[1]}",
        "Text": f"{address_components[6]}",
        "FilterCountries": FILTER_COUNTRIES,
        "MaxResults": 1
    }  
    params = {
        "key": API_KEY,
    }
    headers = {"Content-Type": "application/json"}
    retries = Retry(total=3, backoff_factor=0.5)
    with requests.Session() as session:
        session.mount("http://", requests.adapters.HTTPAdapter(max_retries=retries))
        session.mount("https://", requests.adapters.HTTPAdapter(max_retries=retries))
        response = session.post(API_ENDPOINT, data=json.dumps(payload), headers=headers, params=params)
        logging.info(f"Request: {response.request.method} {response.request.url}")
        logging.info(f"Request Body: {payload}")
        logging.info(f"Response: {response.status_code} {response.text}")
        print(f"\nRequest: {response.request.method} {response.request.url}")
        print(f"Request Body: {payload}")
        print(f"\nResponse: {response.status_code} {response.text}")
        if response.status_code == 200:
            data = response.json()
            #return data.get("latitude"), data.get("longitude")

            try:
                # Access the 'results' array
                results = data.get('Results', [])
                place = results[0].get('Place')
                geometry = place.get('Geometry')
                points = geometry.get('Point')
                        
                # Print the 'lat' and 'lng' values
                print(f"\n\nLatitude: {points[0]}, Longitude: {points[1]}")
                return points[0], points[1]
            
            except:
                print("Cannot get [lat, long] from response")

    return None, None

# Function to process the input CSV file
def process_csv(input_file, output_file):
    with open(input_file, "r") as infile, open(output_file, "w", newline="") as outfile:
        reader = csv.reader(infile, delimiter=";")
        writer = csv.writer(outfile)
        writer.writerow(["LineNumber", "SerialNo", "Latitude", "Longitude"])
        for line_num, line in enumerate(reader, start=1):
            if line_num <= LINES_TO_SKIP:  # Skip the specified number of lines
                continue
            if LINES_TO_READ and line_num > LINES_TO_SKIP + LINES_TO_READ:  # Stop after reading the specified number of lines
                break
            serial_no = line[0]
            latitude, longitude = get_location(line)
            writer.writerow([line_num, serial_no, latitude, longitude])

# Main function
def main():
    input_file = "address_input.csv"
    output_file = f"geocoding_output_{date_time_str}.csv"
    process_csv(input_file, output_file)

if __name__ == "__main__":
    main()
