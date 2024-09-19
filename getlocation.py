import csv
import os
import requests
import json
import datetime
import logging
from itertools import islice
from urllib3.util import Retry

# Set your Amazon Location Service API key
API_KEY = os.environ.get('LOCATION_SERVICE_API_KEY', 'put_your_als_api_key_here')
PLACE_INDEX_NAME = os.environ.get('PLACE_INDEX_NAME','change_to_your_als_place_index_name')
AWS_REGION = os.environ.get('AWS_REGION', 'ap-southeast-1')

# Number of lines to skip from the beginning of the CSV file
LINES_TO_SKIP = int(os.environ.get('LINES_TO_SKIP', '1'))

# Number of lines to read from the CSV file
LINES_TO_READ = int(os.environ.get('LINES_TO_READ', '12500'))

# Get the current date and time
now = datetime.datetime.now()

# Format the date and time as a string
date_time_str = now.strftime("%Y-%m-%d_%H-%M-%S")
output_file_name = f"location_output_{date_time_str}.csv"

# Configure logging
logging.basicConfig(filename=f"locations_output_{date_time_str}.log", level=logging.INFO)


# HTTP status code descriptions
STATUS_CODES = {
    400: "Bad Request",
    403: "Forbidden",
    404: "Not Found",
    500: "Internal Server Error",
    503: "Service Unavailable"
}


# Open the CSV file
with open('location_input.csv', 'r') as file, \
     open(output_file_name, 'w', newline='') as output_file:
        
        reader = csv.DictReader(file)
        fieldnames = reader.fieldnames.copy()  # Get the header row as a list
        
        writer = csv.writer(output_file)
        writer.writerow([
             'LocationId', 'latitude', 'longitude', 
             'Province', 'City', 'District', 
             'Subdistrict', 'Zipcode','Address'])  

        #Read the specified number of lines from the CSV
        for row in islice(reader, LINES_TO_READ):
            location_id = row['LocationID']
            latitude = float(row['latitude'])
            longitude = float(row['longitude'])
            
            # Call the Amazon Location Service API to get the location name
            url = f"https://places.geo.{AWS_REGION}.amazonaws.com/places/v0/indexes/{PLACE_INDEX_NAME}/search/position"
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            
            params = {
                "key": API_KEY
            }

            # Prepare the request payload
            payload = {
                "Position": [longitude, latitude],
                "MaxResults": 1
            }        
            
            retries = Retry(total=3, backoff_factor=0.5)
            with requests.Session() as session:
                session.mount("http://", requests.adapters.HTTPAdapter(max_retries=retries))
                session.mount("https://", requests.adapters.HTTPAdapter(max_retries=retries))
                response = session.post(url, data=json.dumps(payload), headers=headers, params=params)


                # Print the full HTTP request and response
                logging.info(f"Location ID: {location_id}\n")
                logging.info(f"Request: {response.request.method} {response.request.url}\n")
                logging.info(f"Request Headers: {response.request.headers}\n")
                logging.info(f"Request Body: {payload}\n")
                logging.info(f"Response: {response.status_code}\n")
                logging.info(f"Response Headers: {response.headers}\n")
                logging.info(f"{response.text}")

                print(f"Location ID: {location_id}")
                print("Request:")
                print(response.request.method, response.request.url)
                print("\n".join(f"{k}: {v}" for k, v in response.request.headers.items()))
                print(response.request.body)
                print("\nResponse:")
                print(f"Status Code: {response.status_code}")
                print("\n".join(f"{k}: {v}" for k, v in response.headers.items()))
                print(response.text)
                print("\n")
            
                
                # Print the location name
                if response.status_code == 200 and response.json()['Results']:
                
                    try:
                        print(f"Location ID: {location_id}, Name: {response.json()['Results'][0]['Place']['Label']}, Response Payload: {response}")
                        province = response.json()['Results'][0]['Place']['Region']
                        city = response.json()['Results'][0]['Place']['Municipality']
                        district = response.json()['Results'][0]['Place']['SubRegion']
                        subdistrict = response.json()['Results'][0]['Place']['Neighborhood']
                        zipcode = response.json()['Results'][0]['Place']['PostalCode']
                        address = response.json()['Results'][0]['Place']['Label']

                    except:
                        print("Error parsing the reponse") 

                    # "Street":"Tua","Neighborhood":"Tua","Municipality":"Marga","SubRegion":"Tabanan","Region":"Bali","Country":"IDN","PostalCode":"82181"
                    # Province; City; District; Subdistrict; Zipcode; Address
                else:
                    status_code_description = STATUS_CODES.get(response.status_code, "Unknown Error")
                    print(f"Location ID: {location_id}, Name: Not found, Error: {status_code_description}, Response Payload: {response.json()}")
                
                # Write the row to the output CSV file
                writer.writerow([
                    location_id,
                    latitude,
                    longitude,
                    province,
                    city,
                    district,
                    subdistrict,
                    zipcode,
                    address
                ])
