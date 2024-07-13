import csv
import requests
from config import API_KEY, BASE_URL

def get_location_data(city=None, state=None, zipcode=None):
    """
    Fetch location data using city, state, and zipcode. Falls back to zipcode only if city and state are not provided.
    """
    params = {}
    if city:
        params['city'] = city
    if state:
        params['state'] = state
    if zipcode:
        params['zipcode'] = zipcode

    url = f"{BASE_URL}/cst/locations/name-match"
    headers = {
        'Authorization': 'Bearer ' + API_KEY
    }
    print(params)
    response = requests.get(url, headers=headers, params=params)
    return response.json()


def process_csv(input_file, output_file):
    """
    Process the input CSV file to fetch location data and write the enhanced data to the output CSV file.
    """
    with open(input_file, 'r', encoding='utf-8', errors='replace') as infile, open(output_file, 'w', newline='',
                                                                                   encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ['location_id', 'api_city', 'api_state', 'api_zipcode']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            city, state, zipcode = row['City'], row['State'], row['Zipcode']
            api_response = get_location_data(city=city, state=state, zipcode=zipcode)
            locations = api_response['data']['locations']
            print(zipcode)

            if not locations:
                api_response = get_location_data(zipcode=zipcode)
                locations = api_response['data']['locations']

            if locations:
                location = locations[0]
                row.update({
                    'location_id': location['location_id'],
                    'api_city': location['city'],
                    'api_state': location['state'],
                    'api_zipcode': location['zipcode']
                })
                writer.writerow(row)
            else:
                print(f"No location found for {city}, {state} {zipcode}")


if __name__ == "__main__":
    input_file = "../data/inputs/exampleinput.csv"
    output_file = "../data/outputs/NAMEMATCH_exampleoutput.csv"
    process_csv(input_file, output_file)
