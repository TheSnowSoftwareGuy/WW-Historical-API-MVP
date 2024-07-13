import pandas as pd
import requests
import logging
from openpyxl import Workbook
import csv
from config import API_KEY, BASE_URL

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class PrefetchedWeatherDataProcessor:
    def __init__(self, input_file_path, events_output_file_path, sources_output_file_path):
        self.input_file_path = input_file_path
        self.events_output_file_path = events_output_file_path
        self.sources_output_file_path = sources_output_file_path
        self.data = None

    def read_input_file(self):
        try:
            self.data = pd.read_csv(self.input_file_path)
            logging.info("Input file read successfully.")
        except Exception as e:
            logging.error(f"Error reading input file: {e}")
            raise

    @staticmethod
    def fetch_historical_data(location_id, start_season=2006, end_season=2023):
        try:
            response = requests.get(f"{BASE_URL}/snowtistics/history/events", params={
                "api_key": API_KEY,
                "location_id": location_id,
                "start_season": start_season,
                "end_season": end_season
            })
            response.raise_for_status()
            data = response.json().get('data', {})
            logging.info(f"Historical data for location ID {location_id} fetched successfully.")
            return data
        except requests.RequestException as e:
            logging.error(f"Error fetching historical data for location ID {location_id}: {e}")
            return {}

    def process(self):
        self.read_input_file()
        events_results = []
        sources_results = []
        sources_by_season_results = []
        for index, row in self.data.iterrows():
            location_id = row['location_id']
            if pd.notna(location_id):
                historical_data = self.fetch_historical_data(int(location_id))
                events = historical_data.get('events', [])
                source_data = historical_data.get('sources', {})
                sources_by_season = historical_data.get('sourcesBySeason', {})
                # Collecting source data
                self.collect_source_data(source_data, location_id, sources_results)
                # Collecting sources by season data
                self.collect_sources_by_season_data(sources_by_season, location_id, sources_by_season_results)
                # Collecting event data
                self.collect_event_data(events, source_data, location_id, events_results)
            else:
                logging.warning(f"Skipping row {index} due to missing location_id")

        self.write_to_csv(events_results)
        self.write_to_excel(sources_results, sources_by_season_results)

    @staticmethod
    def collect_source_data(source_data, location_id, sources_results):
        for k, v in source_data.items():
            sources_results.append({
                'source': k,
                'CST_location_id': location_id,
                'source_location_id': v.get('location_id', -1),
                'city': v.get('city', ''),
                'state': v.get('state', ''),
                'zipcode': v.get('zipcode', '')
            })

    @staticmethod
    def collect_sources_by_season_data(sources_by_season, location_id, sources_by_season_results):
        sources_by_season_entry = {'location_id': location_id}
        sources_by_season_entry.update(sources_by_season)
        sources_by_season_results.append(sources_by_season_entry)

    @staticmethod
    def collect_event_data(events, source_data, location_id, events_results):

        for event in events:
            events_results.append({
                'location_id': location_id,
                'City': source_data.get('city', ''),
                'State': source_data.get('state', ''),
                'Zipcode': source_data.get('zipcode', ''),
                'source': event.get('source', ''),
                'start_date': event.get('start_date', ''),
                'start': event.get('start', ''),
                'end': event.get('end', ''),
                'snow_amount': event.get('snow', {}).get('amount', ''),
                'snow_amount_formatted': event.get('snow', {}).get('amount_formatted', ''),
                'freezing_rain_amount': event.get('freezing_rain', {}).get('amount', ''),
                'freezing_rain_amount_formatted': event.get('freezing_rain', {}).get('amount_formatted', '')
            })

    def write_to_excel(self, sources_results, sources_by_season_results):
        wb = Workbook()

        # Writing sources data
        sources_ws = wb.active
        sources_ws.title = "Sources"
        sources_ws.append(['source', 'cst_location_id', 'source_location_id', 'city', 'state', 'zipcode'])
        for source in sources_results:
            sources_ws.append([
                source['source'],
                source['CST_location_id'],
                source['source_location_id'],
                source['city'],
                source['state'],
                source['zipcode']
            ])

        # Writing sources by season data
        sources_by_season_ws = wb.create_sheet(title="Sources by Season")
        headers = ['location_id'] + [str(year) for year in range(2006, 2023)]
        sources_by_season_ws.append(headers)
        for source_by_season in sources_by_season_results:
            row = [source_by_season.get('location_id', '')] + [source_by_season.get(str(year), '') for year in
                                                               range(2006, 2023)]
            sources_by_season_ws.append(row)

        wb.save(self.sources_output_file_path)
        logging.info(f"Data written to Excel file: {self.sources_output_file_path}")

    def write_to_csv(self, results):
        try:
            with open(self.events_output_file_path, mode='w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=[
                    'location_id', 'City', 'State', 'Zipcode', 'source', 'start_date', 'start', 'end',
                    'snow_amount', 'snow_amount_formatted', 'freezing_rain_amount', 'freezing_rain_amount_formatted'
                ])
                writer.writeheader()
                writer.writerows(results)
            logging.info(f"Data written to CSV file: {self.events_output_file_path}")
        except Exception as e:
            logging.error(f"Error writing to CSV file: {e}")
            raise


# Example usage
processor = PrefetchedWeatherDataProcessor('../data/outputs/NAMEMATCH_example-output.csv',
                                           '../data/outputs/ExampleOutput_events.csv',
                                           '../data/outputs/ExampleOutput_sources.xlsx')
processor.process()
