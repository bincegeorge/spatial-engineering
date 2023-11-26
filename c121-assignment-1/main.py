import os
import csv
import pyproj
import json
import requests
import concurrent.futures
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime, timedelta

wgs84 = pyproj.CRS("EPSG:4326")
utm29n = pyproj.CRS("EPSG:32629")

transformer = pyproj.Transformer.from_crs(wgs84, utm29n, always_xy=True)

def check_csv_file_path(file_path):
    """
    check_csv_file_path function is  defined with file_path as parameter.
    if the path exists it gets printed else raises a error message.
    f-string is used to add path within the string itself.
    :param file_path: file_path is the path of the csv file to be used.
    :return:
    """
    if os.path.exists(file_path):
        print(f"The file was found at: {os.path.abspath(file_path)}")
    else:
        raise FileNotFoundError(f"The file '{file_path}' was not found.")


def truncate_to_four_digits(input_csv, output_csv):
    """
    - truncate_to_four_digits is defined with two parameters, input and output csv.
    - opening input csv in 'r' mode and output csv in 'w' mode
    - csv reader and writer is created.
    - A for-loop is created to extract first four charachters in coolumn 15 an writes it to new csv file
    - Input and output csv files are assigned and call the function
    :param input_csv:
    :param output_csv:
    :return:
    """
    with open(input_csv, 'r', newline='') as infile, open(output_csv, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        for row in reader:

            if row:
                truncated_number = row[14][:4]
                row[14] = truncated_number
                writer.writerow(row)


def generate_summary(input_csv):
    with open(input_csv, 'r', newline='') as infile:
        reader = csv.reader(infile)
        header = next(reader)
        bat_id_column_index = header.index('individual-taxon-canonical-name')
        data_points_counts = {}
        for row in reader:
            bat_id = row[14]
            if bat_id in data_points_counts:
                data_points_counts[bat_id] += 1
            else:
                data_points_counts[bat_id] = 1
    summaries = [f"There are {count} data points for bat_id {bat_id}." for bat_id, count in data_points_counts.items()]
    return summaries


def reproject_coordinates(input_csv):
    with open(input_csv, 'r', newline='') as infile:
        reader = csv.reader(infile)
        header = next(reader)

        event_id_index = header.index('event-id')
        lon_index = header.index('location-long')
        lat_index = header.index('location-lat')

        reprojected_coordinates = []

        for row in reader:
            event_id = row[event_id_index]
            longitude, latitude = float(row[lon_index]), float(row[lat_index])

            easting, northing = transformer.transform(longitude, latitude)

            reprojected_coordinates.append({
                'Event ID': event_id,
                'Original Coordinates (WGS 84)': {'Longitude': longitude, 'Latitude': latitude},
                'Reprojected Coordinates (UTM zone 29N)': {'Easting': easting, 'Northing': northing}
            })
    return reprojected_coordinates


# Function to add landcover strata to each row in the CSV file
def add_landcover_to_bat_data(bats_data):
    with concurrent.futures.ThreadPoolExecutor(max_workers=200) as executor:
        td = {executor.submit(get_landcover_strata, bat_data): bat_data for bat_data in bats_data}

        print('Threads executing : ', td)

    for th in concurrent.futures.as_completed(td):
        res = td[th]
        print('Response:', res)


# Function to get landcover strata for a given coordinate
def get_landcover_strata(bat_data):
    try:
        if bat_data[0] == "event-id":
            bat_data.append("landcover")
        else:
            lon = bat_data[3]
            lat = bat_data[4]
            url = 'https://gip.itc.utwente.nl/services/lulc/lulc.py?'
            params = {'request': 'getValues', 'coords': f'{lon},{lat}'}

            response = requests.get(url, params=params)

            if response.status_code == 200:
                data = response.json()
                landcover_values = data.get('data', [])

                # Assuming you want the first value from the list
                if landcover_values:
                    bat_data.append(landcover_values[0])
                else:
                    print(f"No landcover values found in the response")
                    return None
            else:
                print(f"Request failed with status code {response.status_code}: {response.text}")
                return None
    except (KeyError, IndexError):
        print(f"Invalid response format: {response.text}")
        return None


def list_to_csv(data_list, csv_file_path):
    with open(csv_file_path, 'w', newline='') as file:
        csv_writer = csv.writer(file)
        csv_writer.writerows(data_list)


# Function to get landcover class for a given landcover value
def get_landcover_class(landcover_value, landcover_legend):
    return landcover_legend.get(str(landcover_value), "Unknown")


def print_event_details(batch, landcover_legend, bar_chart_dict):
    # Process each row in the CSV batch
    for row in batch:
        event_id = row['event-id']
        lon = row['location-long']
        lat = row['location-lat']
        landcover_value = row['landcover']

        print(f"Event ID: {event_id}")
        print(f"Longitude: {lon}")
        print(f"Latitude: {lat}")
        print(f"Land cover value: {landcover_value}")

        # Get landcover class based on the value
        landcover_class = get_landcover_class(landcover_value, landcover_legend)

        if landcover_class:
            print(f"Land cover class: {landcover_class}")
            print()
            bar_chart_dict[landcover_class] = bar_chart_dict[landcover_class] + 1


def read_csv(file_path):
    # Read the CSV file and parse the timestamp column
    df = pd.read_csv(file_path, parse_dates=['timestamp'])
    return df


def calculate_time_spans(df):
    # Sort the DataFrame by timestamp
    df = df.sort_values(by=['indi', 'timestamp'])

    # Calculate time spans between consecutive records for each bat
    df['time_span'] = df.groupby('indi')['timestamp'].diff().dt.total_seconds()

    return df


def calculate_statistics(df):
    # Calculate minimum, average, and maximum time span for each bat
    stats = df.groupby('indi')['time_span'].agg(['min', 'mean', 'max'])

    return stats


def print_statistics(stats):
    # Print the results in a human-friendly format
    for bat, values in stats.iterrows():
        print(f"Bat {bat}:")
        print(f"  Minimum time span: {values['min']} seconds")
        print(f"  Average time span: {values['mean']} seconds")
        print(f"  Maximum time span: {values['max']} seconds")
        print()


def identify_temporal_gaps(df, threshold_hours=1):
    # Identify temporal gaps based on the threshold
    gaps = df[df['time_span'] > threshold_hours * 3600]

    return gaps

def print_temporal_gaps(gaps):
    # Print information about temporal gaps
    for index, row in gaps.iterrows():
        start_time = row['timestamp']
        end_time = start_time + timedelta(seconds=row['time_span'])
        duration_hours = row['time_span'] / 3600

        print(f"Bat {row['indi']}:")
        print(f"  Start timestamp: {start_time}")
        print(f"  End timestamp: {end_time}")
        print(f"  Duration: {duration_hours:.2f} hours")
        print()


def main():
    # task 1.1
    # Using CSV reader to read the file and print it.
    f = open('/Users/i322910/PycharmProjects/bincegoerge/spatial-engineering/c121-assignment-1/input/3d_flights_of_European_free_tailed_bats.csv')
    reader = csv.reader(f)
    for line in reader:
        print(line)
    f.close()

    file_path = "/Users/i322910/PycharmProjects/bincegoerge/spatial-engineering/c121-assignment-1/input/3d_flights_of_European_free_tailed_bats.csv"
    check_csv_file_path(file_path)

    # task 1.2
    bats_data = []
    with open(file_path, 'r') as file:
        reader = csv.reader(file)

        for row in reader:
            bats_data.append(row)

    for row in bats_data:
        print(row)

    input_file = "/Users/i322910/PycharmProjects/bincegoerge/spatial-engineering/c121-assignment-1/input/3d_flights_of_European_free_tailed_bats.csv"
    output_file = 'output.csv'
    truncate_to_four_digits(input_file, output_file)

    # printing the new csv file with short bat id.
    file = open('output.csv')
    reader = csv.reader(file)
    for line in reader:
        print(line)
    file.close()

    # task 1.3
    input_file = 'output.csv'
    summaries = generate_summary(input_file)
    for summary in summaries:
        print(summary)

    # task 2.1
    reprojected_coords = reproject_coordinates(input_file)
    for coords in reprojected_coords:
        print(f"Event ID: {coords['Event ID']}")
        print(
            f"Original Coordinates (WGS 84): Longitude = {coords['Original Coordinates (WGS 84)']['Longitude']}, Latitude = {coords['Original Coordinates (WGS 84)']['Latitude']}")
        print(
            f"Reprojected Coordinates (UTM zone 29N): Easting = {coords['Reprojected Coordinates (UTM zone 29N)']['Easting']}, Northing = {coords['Reprojected Coordinates (UTM zone 29N)']['Northing']}")
        print()

    # task 2.2
    # get legend classes
    base_url = "https://gip.itc.utwente.nl/services/lulc/lulc.py?"
    # http = urllib3.PoolManager()
    url = base_url + "request=getLegend"
    print(url)
    # response = http.request("GET", url)
    response = requests.get(url)
    data = json.loads(response.text)  # Converts the JSON response into a dictionary
    classes = data["data"]  # Extracts value associated to the key "data"
    print("Legend", classes)

    input_csv_path = 'output.csv'
    output_csv_path = 'output_with_landcover.csv'

    bats_data_new = []
    with open(input_csv_path, 'r') as file:
        reader = csv.reader(file)

        for row in reader:
            bats_data_new.append(row)

    add_landcover_to_bat_data(bats_data_new)
    # generate final csv
    list_to_csv(bats_data_new, output_csv_path)

    batch_size = 1000
    # initialize dict for barchart data
    bar_chart_dict = {
        "True desert": 0,
        "Semi-arid": 0,
        "Dense short vegetation": 0,
        "Open tree cover": 0,
        "Dense tree cover": 0,
        "Tree cover gain": 0,
        "Tree cover loss, not fire": 0,
        "Salt pan": 0,
        "Wetland sparse vegetation": 0,
        "Wetland dense short vegetation": 0,
        "Wetland open tree cover": 0,
        "Wetland dense tree cover": 0,
        "Wetland tree cover gain": 0,
        "Wetland tree cover loss, not fire": 0,
        "Ice": 0,
        "Water": 0,
        "Cropland": 0,
        "Built-up": 0,
        "Ocean": 0,
        "No data": 0
    }
    with open(output_csv_path, 'r') as csvfile:
        csv_reader = list(csv.DictReader(csvfile))
        landcover_legend = classes

        # Process the CSV file in batches
        for batch_num in range(0, len(csv_reader), batch_size):
            batch = csv_reader[batch_num:batch_num + batch_size]
            print_event_details(batch, landcover_legend, bar_chart_dict)

    # task 2.3
    print(bar_chart_dict)
    # Extract keys and values from the dictionary
    land_cover_classes = list(bar_chart_dict.keys())
    number_of_records = list(bar_chart_dict.values())
    plt.bar(land_cover_classes, number_of_records)
    plt.xticks(rotation=90)
    plt.show()

    # task 3.1
    # Step 1: Read the CSV file
    df = read_csv(output_csv_path)

    # Step 2: Calculate time spans between consecutive records
    df = calculate_time_spans(df)

    # Step 3: Calculate statistics for each bat
    stats = calculate_statistics(df)

    # Step 4: Print the results
    print_statistics(stats)

    # task 3.3
    # Identify temporal gaps greater than 1 hour
    threshold_hours = 1
    gaps = identify_temporal_gaps(df, threshold_hours)

    # Print information about temporal gaps
    print_temporal_gaps(gaps)


if __name__ == '__main__':
    main()