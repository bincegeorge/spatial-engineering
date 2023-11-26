import os
import csv
import pyproj
import json
import urllib3
import requests

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
def add_landcover_to_csv(input_csv, output_csv, batch_size=1000):
    with open(input_csv, 'r') as infile, open(output_csv, 'w', newline='') as outfile:
        csv_reader = csv.reader(infile)
        csv_writer = csv.writer(outfile)

        # Write the header to the output CSV file
        header = next(csv_reader)
        csv_writer.writerow(header + ['landcover'])

        # Process the CSV file in batches
        for batch_num, batch_rows in enumerate(zip(*[csv_reader] * batch_size)):
            print(f"Processing batch {batch_num + 1}")
            for row in batch_rows:
                lon, lat = float(row[3]), float(row[4])
                landcover = get_landcover_strata(lon, lat)
                row.append(landcover)
                csv_writer.writerow(row)


# Function to get landcover strata for a given coordinate
def get_landcover_strata(lon, lat):
    try:
        url = 'https://gip.itc.utwente.nl/services/lulc/lulc.py?'
        params = {'request': 'getValues', 'coords': f'{lon},{lat}'}

        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            landcover_values = data.get('data', [])

            # Assuming you want the first value from the list
            if landcover_values:
                return landcover_values[0]
            else:
                print(f"No landcover values found in the response")
                return None
        else:
            print(f"Request failed with status code {response.status_code}: {response.text}")
            return None
    except (KeyError, IndexError):
        print(f"Invalid response format: {response.text}")
        return None



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
    output_csv_path = 'output_with_landcover_2.csv'
    add_landcover_to_csv(input_csv_path, output_csv_path, batch_size=1000)


if __name__ == '__main__':
    main()