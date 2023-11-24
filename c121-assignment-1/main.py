import os
import csv

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


if __name__ == '__main__':
    main()