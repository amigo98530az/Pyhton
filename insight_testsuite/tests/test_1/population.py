import csv
import os
import re

absolute_path = os.path.dirname(os.path.abspath(__file__))
source_file = absolute_path + '\input\censustract-00-10.csv'
report_file = absolute_path + '\output\\report.csv'

with open(source_file, 'r')as test_csv_file:
    csv_reader = csv.DictReader(test_csv_file)
    # for line in csv_reader:
    # print(line['CBSA09'],line['CBSA_T'],line['POP00'],line['POP10'],line['PPCHG'])

    with open('trim_columns_data.csv', 'w', newline='') as trim_columns_csv_file:
        fieldnames = ['CBSA09', 'CBSA_T', 'POP00', 'POP10', 'PPCHG']

        csv_writer = csv.DictWriter(trim_columns_csv_file, fieldnames=fieldnames, delimiter=',')

        csv_writer.writeheader()

        for line in csv_reader:
            del line['GEOID'], line['ST10'], line['COU10'], line['TRACT10'], line['AREAL10'], line['AREAW10'], line[
                'CSA09'], line['MDIV09'], line['CSI'], line['COFLG'], line['HU00'], line['HU10'], line['NPCHG'], line[
                'NHCHG'], line['PHCHG']
            csv_writer.writerow(line)

file = 'trim_columns_data.csv'
with open(file, 'r')as f1:
    read_file = csv.DictReader(f1)

    # just to read the file on screen
    # for line in read_file:
    #     print(line)

    # clean data from empty value and (x) value:
    with open('clean_trim_columns_data.csv', 'w', newline='') as trim_clean_result_csv_file:
        fieldnames = ['CBSA09', 'CBSA_T', 'POP00', 'POP10', 'PPCHG']
        csv_writer = csv.DictWriter(trim_clean_result_csv_file, fieldnames=fieldnames, delimiter=',')

        csv_writer.writeheader()

        for line in read_file:
            if line['CBSA09'] == '' or line['PPCHG'] == '(X)':
                continue
            csv_writer.writerow(line)
            # print(line)

# open 'clean_trim_columns_data.csv' to clean it and make it ready for work:

grouped_area = {}
with open('clean_trim_columns_data.csv') as open_file:
    raw_file = csv.reader(open_file)
    next(raw_file)

    # grouped by area number:
    for unit, row in enumerate(raw_file):
        CBSA09, CBSA_T, POP00, POP10, PPCHG = row
        if CBSA09 not in grouped_area:
            grouped_area[CBSA09] = list()
        my_row = (CBSA_T, POP00, POP10, PPCHG)
        grouped_area[CBSA09].append(my_row)
    # print(grouped_area)

    # replace',' with '.':
    for key, value in grouped_area.items():
        # value = [[number.replace(',', '') for number in sub_value] for sub_value in value]
        # corrected becuase its delete (,) from address also!!!
        value = [[number.replace(',', '') if not re.match('^[a-zA-Z]', number) else number for number in sub_value] for
                 sub_value in value]

        grouped_area[key] = value
    # print(grouped_area)

    # change numbers to float:
    for key, value in grouped_area.items():
        value = [[float(number) if not re.match('^[a-zA-Z]', number) else number for number in sub_value] for sub_value
                 in value]
        grouped_area[key] = value
    # print(grouped_area)
    # print('#####################')

    # sort value for (POP00, POP10, PPCHG):
    for key, value in grouped_area.items():
        value = [i for i in zip(*value)]
        grouped_area[key] = value
    # print(grouped_area)
    # print('#############')

    # sum value for (POP00, POP10, PPCHG):
    for key, value in grouped_area.items():
        num_of_repeates_area = len(value[2])
        value[0] = value[0][0]  # keep only on value for the name of the area
        value[1] = int(sum(value[1]))
        value[2] = int(sum(value[2]))
        value[3] = round((sum(value[3]) / len(value[3])), 2)
        grouped_area[key].append(num_of_repeates_area)

    # print(grouped_area)
    # print()

    # reorder the list
    for key, value in grouped_area.items():
        value[0], value[1], value[2], value[3], value[4] = value[0], value[4], value[1], value[2], value[3]


    # print(grouped_area)

    # flat data to be ready for write on csv file:
    # actually I copy the function from other source, but did not understand exactly ,how its works!
    def flatten(data_object):
        for item in data_object:
            if isinstance(item, (list, tuple, set)):
                yield from flatten(item)
            else:
                yield item


    list_grouped_area = list(grouped_area.items())
    my_list = []
    number_of_area = len(list_grouped_area)
    for xx in range(number_of_area):
        nested = list_grouped_area[xx]
        flattened = list(flatten(nested))
        my_list.append(flattened)
    # print(my_list)
    # print()

    # data now is read to save on csv:
    with open(report_file, 'w', newline='') as final_file:
        fieldnames = ['CBSA09', 'CBSA_T', 'No#', 'POP00', 'POP10', 'PPCHG']
        csv_writer = csv.writer(final_file, delimiter=',')
        csv_writer.writerow(fieldnames)

        for line in my_list:
            csv_writer.writerow(line)
        # print(my_list)

# remove unused files:
os.remove("trim_columns_data.csv")
os.remove("clean_trim_columns_data.csv")
