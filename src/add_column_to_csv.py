from csv import writer
from csv import reader
from csv import DictReader
from csv import DictWriter


def add_column_in_csv(input_file, output_file, transform_row):
    """Append a column in existing csv using csv.reader / csv.writer classes"""
    # Open the input_file in read mode and output_file in write mode
    with open(input_file, "r") as read_obj, open(
        output_file, "w", newline=""
    ) as write_obj:
        # Create a csv.reader object from the input file object
        csv_reader = reader(read_obj)
        # Create a csv.writer object from the output file object
        csv_writer = writer(write_obj)
        # Read each row of the input csv file as list
        for row in csv_reader:
            # Pass the list / row in the transform function to add column text for this row
            transform_row(row, csv_reader.line_num)
            # Write the updated row / list to the output file
            csv_writer.writerow(row)


def add_column_in_csv_2(input_file, output_file, transform_row, tansform_column_names):
    """Append a column in existing csv using csv.reader / csv.writer classes"""
    # Open the input_file in read mode and output_file in write mode
    with open(input_file, "r") as read_obj, open(
        output_file, "w", newline=""
    ) as write_obj:
        # Create a DictReader object from the input file object
        dict_reader = DictReader(read_obj)
        # Get a list of column names from the csv
        field_names = dict_reader.fieldnames
        # Call the callback function to modify column name list
        tansform_column_names(field_names)
        # Create a DictWriter object from the output file object by passing column / field names
        dict_writer = DictWriter(write_obj, field_names)
        # Write the column names in output csv file
        dict_writer.writeheader()
        # Read each row of the input csv file as dictionary
        for row in dict_reader:
            # Modify the dictionary / row by passing it to the transform function (the callback)
            transform_row(row, dict_reader.line_num)
            # Write the updated dictionary or row to the output file
            dict_writer.writerow(row)


def main():
    # print('Add a column with same values to an existing csv file')

    # default_text = 'Some Text'

    # # Open the input_file in read mode and output_file in write mode
    # with open('input.csv', 'r') as read_obj, \
    #         open('output_1.csv', 'w', newline='') as write_obj:
    #     # Create a csv.reader object from the input file object
    #     csv_reader = reader(read_obj)

    #     # Create a csv.writer object from the output file object
    #     csv_writer = writer(write_obj)

    #     # Read each row of the input csv file as list
    #     for row in csv_reader:
    #         # Append the default text in the row / list
    #         row.append(default_text)
    #         # Add the updated row / list to the output file
    #         csv_writer.writerow(row)

    # print('Add a column with same values to an existing csv file using generic function & a lambda')
    # default_text = 'Some Text'
    # # Add column with same text in all rows
    # add_column_in_csv('input.csv', 'output_2.csv', lambda row, line_num: row.append(default_text))

    # print('Add a column to an existing csv file, based on values from other column ')
    # # Add column to csv by merging contents from first & second column of csv
    # add_column_in_csv('input.csv', 'output_3.csv', lambda row, line_num: row.append(row[0] + '__' + row[1]))

    # print('Add a list as a column to an existing csv file')
    # list_of_str = ['First', 'Second', 'Third', 'Fourth', 'Fifth']
    # # Add a list as column
    # add_column_in_csv('input.csv', 'output_4.csv', lambda row, line_num: row.append(list_of_str[line_num - 1]))

    # print('Insert a column as second column with same values into an existing csv')
    # # Insert a column in between other columns of the csv file i.e. the second column of csv
    # add_column_in_csv('input.csv', 'output_5.csv', lambda row, line_num: row.insert(1, row[0] + '__' + row[1]))

    print("Add a column with same values to an existing csv file with header")
    header_of_new_col = "OEDVersion"
    default_text = "2.0.0"
    # Add the column in csv file with header
    add_column_in_csv(
        "OED_test2_100k_loc_nrmc.csv",
        "OED2_test2_100k_loc_nrmc.csv",
        lambda row, line_num: row.append(header_of_new_col)
        if line_num == 1
        else row.append(default_text),
    )

    # print('Use DictReader DictWriter to add a column with same values to an existing csv')
    # header_of_new_col = 'Address'
    # default_text = 'Some_Text'
    # # Add a Dictionary as a column in the existing csv file using DictWriter class
    # add_column_in_csv_2('input_with_header.csv', 'output_7.csv',
    #                     lambda row, line_num: row.update({header_of_new_col: default_text}),
    #                     lambda field_names: field_names.append(header_of_new_col))
    # print('Use DictReader DictWriter to insert a column as second column in a csv')
    # header_of_new_col = 'Address'
    # default_text = 'Some_Text'
    # # Insert a Dictionary as the column in between other columns of an existing csv file (Insert as 2nd column)
    # add_column_in_csv_2('input_with_header.csv', 'output_8.csv',
    #                     lambda row, line_num: row.update({header_of_new_col: default_text}),
    #                     lambda field_names: field_names.insert(1, header_of_new_col))


if __name__ == "__main__":
    main()
