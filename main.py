# Author: Kevin Yuen
# Github username: kevin-yuen
# Date: 8/16/2024
# Description: Convert csv files to json format and json files to csv format
import json
import os
import pandas as pd
import shutil

schemas_columns = {}

def load_schemas_file(env_value):
    """
    Attempt to load the schemas.json file
    :param env_value: the value of the variable
    :return: content of the loaded json file
    """
    current_filepath = f'data/{env_value}/schemas.json'

    try:
        return json.load(open(current_filepath))
    except FileNotFoundError:
        # if file not found, search another directory
        new_env_value = 'retail_db_json' if env_value == 'retail_db' else 'retail_db'
        other_filepath = f'data/{new_env_value}/schemas.json'

        # if file found in another directory, copy the schemas.json file to the current directory
        schemas_content = json.load(open(other_filepath))

        if len(schemas_content) > 0:
            print('Replicating the schema...')
            shutil.copy(other_filepath, current_filepath)   # copy the file to the current directory
            return json.load(open(current_filepath))       # and open and load the copied file

def generate_schema_metadata(schemas_content):
    """
    Generate each schema's metadata
    :param schemas_content: schemas.json file
    :return: schema name with a list of columns in dictionary format
    """
    for schema in schemas_content:
        for metadata in schemas_content[schema]:
            if schema not in schemas_columns:
                schemas_columns[schema] = [metadata['column_name']]
            else:
                schemas_columns[schema].append(metadata['column_name'])

    return schemas_columns

def create_new_schema_metadata(schema_names, column_names):
    """
    Create and return a dictionary that includes the schema and columns
    :param schema_names: list of schema names
    :param column_names: list of column names
    :return: a dictionary that includes the schema and columns
    """
    print('Creating the schema...')

    for schema_index, schema in enumerate(schema_names):
        for column_index, column in enumerate(column_names):
            if schema_index == column_index:
                schemas_columns[schema] = column

    return schemas_columns

def write_json_to_file(json_data, env_variable_value):
    """
    Output schema's metadata to json file
    :param json_data: schema's metadata
    :param env_variable_value: value of the environment variable
    """
    with open(f'data/{env_variable_value}/metadata.json', 'w', encoding='utf-8') as file:
        json.dump(json_data, file, ensure_ascii=False, indent=4)

def output_all_csv_to_json(env_variable_value, metadata):
    """
    Output all csv files to json file format
    :param env_variable_value: the value of the environment variable
    :param metadata: the dictionary that includes the schema and columns
    """
    filepath = f'data/{env_variable_value}'

    for schema in metadata:
        try:
            output = pd.read_csv(f'{filepath}/{schema}/part-00000', header=None, names=metadata[schema])

            print('Converting csv to json file format...')
            output.to_json(f'{filepath}/{schema}/part-00000-json', orient='records', lines=True)
        except FileNotFoundError:
            print(f'Error: part-00000 not found in \"{schema}\" folder.')
            pass

def output_csv_to_json(env_variable_value, metadata, directory_name):
    """
    Output a particular csv file to json file format
    :param env_variable_value: the value of the environment variable
    :param metadata: the dictionary that includes the schema and columns
    :param directory_name: the directory of the csv input file is and the destination directory for the json output file
    """
    filepath = f'data/{env_variable_value}'

    try:
        # find the column names from the schema-columns dictionary
        if directory_name in metadata:
            columns = metadata[directory_name]
        else:
            print(f'Error: {directory_name} not found in the schema. Please check the schema.')
            return

        output = pd.read_csv(f'{filepath}/{directory_name}/part-00000', header=None, names=columns)

        print('Converting csv to json file format...')
        output.to_json(f'{filepath}/{directory_name}/part-00000-json', orient='records', lines=True)
    except FileNotFoundError:
        print(f'Error: part-00000 not found in \"{directory_name}\" folder.')
        return

def output_all_json_to_csv(env_variable_value, metadata):
    """
    Output all json files to csv file format
    :param env_variable_value: the value of the environment variable
    :param metadata: the dictionary that includes the schema and columns
    """
    filepath = f'data/{env_variable_value}'

    for schema in metadata:
        try:
            output = pd.read_json(f'{filepath}/{schema}/part-00000', orient='records', lines=True)

            print('Converting json to csv file format...')
            output.to_csv(f'{filepath}/{schema}/part-00000-csv', index=False)
        except FileNotFoundError:
            print(f'Error: part-00000 not found in \"{schema}\" folder.')
            pass

def output_json_to_csv(env_variable_value, directory_name):
    """
    Output a particular json file to csv file format
    :param env_variable_value: the value of the environment variable
    :param metadata: the dictionary that includes schema and columns
    :param directory_name: the directory of the json input file is and the destination directory for the CSV output file
    """
    filepath = f'data/{env_variable_value}'

    try:
        output = pd.read_json(f'{filepath}/{directory_name}/part-00000', orient='records', lines=True)

        print('Converting json to csv file format...')
        output.to_csv(f'{filepath}/{directory_name}/part-00000-csv', index=False)
    except FileNotFoundError:
        print(f'Error: part-00000 not found in \"{directory_name}\" folder')
        return

if __name__ == '__main__':
    file_conversion_opt = ''
    directory_name = ''

    while file_conversion_opt != 1 and file_conversion_opt != 2:
        try:
            file_conversion_opt = int(
                input('Select one of the below options to start file conversion:\n1. CSV to JSON\n2. JSON to CSV\n'))
        except ValueError:
            print('Please select Option 1 or Option 2')
            pass

    # Get the value of the environment variable
    env_variable_value = os.environ.get('HOST_CSV') if file_conversion_opt == 1 else os.environ.get('HOST_JSON')

    # Attempt to open and load the schemas.json file
    try:
        metadata = generate_schema_metadata(load_schemas_file(env_variable_value))
    except FileNotFoundError:
        schema_names = ['departments', 'categories', 'orders', 'products', 'customers', 'order_items']

        column_names = [
            ['department_id', 'department_name'],
            ['category_id', 'category_department_id', 'category_name'],
            ['order_id', 'order_date', 'order_customer_id', 'order_status'],
            ['product_id', 'product_cateogry_id', 'product_name', 'product_description', 'product_price',
             'product_image'],
            ['customer_id', 'customer_fname', 'customer_lname', 'customer_email', 'customer_password',
             'customer_street', 'customer_city', 'customer_state', 'customer_zipcode'],
            ['order_item_id', 'order_item_order_id', 'order_item_product_id', 'order_item_quantity',
             'order_item_subtotal', 'order_item_product_price']
        ]

        metadata = create_new_schema_metadata(schema_names, column_names)

    write_json_to_file(metadata, env_variable_value)    # write metadata (in json format) to file

    # file conversion process starts here
    while directory_name == '':
        directory_name = input(f'Enter a folder name or \"All\" to start file conversion\n').lower()

        if directory_name != 'all' and directory_name not in metadata:
            print(f'Error: \"{directory_name} not found. Please try again.')
            directory_name = ''

    if file_conversion_opt == 1:    # CSV to JSON
        if directory_name == 'all':
            output_all_csv_to_json(env_variable_value, metadata)
        else:
            output_csv_to_json(env_variable_value, metadata, directory_name)
    else:                           # JSON to CSV
        if directory_name == 'all':
            output_all_json_to_csv(env_variable_value, metadata)
        else:
            output_json_to_csv(env_variable_value, directory_name)



