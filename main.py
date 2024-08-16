# Author: Kevin Yuen
# Github username: kevin-yuen
# Date: 8/16/2024
# Description: Convert csv files to json format and json files to csv format
import json
import glob
import os
import pandas as pd

# file path for getting source files and outputting json files
fpath = 'data-engineering-essentials/035 Python Essentials for Data Engineers/05 Project 1 - File Format Converter/data/retail_db/'

column_names = {} # {'departments': ['department_id', 'department_name']}
schemas = json.load(open(f'{fpath}/schemas.json'))  # get schemas

for schema_name in schemas:
    metadata_list = sorted(schemas[schema_name], key=lambda metadata: metadata['column_position'])

    # get column names for each schema
    for metadata in metadata_list:
        if schema_name not in column_names:
            column_names[schema_name] = [metadata['column_name']]
        else:
            column_names[schema_name].append(metadata['column_name'])

# get all source csv files
source_file_directories = glob.glob(f'{fpath}*/*')
print(source_file_directories)

for source_file_directory in source_file_directories:
    for col_names_identifier in column_names:
        if col_names_identifier == source_file_directory.split('/')[-2]:    # match source file's folder with the key in col_names dict
            output_directory = f'{fpath}/{col_names_identifier}'

            df = pd.read_csv(source_file_directory, header=None, names=column_names[col_names_identifier])

            print(df.shape)

            # check if the json file exists
            # if os.path.exists(f'{output_directory}/{col_names_identifier}-json'):
            #     print(f'{col_names_identifier}-json exists!!!')
            #     os.remove(f'{col_names_identifier}-json')

            # write dataframe to json file and store in the destination folder
            print('writing to json format...')
            df.to_json(f'{fpath}/{col_names_identifier}/{col_names_identifier}-json', orient='records', lines=True)


