import os
from dotenv import load_dotenv
import camelot
import requests
import pandas as pd
import datetime as dt
from datetime import date as dt
from bs4 import BeautifulSoup
import json
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from app.utils.logging_init import init_logger
load_dotenv()

logger = init_logger()

folder_path = os.getenv('BASE_DATA_PATH')
folder_filter = json.loads(os.getenv('FIELDS'))
parent_path = os.getenv('BASE_DATA_PATH')
answer = os.getenv('ALL_YEARS')
units_table = pd.read_csv('C:/Users/DHEERAJ/PycharmProjects/hs-energy-dl/tbl_unit.csv', header=0, sep=',')
product_table = pd.read_csv('C:/Users/DHEERAJ/PycharmProjects/hs-energy-dl/energy_product.csv', header=0, sep=',')
fields_table = pd.read_csv('C:/Users/DHEERAJ/PycharmProjects/hs-energy-dl/tbl_cnlopb_fields.csv', header=0, sep=',')
wells_table = pd.read_csv('C:/Users/DHEERAJ/PycharmProjects/hs-energy-dl/tbl_cnlopb_wells.csv', header=0, sep=',')

def pdffile_download(parent_path, folder_filter, answer):
    url = "https://www.cnlopb.ca/information/statistics/#rm"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a')
    folders = list(folder_filter.keys())
    todays_date = dt.today()
    present_year = todays_date.year
    links_curr_year = soup.find_all('a', text=present_year)
    # Setting the current working directory
    os.chdir(parent_path)
    for folder in folders:
        curr_folder_path = parent_path + '//' + folder
        if not os.path.isdir(curr_folder_path):
            os.mkdir(folder)
        os.chdir(curr_folder_path)
        if answer == 'yes':
            folder_filter_curr = folder_filter[folder]
            # To display respective folder where related files going to download
            print("In Folder:", folder)
            i = 0
            try:
                for link in links:
                    if folder_filter_curr in link.get('href', []):
                        i += 1
                        # To display downloading file number
                        print("Downloading file: ", i)
                        response_page = requests.get(link.get('href'))
                        name = link.get('href').split('/')[-1]
                        pdf = open(name, 'wb')
                        pdf.write(response_page.content)
                        pdf.close()
                        # To display specific file is downloaded
                        print("File ", i, " downloaded")
                os.chdir(parent_path)
            except Exception as e:
                logger.error(e)
        else:
            try:
                folder_filter_curr = folder_filter[folder]
                print("In Folder:", folder)
                i = 0
                for link in links_curr_year:
                    if folder_filter_curr in link.get('href', []):
                        i += 1
                        # To display downloading file number
                        print("Downloading file: ", i)
                        response_page = requests.get(link.get('href'))
                        name = link.get('href').split('/')[-1]
                        pdf = open(name, 'wb')
                        pdf.write(response_page.content)
                        pdf.close()
                        # To display specific file is downloaded
                        print("File ", i, " downloaded")
                os.chdir(parent_path)
            except Exception as e:
                logger.error(e)

pdffile_download(parent_path, folder_filter, answer)
# To show all files are downloaded.
print("All PDF files downloaded")


def pre_cleaning(new_tables):
    if len(new_tables.columns) == 7:
        try:
            new_tables.set_index('Total').filter(like='Yearly', axis=0)
            new_tables = new_tables.drop('Total', axis=1)
        except Exception as e:
            logger.error(e)
    else:
        try:
            new_tables = new_tables[~new_tables['Month'].str.contains("Yearly")]
        except Exception as e:
            logger.error(e)
    return new_tables


def final_cleaning(sample_df):
    sample_df = sample_df.replace('', np.NaN)
    try:
        for columns in ['Well Name', 'Year', 'Month']:
            sample_df[columns] = sample_df[columns].fillna(method='ffill')
            dropped_null = sample_df.dropna()
        return dropped_null
    except Exception as e:
        logger.error(e)


def extraction(folder_path):
    final_data = pd.DataFrame()
    for path, dirs, files in os.walk(folder_path):
        for file in files:
            filename = os.path.join(path, file)
            tables = camelot.read_pdf(filename, pages='all', flavor='stream', edge_tol=1000)
            table_number = tables.n
            all_tables = pd.DataFrame()

            for table in range(table_number):
                temp_df = tables[table].df
                column_len = len(temp_df.columns)
                if column_len == 6:
                    try:
                        temp_df.columns = ['Well Name', 'Year', 'Month', 'Oil_(m³)', 'Gas_(10³m³)', 'Water_(m³)']
                        all_tables = pd.concat([all_tables, temp_df])
                        all_tables = all_tables[~all_tables['Well Name'].str.contains('Well Name')]
                    except Exception as e:
                        logger.error(e)
                elif column_len == 5:
                    try:
                        temp_df['Well Name'] = np.NAN
                        temp_df.columns = ['Well Name', 'Year', 'Month', 'Oil_(m³)', 'Gas_(10³m³)', 'Water_(m³)']
                        all_tables = pd.concat([all_tables, temp_df])
                        all_tables = all_tables[~all_tables['Well Name'].str.contains('Well Name')]
                    except Exception as e:
                        logger.error(e)
                else:
                    try:
                        temp_df.columns = ['Well Name', 'Year', 'Month', 'Total', 'Oil_(m³)', 'Gas_(10³m³)', 'Water_(m³)']
                        all_tables = pd.concat([all_tables, temp_df])
                        all_tables = all_tables[~all_tables['Well Name'].str.contains('Well Name')]
                    except Exception as e:
                        logger.error(e)
            pre_cleaning_data = pre_cleaning(all_tables)
            final_cleaning_data = final_cleaning(pre_cleaning_data)
            final_cleaning_data = final_cleaning_data[~final_cleaning_data['Month'].str.contains("Yearly")]
            final_cleaning_data['Month'] = pd.to_datetime(final_cleaning_data[['Month', 'Year']].assign(DAY=1))
            final_cleaning_data = final_cleaning_data.drop(columns='Year', axis=1)
            cleaning_data = final_cleaning_data
            print(cleaning_data)
            final_data = pd.concat([final_data, cleaning_data])
    return final_data.to_csv('C:/Users/DHEERAJ/PycharmProjects/hs-energy-dl/data/csv_file.csv', header=True, index=False)


extraction(folder_path)


def file_processing(file_path, units_table, product_table):
    try:
        read_file = pd.read_csv(file_path)
        read_file.columns = ['well_name', 'month', 'crude_oil(m3)', 'natural_gas(km3)', 'other(m3)']
        # # un pivotizing dataframe to change rows to column i.e. change shape of dataframe by specification
        structured_df = pd.melt(read_file, id_vars=["well_name", "month"], var_name="Commodity")
        # # adding columns- 'energy' and 'units' to dataframe.
        structured_df[['energy', 'units']] = structured_df['Commodity'].str.split('(', expand=True)
        structured_df['units'] = structured_df['units'].str.replace('[)]', '', regex=True)
        # calling Id's corresponding to attributes in unit and product id columns.
        structured_df['energy_product_id'] = structured_df.energy.str.lower().map(product_table.set_index('energy_product')['ID'])
        structured_df['energy_unit_id'] = structured_df.units.str.lower().map(units_table.set_index('uom')['unit_of_measure_id'])
        # structured_df['well_id'] = structured_df.well_name.map(wells_table.set_index('well_name')['ID'].astype('string'))
        # structured_df['date_created'] = structured_df.well_name.map(wells_table.set_index('well_name')['date_created'])
        structured_df = structured_df.drop(['energy', 'units', 'Commodity'], axis=1)
        print(structured_df)
        structured_df.to_csv('C:/Users/DHEERAJ/PycharmProjects/hs-energy-dl/output_file.csv', index=False)
        return structured_df
    except Exception as e:
        logger.error(e)


file_processing('C:/Users/DHEERAJ/PycharmProjects/hs-energy-dl/csv_file.csv', units_table, product_table)
