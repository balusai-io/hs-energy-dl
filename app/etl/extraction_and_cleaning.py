import os
import camelot
import pandas as pd
import numpy as np
from app.utils.logging_init import init_logger

logger = init_logger()

def pre_cleaning(new_tables):
    if len(new_tables.columns) == 7:
        # print(new_tables[~new_tables['Total'].str.contains("Yearly")])
        new_tables.set_index('Total').filter(like='Yearly', axis=0)
        new_tables = new_tables.drop('Total', axis=1)
    else:
        new_tables = new_tables[~new_tables['Month'].str.contains("Yearly")]
    return new_tables


def final_cleaning(sample_df):
    sample_df = sample_df.replace('', np.NaN)
    for columns in ['Well Name', 'Year', 'Month']:
        sample_df[columns] = sample_df[columns].fillna(method='ffill')
        dropped_null = sample_df.dropna()
    return dropped_null


def extraction(path):
    for path, dirs, files in os.walk(path):
        for file in files:
            # logger.info(file)
            filename = os.path.join(path, file)
            tables = camelot.read_pdf(filename, pages='all', flavor='stream', edge_tol=1000)
            table_number = tables.n
            # logger.info(table_number)
            all_tables = pd.DataFrame()
            for table in range(table_number):
                temp_df = tables[table].df
                column_len = len(temp_df.columns)
                if column_len == 6:
                    temp_df.columns = ['Well Name', 'Year', 'Month', 'Oil_(m³)', 'Gas_(10³m³)', 'Water_(m³)']
                    all_tables = pd.concat([all_tables, temp_df])
                elif column_len == 5:
                    temp_df['Well Name'] = np.NAN
                    temp_df.columns = ['Well Name', 'Year', 'Month', 'Oil_(m³)', 'Gas_(10³m³)', 'Water_(m³)']
                    all_tables = pd.concat([all_tables, temp_df])
                else:
                    temp_df.columns = ['Well Name', 'Year', 'Month', 'Total', 'Oil_(m³)', 'Gas_(10³m³)', 'Water_(m³)']
                all_tables = pd.concat([all_tables, temp_df])
                all_tables = all_tables[~all_tables['Well Name'].str.contains('Well Name')]
                pre_cleaning_data = pre_cleaning(all_tables)

                final_cleaning_data = final_cleaning(pre_cleaning_data)
                print(final_cleaning_data)
                # final_cleaning_data.to_csv(r'C:\Users\DHEERAJ\PycharmProjects\hs-energy-dl\data\csv_file.csv', header=True)

    return final_cleaning_data


extraction(r'C:\Users\DHEERAJ\PycharmProjects\hs-energy-dl\Hibernia')
