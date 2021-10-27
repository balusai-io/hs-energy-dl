import pandas as pd
from app.utils.logging_init import init_logger

logger = init_logger()

units_table = pd.read_csv(r'C:\Users\DHEERAJ\PycharmProjects\hs-energy-dl\tbl_unit.csv', header=0, sep=',')
product_table = pd.read_csv(r'C:\Users\DHEERAJ\PycharmProjects\hs-energy-dl\energy_product.csv', header=0, sep=',')


def file_processing(file_path, units_table, product_table):
    try:
        read_file = pd.read_csv(file_path)
        read_file.columns = ['well_name', 'month', 'crude_oil(m3)', 'natural_gas(km3)', 'other(m3)']
        # un pivotizing dataframe to change rows to column i.e. change shape of dataframe by specification
        structured_df = pd.melt(read_file, id_vars=["well_name", "month"], var_name="Commodity")
        # adding columns- 'energy' and 'units' to dataframe.
        structured_df[['energy', 'units']] = structured_df['Commodity'].str.split('(', expand=True)
        structured_df['units'] = structured_df['units'].str.replace('[)]', '', regex=True)
        # calling Id's corresponding to attributes in unit and product id columns
        structured_df['energy_product_id'] = structured_df.energy.str.lower().map(product_table.set_index('energy_product')['ID'])
        structured_df['energy_unit_id'] = structured_df.units.str.lower().map(units_table.set_index('uom')['unit_of_measure_id'])
        structured_df = structured_df.drop(['energy', 'units', 'Commodity'], axis=1)
        print(structured_df)
        return structured_df
    except Exception as e:
        logger.error(e)


file_processing(r'C:\Users\DHEERAJ\PycharmProjects\hs-energy-dl\data\csv_file.csv', units_table, product_table)
