import pandas as pd
from app.utils.logging_init import init_logger
from app.etl import lookup_tables

logger = init_logger()


def file_processing(clean_df):
    try:
        units_table = lookup_tables.get_unitof_measure()
        energy_product_table = lookup_tables.get_energy_units()
        well_lookup_df = lookup_tables.get_all_wellids()
        clean_df.columns = ['well_name', 'month', 'crude_oil(m3)', 'natural_gas(km3)', 'other(m3)']
        # un pivotizing dataframe to change rows to column i.e. change shape of dataframe by specification
        structured_df = pd.melt(clean_df, id_vars=["well_name", "month"], var_name="Commodity")
        # # adding columns- 'energy' and 'units' to dataframe.
        structured_df[['energy', 'units']] = structured_df['Commodity'].str.split('(', expand=True)
        structured_df['units'] = structured_df['units'].str.replace('[)]', '', regex=True)
        # calling Id's corresponding to attributes in unit and product id columns.
        structured_df['energy_product_id'] = structured_df.energy.str.lower().map(energy_product_table.set_index('energy_product')['id'])
        structured_df['unit_of_measure_id'] = structured_df.units.str.lower().map(units_table.set_index('uom')['unit_of_measure_id'])
        structured_df['well_id'] = structured_df.well_name.map(well_lookup_df.set_index('well_name')['id'])
        structured_df['month'] = structured_df['month'].dt.strftime('%Y-%m-%d')
        structured_df['value'] = structured_df['value'].str.replace(',', '').astype(float)
        # Map well_name with well_id and remove well_name column
        structured_df = structured_df.drop(['energy', 'units', 'Commodity', 'well_name'], axis=1)
        return structured_df
    except Exception as e:
        logger.error(e)
