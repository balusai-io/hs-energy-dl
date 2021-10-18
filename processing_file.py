import pandas as pd


def pandas_processing():
    read_file = pd.read_csv('heb_oil.csv')
    # dropping column and rows having null values
    filenull_drop_col = read_file.drop('Unnamed: 5', axis=1)
    filenull_drop_row = filenull_drop_col.dropna(how='any', axis=0)
    # un pivotizing dataframe to change rows to column i.e. change shape of dataframe
    unpivot_df = pd.melt(filenull_drop_row, id_vars=["well_name", "month"], var_name="commodity")
    structured_df = unpivot_df.sort_values(by=["well_name", "month"])
    # converting unit 103m3 to km3
    structured_df['commodity'] = [i.replace('103', 'k') for i in structured_df['commodity']]
    # creating columns- 'energy' and 'units' from column 'commodity'
    structured_df['energy'] = [fuel.split(' ')[0] for fuel in structured_df['commodity']]
    structured_df['units'] = [metric.split(' ')[1].replace('(', '').replace(')', '') for metric in structured_df['commodity']]
    return print(structured_df)


pandas_processing()
