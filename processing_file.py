import pandas as pd


def pandas_processing(file_path):
    read_file = pd.read_csv(file_path, header=0, delimiter=',')
    read_file_rename = read_file.rename(
        columns={'Oil (m3)': 'crude_oil (m3)', 'Gas (103m3)': 'natural_gas (km3)', 'Water (m3)': 'other (m3)',
                 'value': 'Value'}, inplace=False)
    # un pivotizing dataframe to change rows to column i.e. change shape of dataframe by specification
    structured_df = pd.melt(read_file_rename, id_vars=["well_name", "month"], var_name="Commodity")
    # adding columns- 'energy' and 'units' to dataframe.
    structured_df[['energy', 'units']] = structured_df['Commodity'].str.split('(', expand=True)
    structured_df['units'] = structured_df['units'].str.replace('[)]', '', regex=True)

    return print(structured_df)


pandas_processing(r'C:\Users\DHEERAJ\PycharmProjects\hs-energy-dl\data\heb_oil.csv')
