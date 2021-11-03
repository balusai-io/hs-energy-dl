import os
from dotenv import load_dotenv
from app.etl.pdf_downloader import pdffile_download
from app.etl.extraction_and_cleaning import extraction

from app.etl.processing_file import file_processing
import json
import pandas as pd
from app.utils.logging_init import init_logger
from app.etl import lookup_tables

load_dotenv()
logger = init_logger()

folder_path = os.getenv('BASE_DATA_PATH')
folder_filter = json.loads(os.getenv('FIELDS'))
parent_path = os.getenv('BASE_DATA_PATH')
answer = os.getenv('ALL_YEARS')
units_table = lookup_tables.get_unitof_measure()
product_table = lookup_tables.get_energy_units()
file_path = r'C:/Users/DHEERAJ/PycharmProjects/hs-energy-dl/csv_file.csv'


def data_loader():
    pdffile_download(parent_path, folder_filter, answer)
    fields = list(json.loads(os.getenv('FIELDS')).keys())
    for field in fields:
        extracting_and_cleaning = extraction(folder_path, field)
        lookup_tables.update_fields_table(field)
    # fieldsdb_df = lookup_tables.fieldsdb_df()
    file_transformation = file_processing(extracting_and_cleaning)
    logger.info(file_transformation)


if __name__ == '__main__':
    data_loader()
