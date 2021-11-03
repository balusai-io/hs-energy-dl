import os
from dotenv import load_dotenv
from app.etl.pdf_downloader import pdffile_download
from app.etl.extraction_and_cleaning import extraction

from app.etl.processing_file import file_processing
import json
import pandas as pd
from app.utils.logging_init import init_logger
load_dotenv()

logger = init_logger()

folder_path = os.getenv('BASE_DATA_PATH')
folder_filter = json.loads(os.getenv('FIELDS'))
parent_path = os.getenv('BASE_DATA_PATH')
answer = os.getenv('ALL_YEARS')
units_table = pd.read_csv(r'C:/Users/DHEERAJ/PycharmProjects/hs-energy-dl/tbl_unit.csv', header=0)
product_table = pd.read_csv(r'C:/Users/DHEERAJ/PycharmProjects/hs-energy-dl/energy_product.csv', header=0)
file_path = r'C:/Users/DHEERAJ/PycharmProjects/hs-energy-dl/csv_file.csv'


def data_loader():
    pdf_files_download = pdffile_download(parent_path, folder_filter, answer)
    extracting_and_cleaning = extraction(folder_path)
    file_transformation = file_processing(file_path, units_table, product_table)
    return file_transformation