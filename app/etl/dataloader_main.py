import os
from dotenv import load_dotenv
from app.etl.pdf_downloader import pdffile_download
from app.etl.extraction_and_cleaning import extraction_data
from app.etl import production_data

from app.etl.processing_file import file_processing
import json
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


def data_loader():
    try:
        pdffile_download(parent_path, folder_filter, answer)
        fields = list(json.loads(os.getenv('FIELDS')).keys())
        for field in fields:
            extracting_and_cleaning = extraction_data(folder_path, field)
            extracting_and_cleaning['Field Name'] = field
            lookup_tables.update_fields_table(field)
            well_df = extracting_and_cleaning[['Well Name', 'Field Name']]

            lookup_tables.update_well_lookup_table(well_df, field)
            extracting_and_cleaning = extracting_and_cleaning.drop(columns=['Field Name'])
            file_transformation = file_processing(extracting_and_cleaning)
            production_data.production_update_table(file_transformation)
        logger.info("Data extracted and inserted successfully")
    except Exception as e:
        logger.error(e)


if __name__ == '__main__':
    data_loader()
