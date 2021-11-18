from app.utils import dbconnection
import pandas as pd
from app.utils.logging_init import init_logger
from app.etl.processing_file import file_processing
import sqlalchemy

logger = init_logger()


def production_update_table(structured_df):
    # updating production lookup table
    try:
        print(structured_df.dtypes)
        conn = dbconnection.db_connection()
        structured_df.to_sql(
            'tbl_cnlopb_production',
            con=conn,
            if_exists='append',
            method='multi',
            index=False,
            schema='collections',
            dtype={"well_id": sqlalchemy.types.INT,
                   "energy_product_id": sqlalchemy.types.INT,
                   "unit_of_measure_id": sqlalchemy.types.INT,
                   "month": sqlalchemy.types.DATE,
                   "value": sqlalchemy.types.FLOAT(),
                   "field_id": sqlalchemy.types.CHAR(length=5)
                   }
        )
    except Exception as e:
        logger.error(e)
