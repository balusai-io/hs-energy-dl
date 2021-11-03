from app.utils import dbconnection
import pandas as pd
from app.utils.logging_init import init_logger

logger = init_logger()


def get_wellids(field_id):
    # Reading well lookup table based on field_id
    try:
        conn = dbconnection.db_connection()

        sql = f"select id, field_id, well_name from collections.tbl_cnlopb_wells where field_id = '{field_id}'"
        wellsdb_df = pd.read_sql(con=conn, sql=sql)
        logger.info('Returned the available Well table successfully')
    except Exception as e:
        logger.error(e)
    return wellsdb_df


def get_all_wellids():
    # Reading all wells from well lookup table
    try:
        conn = dbconnection.db_connection()

        sql = f"select id, field_id, well_name from collections.tbl_cnlopb_wells"
        wellsdb_df = pd.read_sql(con=conn, sql=sql)

        logger.info('Returned the available Well table successfully')
    except Exception as e:
        logger.error(e)
    return wellsdb_df


def update_fields_table(fieldname):
    try:
        conn = dbconnection.db_connection()
        sql = 'select * from collections.tbl_cnlopb_fields'
        field_table = pd.read_sql(sql=sql, con=conn)
        # Checking the field is new or already exists in the fields lookup table
        if fieldname in list(field_table.field_name.values):
            logger.info("{} field is already there".format(fieldname))
        else:
            sql = f"insert into collections.tbl_cnlopb_fields (field_name) values ('{fieldname}')"
            conn.execute(sql)
            logger.info(f"Inserted new field {fieldname} into table")
    except Exception as e:
        logger.error(e)


def fieldsdb_df():
    # Reading fields lookup table
    try:
        conn = dbconnection.db_connection()
        sql = 'select ID, field_name from collections.tbl_cnlopb_fields'
        fields_db = pd.read_sql(con=conn, sql=sql)
        logger.info("Returned the fields energy_etl")
    except Exception as e:
        logger.error(e)
    return fields_db


def get_energy_units():
    # Reading metadata energy product table
    try:
        conn = dbconnection.db_connection()

        sql = 'select ID,energy_product from metadata.tbl_energy_product'
        tbl_energy_product_df = pd.read_sql(con=conn, sql=sql)
        logger.info("Returned the energy product energy_etl")
    except Exception as e:
        logger.error(e)
    return tbl_energy_product_df


def get_unitof_measure():
    # Reading metadata unit table
    try:
        conn = dbconnection.db_connection()

        sql = 'select id,unit_short from metadata.tbl_unit'
        tbl_uom_df = pd.read_sql(con=conn, sql=sql)
        tbl_uom_df.columns = ['unit_of_measure_id','uom']
        logger.info("Returned the units energy_etl ")
    except Exception as e:
        logger.error(e)
    return tbl_uom_df
