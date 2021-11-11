from app.utils import dbconnection
import pandas as pd
from app.utils.logging_init import init_logger
import sqlalchemy

logger = init_logger()


def production_update_table(structured_df):
    #updating production lookup table
    try:
        structured_df.to_sql(
            'tbl_cnlopb_wells',
            con=conn,
            if_exists='append',
            method='multi',
            index=False,
            schema='collections',
            dtype={"well_id": sqlalchemy.types.INT,
                   "energy_product_id": sqlalchemy.types.INT,
                   "unit_of_measure_id": sqlalchemy.types.INT,
                   "month": sqlalchemy.types.DATE,
                   "value": sqlalchemy.types.INT,
                   "field_id": sqlalchemy.types.CHAR(length=5)
                   }
        )
    except Exception as e:
        logger.error(e)


def update_well_lookup_table(well_names, field):
    # Updating well lookup table
    try:
        wells_df = well_names.drop_duplicates(subset=['Well Name'], ignore_index=True)
        # Mapping field_id for well lookup table FK reference
        fields = fieldsdb_df()
        wells_df['field_id'] = wells_df['Field Name'].map(fields.set_index('field_name')['id'])
        wells_df = wells_df.drop(columns='Field Name')
        fields_df_q = fields[fields['field_name'] == field]['id'].values[0]

        wells_df = wells_df[['Well Name', 'field_id']]
        wells_df.columns = ['well_name', 'field_id']
        # Added flag file for all input values
        wells_df['flag'] = "file"

        wellsdb_check_df = get_wellids(fields_df_q)
        wellsdb_check_df = wellsdb_check_df[['well_name', 'field_id']]
        # Added flag db which were read from database
        wellsdb_check_df['flag'] = 'db'
        # Dropping the duplicate well names
        well_filter_names = pd.concat([wells_df, wellsdb_check_df]).drop_duplicates(subset=['well_name', 'field_id'],
                                                                                    keep=False)
        # Taking the well names left with file flag after dropping duplicate well names
        well_filter_names = well_filter_names[well_filter_names['flag'] == 'file']
        well_filter_names = well_filter_names.drop(columns=['flag'])
        logger.info("Number of new wellnames into lookup table {}".format(len(well_filter_names)))

        if len(well_filter_names) > 0:
            logger.info('Inserting new wells energy_etl into lookup table')
            conn = dbconnection.db_connection()
            well_filter_names.to_sql(
                'tbl_cnlopb_wells',
                con=conn,
                if_exists='append',
                method='multi',
                index=False,
                schema='collections',
                dtype={"well_name": sqlalchemy.types.VARCHAR(length=30),
                       "field_id": sqlalchemy.types.CHAR(length=5)
                       }
            )
        else:
            logger.info("All wells are already exists in lookup table")
    except Exception as e:
        logger.error(e)


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


