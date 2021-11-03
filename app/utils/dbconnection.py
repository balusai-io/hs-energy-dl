import os
from sqlalchemy import create_engine
from app.utils.logging_init import init_logger
from dotenv import load_dotenv

load_dotenv()


def db_connection():
    logger = init_logger()
    try:
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        database = os.getenv('DB_NAME')
        conn_url = f'''postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{database}'''
        engine = create_engine(f"{conn_url}")
        logger.info('Returned connection engine')
    except Exception as e:
        logger.error(f'{e}, Failed to connect to database, please contact administrator')
    return engine
