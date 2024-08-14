import yaml
import sqlalchemy as db
from sqlalchemy import inspect
import logging
from utils.db_utils import create_engine_connection, get_table_detail, truncate_table, select_all_data
from utils.data_generator import create_dummy_data_list
from utils.insert_data import insert_dummy_data

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('application.log'),
        logging.StreamHandler()
    ]
)

def main():
    try:
        def load_settings(file_path):
            try:
                with open(file_path, 'r') as file:
                    settings = yaml.safe_load(file)
                logging.info(f"Loaded settings from {file_path}")
                return settings
            except Exception as e:
                logging.error(f"Failed to load settings from {file_path}: {e}", exc_info=True)
                raise

        def load_connection_config(file_path):
            try:
                with open(file_path, 'r') as file:
                    settings = yaml.safe_load(file)
                logging.info(f"Loaded connection config from {file_path}")
                return settings
            except Exception as e:
                logging.error(f"Failed to load connection config from {file_path}: {e}", exc_info=True)
                raise

        settings = load_settings('config/settings.yaml')
        connection_config = load_connection_config('config/connection.yaml')

        db_config = connection_config['database']
        BASE_DATABASE_URL = f"{db_config['type']}+{db_config['driver']}://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/"
        DATABASE_NAME = db_config['database_name']
        
        logging.info(f"Connecting to database {DATABASE_NAME} at {BASE_DATABASE_URL}")

        engine = create_engine_connection(BASE_DATABASE_URL, DATABASE_NAME)
        metadata = db.MetaData()
        metadata.reflect(bind=engine)
        inspector = inspect(engine)

        tables = inspector.get_table_names()
        logging.info(f"Tables in database: {tables}")

        table_detail = get_table_detail(engine, tables)
        
        table_names = settings.get('tables', [])
        dummy_nums = settings.get('dummy_numbers', {})

        for table_name in table_names:
            if table_name not in tables:
                logging.error(f'{table_name} 테이블은 데이터베이스에 존재하지 않는 테이블 이름입니다.')
                continue

            dummy_num = dummy_nums.get(table_name, 1000)  # 기본값 1000
            logging.info(f"Processing table {table_name} with {dummy_num} dummy records.")
            
            try:
                truncate_table(table_name, engine)
                datas = create_dummy_data_list(table_name, dummy_num, table_detail, engine)
                insert_dummy_data(table_name, metadata, engine, datas)
                select_all_data(table_name, engine)
                logging.info(f"Successfully processed table {table_name}.")
            except Exception as e:
                logging.error(f"An error occurred while processing table {table_name}: {e}", exc_info=True)

        logging.info("Script execution completed.")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    main()