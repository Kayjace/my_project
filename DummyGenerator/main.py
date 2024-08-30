import logging
import os
from utils.db_setup import (
    load_connection_config, 
    setup_database, 
    create_or_load_yaml,
    get_database_names
)
from utils.db_utils import (
    get_table_detail, 
    truncate_table, 
    select_all_data, 
    check_yaml_exists
)
from utils.data_generator import create_dummy_data_list
from utils.insert_data import insert_dummy_data

DEFAULT_DUMMY_NUM = 1000

# 로깅 기본 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('application.log'),
        logging.StreamHandler()
    ]
)


def get_database_names_from_config(connection_config):
    predefined_db_names = connection_config.get('dbnames', [])
    if not predefined_db_names:
        logging.error("No predefined database names found in connection.yaml.")
        return None

    missing_yaml_files = check_yaml_exists(predefined_db_names)
    if missing_yaml_files:
        logging.error(f"The following databases have no corresponding YAML files: {', '.join(missing_yaml_files)}")
        return None

    return predefined_db_names

def get_user_database_names(connection_config_no_db):
    while True:
        database_names = get_database_names()
        if not database_names:
            logging.error('DB를 입력받지 못했습니다.')
            continue

        invalid_db_names = []
        for db_name in database_names:
            connection_config_no_db['database_name'] = db_name
            try:
                setup_database(connection_config_no_db, db_name)
            except Exception:
                invalid_db_names.append(db_name)

        if invalid_db_names:
            logging.error(f"The following database names do not exist: {', '.join(invalid_db_names)}")
            print(f"다음 DB는 존재하지 않습니다: {', '.join(invalid_db_names)}")
            print("DB 이름들을 다시 입력해 주세요.")
        else:
            return database_names


def process_table(engine, metadata, table_name, table_detail, command):
    if command == 'truncate':
        truncate_table(table_name, engine)
        datas = create_dummy_data_list(table_name, DEFAULT_DUMMY_NUM, table_detail, engine)
        insert_dummy_data(table_name, metadata, engine, datas)
        select_all_data(table_name, engine)
    elif command == 'insert':
        datas = create_dummy_data_list(table_name, DEFAULT_DUMMY_NUM, table_detail, engine)
        insert_dummy_data(table_name, metadata, engine, datas)
        select_all_data(table_name, engine)
    elif command == 'view':
        select_all_data(table_name, engine)
    else:
        logging.error(f"Unknown command '{command}' in YAML configuration.")

def main():
    try:
        connection_config = load_connection_config('connection.yaml')
        connection_config_no_db = {k: v for k, v in connection_config.items() if k != 'database_name'}
        setup_database(connection_config_no_db, None)

        use_predefined_dbs = input("DB 이름들을 직접 입력하는 대신 connection.yaml 의 설정을 사용할까요? (yes/no): ").strip().lower()
        while use_predefined_dbs not in {'yes', 'no'}:
            print("잘못된 입력입니다. 'yes' or 'no'를 입력해주세요.")
            use_predefined_dbs = input("DB 이름들을 직접 입력하는 대신 connection.yaml 의 설정을 사용할까요? (yes/no): ").strip().lower()

        if use_predefined_dbs == 'yes':
            database_names = get_database_names_from_config(connection_config)
            if not database_names:
                return
        else:
            database_names = get_user_database_names(connection_config_no_db)
            if not database_names:
                return

        for db_name in database_names:
            logging.info(f"Processing database: {db_name}")
            connection_config['database_name'] = db_name

            try:
                engine, metadata, _, tables = setup_database(connection_config, db_name)
            except Exception as e:
                logging.error(f"Skipping database {db_name} due to setup error: {e}", exc_info=True)
                continue

            yaml_data = create_or_load_yaml(db_name, tables)
            if yaml_data is None:
                logging.error(f"Failed to create or load YAML configuration for database {db_name}.")
                continue

            table_detail = get_table_detail(engine, tables)
            table_names = yaml_data.get('table_names', [])
            dummy_nums = yaml_data.get('dummy_nums', {})
            command = yaml_data.get('command')

            if not table_names:
                logging.error(f'No tables provided in the configuration file for database {db_name}.')
                continue

            for table_name in table_names:
                if table_name not in tables:
                    logging.error(f'{table_name} 테이블은 데이터베이스에 존재하지 않는 테이블 이름입니다.')
                    continue

                dummy_num = max(dummy_nums.get(table_name, DEFAULT_DUMMY_NUM), DEFAULT_DUMMY_NUM)
                logging.info(f"Processing table {table_name} with {dummy_num} dummy records.")

                try:
                    process_table(engine, metadata, table_name, table_detail, command)
                    logging.info(f"Successfully processed table {table_name}.")
                except Exception as e:
                    logging.error(f"An error occurred while processing table {table_name}: {e}", exc_info=True)

            logging.info(f"Database {db_name} processing completed.")

        os.environ.pop('DB_PASSWORD', None)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    main()