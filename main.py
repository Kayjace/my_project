import logging
from utils.db_setup import load_settings, load_connection_config, setup_database, get_database_names, create_or_load_yaml
from utils.db_utils import get_table_detail, truncate_table, select_all_data
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
        # 설정 파일 로드
        connection_config = load_connection_config('config/connection.yaml')

        # 사용자로부터 데이터베이스 이름 입력받기
        database_names = get_database_names()
        if not database_names:
            logging.error('No database names found.')
            return

        for db_name in database_names:
            logging.info(f"Processing database: {db_name}")

            # 데이터베이스 설정
            connection_config['database_name'] = db_name  # 설정 파일에 데이터베이스 이름 추가
            try:
                engine, metadata, inspector, tables = setup_database(connection_config, db_name)
            except Exception as e:
                logging.error(f"Skipping database {db_name} due to setup error: {e}", exc_info=True)
                continue

            # YAML 파일 설정
            yaml_data = create_or_load_yaml(db_name, tables)
            if yaml_data is None:
                logging.error(f"Failed to create or load YAML configuration for database {db_name}.")
                continue

            table_detail = get_table_detail(engine, tables)
            
            table_names = yaml_data.get('table_names', [])
            dummy_nums = yaml_data.get('dummy_nums', {})

            if not table_names:
                logging.error(f'No tables provided in the configuration file for database {db_name}.')
                continue

            for table_name in table_names:
                if table_name not in tables:
                    logging.error(f'{table_name} 테이블은 데이터베이스에 존재하지 않는 테이블 이름입니다.')
                    continue

                dummy_num = dummy_nums.get(table_name, 1000)  # 기본값 1000
                dummy_num = max(dummy_num, 1000)  # dummy_num이 1000보다 작으면 1000으로 설정
                
                logging.info(f"Processing table {table_name} with {dummy_num} dummy records.")
                
                try:
                    if yaml_data.get('command') == 'truncate':
                        truncate_table(table_name, engine)
                        datas = create_dummy_data_list(table_name, dummy_num, table_detail, engine)
                        insert_dummy_data(table_name, metadata, engine, datas)
                        select_all_data(table_name, engine)
                        logging.info(f"Successfully processed table {table_name}.")
                    if yaml_data.get('command') == 'insert':
                        datas = create_dummy_data_list(table_name, dummy_num, table_detail, engine)
                        insert_dummy_data(table_name, metadata, engine, datas)
                        select_all_data(table_name, engine)
                        logging.info(f"Successfully processed table {table_name}.")
                    if yaml_data.get('command') == 'view':
                        select_all_data(table_name, engine)
                    logging.info(f"Successfully processed table {table_name}.")
                except Exception as e:
                    logging.error(f"An error occurred while processing table {table_name}: {e}", exc_info=True)

            logging.info(f"Database {db_name} processing completed.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    main()