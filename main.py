import logging
from utils.db_setup import load_settings, load_connection_config, setup_database
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
        settings = load_settings('config/settings.yaml')
        connection_config = load_connection_config('config/connection.yaml')

        # 데이터베이스 설정
        engine, metadata, inspector, tables = setup_database(connection_config)

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