import logging
from utils.db_setup import load_connection_config, setup_database, get_database_names, create_or_load_yaml
from utils.db_utils import get_table_detail, truncate_table, select_all_data, check_yaml_exists
from utils.data_generator import create_dummy_data_list
from utils.insert_data import insert_dummy_data


#최소 더미데이터 숫자
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

#메인 시작
def main():
    try:
        # 설정파일 로드
        connection_config = load_connection_config('connection.yaml')

        # 초기 엔진 연결. 데이터베이스 이름 없이 연결.
        connection_config_no_db = connection_config.copy()
        connection_config_no_db.pop('database_name', None)  # 기본 DB 이름 제거
        engine, _, _, _ = setup_database(connection_config_no_db, None)

        # DB 이름 대신 설정파일을 사용할지 여부 확인
        use_predefined_dbs = input("DB 이름들을 직접 입력하는 대신 connection.yaml 의 설정을 사용할까요? (yes/no): ").strip().lower()
        while use_predefined_dbs not in {'yes', 'no'}:
            print("잘못된 입력입니다. 'yes' or 'no'를 입력해주세요.")
            use_predefined_dbs = input("DB 이름들을 직접 입력하는 대신 connection.yaml 의 설정을 사용할까요? (yes/no): ").strip().lower()
        
        if use_predefined_dbs == 'yes':
            # connection.yaml에서 사전 설정된 DB 네임들 가져오기
            predefined_db_names = connection_config.get('dbnames', [])
            if not predefined_db_names:
                logging.error("No predefined database names found in connection.yaml.")
                return
            
            # YAML 파일이 존재 여부 체크
            missing_yaml_files = check_yaml_exists(predefined_db_names)
            if missing_yaml_files:
                logging.error(f"The following databases have no corresponding YAML files: {', '.join(missing_yaml_files)}")
                return

            database_names = predefined_db_names
        

        elif use_predefined_dbs == 'no':
            while True:
                # 사용자로부터 데이터베이스 이름 입력받기
                database_names = get_database_names()
                if not database_names:
                    logging.error('DB를 입력받지 못했습니다.')
                    continue

                invalid_db_names = []
                for db_name in database_names:
                    # DB이름 존재여부 체크
                    connection_config_no_db['database_name'] = db_name
                    try:
                        temp_engine, _, _, _ = setup_database(connection_config_no_db, db_name)
                    except Exception as e:
                        invalid_db_names.append(db_name)
                #존재하지 않는 DB이름들 보여주고 다시 입력받음
                if invalid_db_names:
                    logging.error(f"The following database names do not exist: {', '.join(invalid_db_names)}")
                    print(f"다음 DB는 존재하지 않습니다: {', '.join(invalid_db_names)}")
                    print("DB 이름들을 다시 입력해 주세요.")
                    continue
                else:
                    break


        for db_name in database_names:
            logging.info(f"Processing database: {db_name}")

            # 데이터베이스 이름으로 연결 설정
            connection_config['database_name'] = db_name
            try:
                engine, metadata, inspector, tables = setup_database(connection_config, db_name)
            except Exception as e:
                logging.error(f"Skipping database {db_name} due to setup error: {e}", exc_info=True)
                continue

            # DB에 해당하는 YAML 파일 불러오거나 작성
            yaml_data = create_or_load_yaml(db_name, tables)
            if yaml_data is None:
                logging.error(f"Failed to create or load YAML configuration for database {db_name}.")
                continue

            #테이블들의 디테일 가져옴
            table_detail = get_table_detail(engine, tables)
            #yaml파일에서 테이블 이름, 더미데이터 수, 커맨드 불러옴
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
                #더미데이터 최소 숫자를 1000개로 설정
                dummy_num = max(dummy_nums.get(table_name, DEFAULT_DUMMY_NUM), DEFAULT_DUMMY_NUM)
                logging.info(f"Processing table {table_name} with {dummy_num} dummy records.")
                #command에 따라 더미데이터 삽입 등 수행
                try:
                    if command == 'truncate':
                        truncate_table(table_name, engine)
                        datas = create_dummy_data_list(table_name, dummy_num, table_detail, engine)
                        insert_dummy_data(table_name, metadata, engine, datas)
                        select_all_data(table_name, engine)
                    elif command == 'insert':
                        datas = create_dummy_data_list(table_name, dummy_num, table_detail, engine)
                        insert_dummy_data(table_name, metadata, engine, datas)
                        select_all_data(table_name, engine)
                    elif command == 'view':
                        select_all_data(table_name, engine)
                    else:
                        logging.error(f"Unknown command '{command}' in YAML configuration.")
                        continue

                    logging.info(f"Successfully processed table {table_name}.")
                except Exception as e:
                    logging.error(f"An error occurred while processing table {table_name}: {e}", exc_info=True)

            logging.info(f"Database {db_name} processing completed.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    main()