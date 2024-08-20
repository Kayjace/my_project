import yaml
import os
import sqlalchemy as db
from sqlalchemy import inspect
import logging
import getpass

#yaml파일 로드 펑션
def load_yaml(file_path, log_message):
    try:
        with open(file_path, 'r') as file:
            settings = yaml.safe_load(file)
        logging.info(f"{log_message} from {file_path}")
        return settings
    except Exception as e:
        logging.error(f"Failed to {log_message.lower()} from {file_path}: {e}", exc_info=True)
        raise

#커넥션 설정 yaml파일 로드
def load_connection_config(file_path):
    return load_yaml(file_path, "Loaded connection config")

#데이터베이스 이름들을 입력받고, 입력이 없으면 빈 리스트 반환.
def get_database_names():
    db_names = input("더미를 삽입할 데이터베이스를 입력해주세요 (,로 구분) : ")
    return [db.strip() for db in db_names.split(',') if db.strip()]

#사용자 응답에 따라 새로운 yaml파일을 생성하거나 기존 파일을 로드하기 위한 펑션
def create_or_load_yaml(db_name, tables):
    #yaml파일 이름은 컨피그 폴더의 db 이름과 동일한 네임을 사용
    yaml_filename = f'config/{db_name}.yaml'
    #새로운 설정파일을 만들지 확인
    create_new = input(f"{db_name}에 새로운 설정을 만들까요? (yes/no): ").strip().lower()
    #새로운 설정파일 생성시
    if create_new == 'yes':
        command = ''
        #truncate, insert, view 중 하나의 command를 입력받음
        while command not in {'truncate', 'insert', 'view'}:
            command = input("Enter command (truncate, insert, view): ").strip().lower()
            if command not in {'truncate', 'insert', 'view'}:
                print("Invalid command. Please enter 'truncate', 'insert', or 'view'.")
        #해당 DB의 테이블 이름들을 가져옴
        print(f"{db_name}의 테이블 목록: {', '.join(tables)}")

        table_entries = input("테이블의 이름과 입력할 더미데이터의 숫자를 다음 형식으로 입력하세요. 예시 - 'airport: 1000, employee: 2000, ...': ")
        table_entries = [entry.strip() for entry in table_entries.split(',')]
        #입력된 데이터를 기반으로 yaml 설정파일 생성
        yaml_data = create_yaml_data(tables, table_entries, command)

        with open(yaml_filename, 'w') as file:
            yaml.dump(yaml_data, file, default_flow_style=False)
        print(f"{yaml_filename} 설정파일을 만들었습니다.")
        return yaml_data
    #응답이 no 일시 기존에 존재하는 yaml파일을 불러옴
    elif create_new == 'no':
        print(f"기존 {yaml_filename} 설정을 불러옵니다.")
        with open(yaml_filename, 'r') as file:
            return yaml.safe_load(file)
    
    else:
        raise ValueError("잘못된 입력. 'yes' or 'no' 를 입력해주세요.")

#yaml 데이터를 만드는 펑션
def create_yaml_data(tables, table_entries, command):
    table_names_list = []
    dummy_nums = {}
    for entry in table_entries:
        if ':' in entry:
            table_name, dummy_count = entry.split(':', 1)
            table_name = table_name.strip()
            try:
                dummy_count = int(dummy_count.strip())
            except ValueError:
                print(f"Invalid dummy count for table {table_name}. Skipping.")
                continue
            if table_name in table_names_list:
                print(f"Duplicate table name {table_name} found. Skipping.")
                continue
            if table_name in tables:
                table_names_list.append(table_name)
                dummy_nums[table_name] = dummy_count
            else:
                print(f"Table name {table_name} does not exist in the database. Skipping.")

    return {
        'command': command,
        'table_names': table_names_list,
        'dummy_nums': dummy_nums,
    }

#DB패스워드를 입력받고 환경변수에 저장
def set_database_password():
    password = getpass.getpass("데이터베이스 패스워드를 입력하세요 : ")
    os.environ['DB_PASSWORD'] = password
    print("데이터베이스 패스워드가 설정되었음.")

#환경변수에 저장된 데이터베이스 패스워드를 가져옴
def get_database_password():
    return os.getenv('DB_PASSWORD')

#DB 엔진 연결을 생성
def create_engine_connection(connection_config, database_name=None):
    #connection.yaml에서 설정을 가져와서 연결 시도
    try:
        db_config = connection_config.get('database', {})
        user = db_config.get('user')
        password = get_database_password()
        if not password:
            set_database_password()
            password = get_database_password()
        host = db_config.get('host')
        port = db_config.get('port')

        if not all([user, password, host, port]):
            raise ValueError("Missing required connection parameters.")

        # 데이터베이스 이름이 주어진 경우에는 해당 이름의 DB에 연결
        if database_name:
            connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database_name}"
        else:
            connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/"
        engine = db.create_engine(connection_string)
        logging.info(f"Created database engine with connection string: {connection_string}")
        return engine
    except Exception as e:
        logging.error(f"An error occurred while creating the database engine: {e}", exc_info=True)
        raise

#설정과 데이터베이스 이름으로 데이터베이스 엔진과 메타데이터를 설정, 테이블 정보를 가져옴.
def setup_database(connection_config, database_name=None):
    try:
        engine = create_engine_connection(connection_config, database_name)
        metadata = db.MetaData()
        # 데이터베이스 이름이 제공된 경우에만 메타데이터 반영.
        if database_name:
            metadata.reflect(bind=engine)
            inspector = inspect(engine)
            tables = inspector.get_table_names()
        else:
            inspector = None
            tables = []
        #엔진, 메타데이터, 인스펙터, 테이블 반환
        return engine, metadata, inspector, tables
    except Exception as e:
        logging.error(f"An error occurred while setting up the database: {e}", exc_info=True)
        raise