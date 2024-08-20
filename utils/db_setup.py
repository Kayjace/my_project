import yaml
import sqlalchemy as db
from sqlalchemy import inspect
import logging

def load_settings(file_path):
    """
    설정 파일을 로드합니다.
    """
    try:
        with open(file_path, 'r') as file:
            settings = yaml.safe_load(file)
        logging.info(f"Loaded settings from {file_path}")
        return settings
    except Exception as e:
        logging.error(f"Failed to load settings from {file_path}: {e}", exc_info=True)
        raise

def load_connection_config(file_path):
    """
    데이터베이스 연결 설정 파일을 로드합니다.
    """
    try:
        with open(file_path, 'r') as file:
            settings = yaml.safe_load(file)
        logging.info(f"Loaded connection config from {file_path}")
        return settings
    except Exception as e:
        logging.error(f"Failed to load connection config from {file_path}: {e}", exc_info=True)
        raise

def get_database_names():
    # 사용자로부터 데이터베이스 이름을 입력받음
    db_names = input("Enter database names (comma separated) : ")
    if db_names.strip():
        return [db.strip() for db in db_names.split(',')]

def create_or_load_yaml(db_name, tables):
    yaml_filename = f'config/{db_name}.yaml'

    # 사용자가 새 YAML 파일을 생성할지, 기존 파일을 사용할지 선택하게 함
    create_new = input(f"Do you want to create a new YAML configuration file for {db_name}? (yes/no): ").strip().lower()
    
    if create_new == 'yes':
        command = ''
        while command not in {'truncate', 'insert', 'view'}:
            command = input("Enter command (truncate, insert, view): ").strip().lower()
            if command not in {'truncate', 'insert', 'view'}:
                print("Invalid command. Please enter 'truncate', 'insert', or 'view'.")

        # 사용자에게 테이블 이름을 보여주기 전에 새 YAML 파일을 생성
        print(f"Available tables in {db_name}: {', '.join(tables)}")

        table_entries = input("Enter tables and dummy data count in the format 'table1: count, table2: count, ...': ")
        table_entries = [entry.strip() for entry in table_entries.split(',')]

        table_names_list = []
        dummy_nums = {}
        for entry in table_entries:
            if ':' in entry:
                table_name, dummy_count = entry.split(':', 1)
                table_name = table_name.strip()
                dummy_count = int(dummy_count.strip())
                if table_name in table_names_list:
                    print(f"Duplicate table name {table_name} found. Skipping.")
                    continue
                if table_name in tables:
                    table_names_list.append(table_name)
                    dummy_nums[table_name] = dummy_count
                else:
                    print(f"Table name {table_name} does not exist in the database. Skipping.")

        yaml_data = {
            'command': command,                 # command
            'table_names': table_names_list,  # table_names 위로
            'dummy_nums': dummy_nums,          # dummy_nums 아래로
        }

        with open(yaml_filename, 'w') as file:
            yaml.dump(yaml_data, file, default_flow_style=False)

        print(f"{yaml_filename} has been created.")
        return yaml_data

    elif create_new == 'no':
        print(f"Loading existing {yaml_filename} configuration.")
        with open(yaml_filename, 'r') as file:
            return yaml.safe_load(file)
    
    else:
        raise ValueError("Invalid input. Please enter 'yes' or 'no'.")

def create_engine_connection2(config, database_name=None):
    """
    데이터베이스 엔진을 생성합니다.
    """
    try:
        db_config = config['database']
        base_url = f"{db_config['type']}+{db_config['driver']}://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/"
        if database_name:
            connection_string = f"{base_url}{database_name}"
        else:
            connection_string = base_url
        engine = db.create_engine(connection_string)
        logging.info(f"Created database engine for {database_name if database_name else 'default'}")
        return engine
    except Exception as e:
        logging.error(f"Failed to create database engine: {e}", exc_info=True)
        raise

def create_engine_connection(connection_config, database_name=None):
    """
    데이터베이스 엔진을 생성합니다.
    """
    try:
        # 'database' 키에서 설정 읽기
        db_config = connection_config.get('database', {})
        
        # 데이터베이스 이름이 주어지면 사용, 없으면 기본 데이터베이스 이름 사용
        db_name = database_name or db_config.get('database_name', 'default_db')
        
        # 필요한 설정 값 추출
        user = db_config.get('user')
        password = db_config.get('password')
        host = db_config.get('host')
        port = db_config.get('port')

        # 필수 설정 값이 모두 있는지 확인
        if not all([user, password, host, port]):
            raise ValueError("Missing required connection parameters.")
        
        # 연결 문자열 생성
        connection_string = (
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"
        )
        
        # 엔진 생성
        engine = db.create_engine(connection_string)
        logging.info(f"Created database engine with connection string: {connection_string}")
        return engine
    except Exception as e:
        logging.error(f"An error occurred while creating the database engine: {e}", exc_info=True)
        raise

def setup_database2(connection_config, database_name=None):
    """
    데이터베이스 엔진과 메타데이터를 설정하고 테이블 정보를 가져옵니다.
    """
    engine = create_engine_connection(connection_config, database_name)
    metadata = db.MetaData()
    metadata.reflect(bind=engine)
    inspector = inspect(engine)

    tables = inspector.get_table_names()
    logging.info(f"Tables in database {database_name if database_name else 'default'}: {tables}")

    return engine, metadata, inspector, tables

def setup_database(connection_config, database_name=None):
    """
    데이터베이스 엔진과 메타데이터를 설정하고 테이블 정보를 가져옵니다.
    """
    try:
        engine = create_engine_connection(connection_config, database_name)
        metadata = db.MetaData()
        metadata.reflect(bind=engine)
        inspector = inspect(engine)

        tables = inspector.get_table_names()

        return engine, metadata, inspector, tables
    except Exception as e:
        logging.error(f"An error occurred while setting up the database: {e}", exc_info=True)
        raise