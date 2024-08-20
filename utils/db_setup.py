import yaml
import sqlalchemy as db
from sqlalchemy import inspect
import logging

def load_yaml(file_path, log_message):
    """
    YAML 파일을 로드합니다.
    """
    try:
        with open(file_path, 'r') as file:
            settings = yaml.safe_load(file)
        logging.info(f"{log_message} from {file_path}")
        return settings
    except Exception as e:
        logging.error(f"Failed to {log_message.lower()} from {file_path}: {e}", exc_info=True)
        raise

def load_connection_config(file_path):
    """
    데이터베이스 연결 설정 파일을 로드합니다.
    """
    return load_yaml(file_path, "Loaded connection config")

def get_database_names2(default_db_name="default_db"):
    """
    사용자로부터 데이터베이스 이름을 입력받습니다. 입력이 없으면 기본 데이터베이스 이름을 사용합니다.
    """
    db_names = input("Enter database names (comma separated), or press Enter to use default: ")
    return [db.strip() for db in db_names.split(',')] if db_names.strip() else [default_db_name]

def get_database_names():
    """
    사용자로부터 데이터베이스 이름을 입력받습니다.
    입력이 없으면 빈 리스트를 반환합니다.
    """
    db_names = input("Enter database names (comma separated), or press Enter to use default: ")
    return [db.strip() for db in db_names.split(',') if db.strip()]


def create_or_load_yaml(db_name, tables):
    """
    새 YAML 파일을 생성하거나 기존 YAML 파일을 로드합니다.
    """
    yaml_filename = f'config/{db_name}.yaml'

    create_new = input(f"Do you want to create a new YAML configuration file for {db_name}? (yes/no): ").strip().lower()
    
    if create_new == 'yes':
        command = ''
        while command not in {'truncate', 'insert', 'view'}:
            command = input("Enter command (truncate, insert, view): ").strip().lower()
            if command not in {'truncate', 'insert', 'view'}:
                print("Invalid command. Please enter 'truncate', 'insert', or 'view'.")

        print(f"Available tables in {db_name}: {', '.join(tables)}")

        table_entries = input("Enter tables and dummy data count in the format 'table1: count, table2: count, ...': ")
        table_entries = [entry.strip() for entry in table_entries.split(',')]

        yaml_data = create_yaml_data(tables, table_entries, command)

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

def create_yaml_data(tables, table_entries, command):
    """
    YAML 데이터 구조를 생성합니다.
    """
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

def create_engine_connection(connection_config, database_name=None):
    """
    데이터베이스 엔진을 생성합니다.
    """
    try:
        db_config = connection_config.get('database', {})
        user = db_config.get('user')
        password = db_config.get('password')
        host = db_config.get('host')
        port = db_config.get('port')

        if not all([user, password, host, port]):
            raise ValueError("Missing required connection parameters.")

        # 데이터베이스 이름이 제공된 경우만 추가
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

def setup_database(connection_config, database_name=None):
    """
    데이터베이스 엔진과 메타데이터를 설정하고 테이블 정보를 가져옵니다.
    데이터베이스 이름이 없는 경우 메타데이터를 반영하지 않습니다.
    """
    try:
        engine = create_engine_connection(connection_config, database_name)
        metadata = db.MetaData()

        # 데이터베이스 이름이 제공된 경우에만 메타데이터를 반영합니다.
        if database_name:
            metadata.reflect(bind=engine)
            inspector = inspect(engine)
            tables = inspector.get_table_names()
        else:
            inspector = None
            tables = []

        return engine, metadata, inspector, tables
    except Exception as e:
        logging.error(f"An error occurred while setting up the database: {e}", exc_info=True)
        raise