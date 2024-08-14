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

def create_engine_connection(config):
    """
    데이터베이스 엔진을 생성합니다.
    """
    try:
        db_config = config['database']
        base_url = f"{db_config['type']}+{db_config['driver']}://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/"
        database_name = db_config['database_name']
        connection_string = f"{base_url}{database_name}"
        engine = db.create_engine(connection_string)
        logging.info(f"Created database engine for {database_name}")
        return engine
    except Exception as e:
        logging.error(f"Failed to create database engine: {e}", exc_info=True)
        raise

def setup_database(connection_config):
    """
    데이터베이스 엔진과 메타데이터를 설정하고 테이블 정보를 가져옵니다.
    """
    engine = create_engine_connection(connection_config)
    metadata = db.MetaData()
    metadata.reflect(bind=engine)
    inspector = inspect(engine)

    tables = inspector.get_table_names()
    logging.info(f"Tables in database: {tables}")

    return engine, metadata, inspector, tables