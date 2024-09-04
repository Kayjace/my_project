import logging
import os
from sqlalchemy import text
from sqlalchemy.engine import reflection
from utils.db_setup import (
    load_connection_config, 
    setup_database, 
    create_or_load_yaml,
    get_database_names,
    set_database_password
)
from utils.db_utils import (
    get_table_detail, 
    truncate_table, 
    select_paginated_data, 
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

def select_server_instance(connection_config):
    servers = connection_config.get('servers', {})
    if not servers:
        logging.error("No server instances found in connection.yaml.")
        return None

    print("Available server instances:")
    for i, server_name in enumerate(servers.keys(), 1):
        print(f"{i}. {server_name}")

    while True:
        choice = input("Select a server instance (enter the number): ")
        try:
            index = int(choice) - 1
            if 0 <= index < len(servers):
                return list(servers.keys())[index]
            else:
                print("Invalid selection. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a number.")

def get_database_names_from_config(server_config, server_name):
    predefined_db_names = server_config.get('dbnames', [])
    if not predefined_db_names:
        logging.error("No predefined database names found for this server.")
        return None

    missing_yaml_files = check_yaml_exists(predefined_db_names, server_name)
    if missing_yaml_files:
        logging.error(f"The following databases have no corresponding YAML files: {', '.join(missing_yaml_files)}")
        return None

    return predefined_db_names

def process_table(engine, metadata, table_name, table_detail, command):
    if command == 'truncate':
        truncate_table(table_name, engine)
        datas = create_dummy_data_list(table_name, DEFAULT_DUMMY_NUM, table_detail, engine)
        insert_dummy_data(table_name, metadata, engine, datas)
        select_paginated_data(table_name, engine)
    elif command == 'insert':
        datas = create_dummy_data_list(table_name, DEFAULT_DUMMY_NUM, table_detail, engine)
        insert_dummy_data(table_name, metadata, engine, datas)
        select_paginated_data(table_name, engine)
    elif command == 'view':
        page = 1
        while True:
            row_count = select_paginated_data(table_name, engine, page=page)
            if row_count < 1000:
                break  # 더 이상 데이터가 없으면 종료
            next_page = input("다음 페이지를 보시겠습니까? (yes/no): ").strip().lower()
            if next_page != 'yes':
                break
            page += 1
    else:
        logging.error(f"Unknown command '{command}' in YAML configuration.")

def get_schemas(engine):
    inspector = reflection.Inspector.from_engine(engine)
    all_schemas = inspector.get_schema_names()
    # 시스템 스키마 제외
    user_schemas = [schema for schema in all_schemas if schema not in ['information_schema', 'mysql', 'performance_schema']]
    return user_schemas

def get_tables(engine, schema):
    inspector = reflection.Inspector.from_engine(engine)
    return inspector.get_table_names(schema=schema)

def get_views(engine, schema):
    inspector = reflection.Inspector.from_engine(engine)
    return inspector.get_view_names(schema=schema)

def get_table_info(engine, schema, table):
    inspector = reflection.Inspector.from_engine(engine)
    columns = inspector.get_columns(table, schema=schema)
    pk_constraint = inspector.get_pk_constraint(table, schema=schema)
    indexes = inspector.get_indexes(table, schema=schema)
    return {
        'columns': columns,
        'primary_key': pk_constraint,
        'indexes': indexes
    }

def get_view_info(engine, schema, view):
    inspector = reflection.Inspector.from_engine(engine)
    columns = inspector.get_columns(view, schema=schema)
    return {
        'columns': columns
    }

def get_table_ddl(engine, schema, table):
    ddl_query = text(f"SHOW CREATE TABLE {schema}.{table}")
    with engine.connect() as conn:
        result = conn.execute(ddl_query)
        ddl = result.fetchone()[1]
    return ddl

def main_menu():
    print("\nMain Menu:")
    print("1. List schemas")
    print("2. List tables in a schema")
    print("3. List views in a schema")
    print("4. Get table and column info with comments in a schema")
    print("5. Get view and column info with comments in a schema")
    print("6. Get specific table column information")
    print("7. Generate table DDL")
    print("8. Insert/Truncate/View DummyData")
    print("9. Exit")
    return input("Enter your choice: ")

def main():
    try:
        # YAML 파일의 절대 경로 설정
        current_dir = os.path.dirname(os.path.abspath(__file__))
        yaml_path = os.path.join(current_dir, 'connection.yaml')
        
        connection_config = load_connection_config(yaml_path)
        server_name = select_server_instance(connection_config)
        if not server_name:
            return

        server_config = connection_config['servers'][server_name]
        connection_config_no_db = {
            'database': {
                'host': server_config.get('host'),
                'user': server_config.get('user'),
                'port': server_config.get('port'),
                'type': server_config.get('type'),
                'driver': server_config.get('driver')
            }
        }
        
        set_database_password()
        
        try:
            engine, _, _, _ = setup_database(connection_config_no_db)
        except Exception as e:
            logging.error(f"Failed to connect to the server: {e}")
            return

        while True:
            choice = main_menu()
            if choice == '1':
                schemas = get_schemas(engine)
                print(f"Schemas in database {server_name}:", schemas)
            elif choice == '2':
                schemas = get_schemas(engine)
                print(f"Schemas in database {server_name}:", schemas)
                schema = input("Enter schema name: ")
                tables = get_tables(engine, schema)
                print(f"Tables in schema {schema}:", tables)
            elif choice == '3':
                schemas = get_schemas(engine)
                print(f"Schemas in database {server_name}:", schemas)
                schema = input("Enter schema name: ")
                views = get_views(engine, schema)
                print(f"Views in schema {schema}:", views)
            elif choice == '4':
                schemas = get_schemas(engine)
                print(f"Schemas in database {server_name}:", schemas)
                schema = input("Enter schema name: ")
                tables = get_tables(engine, schema)
                print(f"Tables in schema {schema}:", tables)
                for table in tables:
                    info = get_table_info(engine, schema, table)
                    print(f"Table: {table}, Info: {info}")
            elif choice == '5':
                schemas = get_schemas(engine)
                print(f"Schemas in database {server_name}:", schemas)
                schema = input("Enter schema name: ")
                views = get_views(engine, schema)
                print(f"Views in schema {schema}:", views)
                for view in views:
                    info = get_view_info(engine, schema, view)
                    print(f"View: {view}, Info: {info}")
            elif choice == '6':
                schemas = get_schemas(engine)
                print(f"Schemas in database {server_name}:", schemas)
                schema = input("Enter schema name: ")
                tables = get_tables(engine, schema)
                print(f"Tables in schema {schema}:", tables)
                table = input("Enter table name: ")
                info = get_table_info(engine, schema, table)
                print("Column info:", info['columns'])
            elif choice == '7':
                schemas = get_schemas(engine)
                print(f"Schemas in database {server_name}:", schemas)
                schema = input("Enter schema name: ")
                tables = get_tables(engine, schema)
                print(f"Tables in schema {schema}:", tables)
                table = input("Enter table name: ")
                ddl = get_table_ddl(engine, schema, table)
                print("Table DDL:", ddl)
            elif choice == '8':
                use_predefined_dbs = input("Use predefined databases from connection.yaml? (yes/no): ").strip().lower()
                while use_predefined_dbs not in {'yes', 'no'}:
                    print("Invalid input. Please enter 'yes' or 'no'.")
                    use_predefined_dbs = input("Use predefined databases from connection.yaml? (yes/no): ").strip().lower()

                if use_predefined_dbs == 'yes':
                    database_names = get_database_names_from_config(server_config, server_name)
                    if not database_names:
                        continue
                else:
                    database_names = get_database_names()
                    if not database_names:
                        continue

                for db_name in database_names:
                    logging.info(f"Processing database: {db_name}")
                    connection_config_no_db['database']['database_name'] = db_name

                    try:
                        engine, metadata, _, tables = setup_database(connection_config_no_db, db_name)
                    except Exception as e:
                        logging.error(f"Skipping database {db_name} due to setup error: {e}", exc_info=True)
                        continue

                    yaml_data = create_or_load_yaml(db_name, tables, server_name)
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
                            logging.error(f'{table_name} table does not exist in the database.')
                            continue

                        dummy_num = max(dummy_nums.get(table_name, DEFAULT_DUMMY_NUM), DEFAULT_DUMMY_NUM)
                        logging.info(f"Processing table {table_name} with {dummy_num} dummy records.")

                        try:
                            process_table(engine, metadata, table_name, table_detail, command)
                            logging.info(f"Successfully processed table {table_name}.")
                        except Exception as e:
                            logging.error(f"An error occurred while processing table {table_name}: {e}", exc_info=True)

                    logging.info(f"Database {db_name} processing completed.")
            elif choice == '9':
                print("Exiting...")
                break
            else:
                print("Invalid choice. Please try again.")

        os.environ.pop('DB_PASSWORD', None)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    main()