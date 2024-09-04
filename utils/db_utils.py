from sqlalchemy import create_engine, inspect, text

<<<<<<< Updated upstream:utils/db_utils.py
def create_engine_connection(base_url, database_name):
    connection_string = f"{base_url}{database_name}"
    engine = create_engine(connection_string, echo=False)
    return engine
=======
def check_yaml_exists(db_names, server_name):
    missing_yaml_files = []
    for db_name in db_names:
        yaml_filename = f'config/{server_name}/{db_name}.yaml'
        if not os.path.exists(yaml_filename):
            missing_yaml_files.append(db_name)
    return missing_yaml_files
>>>>>>> Stashed changes:DummyGenerator/utils/db_utils.py

def get_table_detail(engine, tables):
    inspector = inspect(engine)
    table_detail = {}
<<<<<<< Updated upstream:utils/db_utils.py

=======
>>>>>>> Stashed changes:DummyGenerator/utils/db_utils.py
    for table in tables:
        columns = inspector.get_columns(table)
        primary_keys = set(inspector.get_pk_constraint(table)['constrained_columns'])
        unique_constraints = inspector.get_unique_constraints(table)
        unique_column_names = set(
            column_name
            for constraint in unique_constraints
            for column_name in constraint['column_names']
        )
<<<<<<< Updated upstream:utils/db_utils.py

=======
>>>>>>> Stashed changes:DummyGenerator/utils/db_utils.py
        with engine.connect() as connection:
            auto_increment_query = text("""
                SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = :table_name 
                AND EXTRA LIKE '%auto_increment%'
            """)
            result = connection.execute(auto_increment_query, {'table_name': table})
            auto_increment_columns = {row[0] for row in result}

        temp = []
        for column in columns:
            column_name = column['name']
            column['primary_key'] = column_name in primary_keys
            column['unique'] = column_name in unique_column_names
            column['autoincrement'] = column_name in auto_increment_columns
            temp.append(column)

        table_detail[table] = {'details': temp}

    return table_detail

<<<<<<< Updated upstream:utils/db_utils.py
def get_existing_unique_values(table_name, column_name, engine):
    with engine.connect() as conn:
        query = text(f"SELECT {column_name} FROM {table_name};")
        result = conn.execute(query)
        return set(row[0] for row in result)

=======
>>>>>>> Stashed changes:DummyGenerator/utils/db_utils.py
def truncate_table(selected_table, engine):
    conn = engine.connect()
    conn.execute(text(f"TRUNCATE TABLE {selected_table};"))
    conn.commit()
    print(f"{selected_table} 내 데이터들을 삭제했습니다.")

<<<<<<< Updated upstream:utils/db_utils.py
def select_all_data(selected_table, engine):
    with engine.begin() as conn:
        result = conn.execute(text(f"SELECT * FROM {selected_table};"))
    print(f"{selected_table}의 데이터")
    for i in result:
        print(i)
=======
def select_paginated_data(selected_table, engine, page=1, page_size=1000):
    offset = (page - 1) * page_size
    query = text(f"SELECT * FROM {selected_table} LIMIT :limit OFFSET :offset")
    
    with engine.connect() as conn:
        result = conn.execute(query, {'limit': page_size, 'offset': offset})
    
    print(f"{selected_table}의 데이터 (페이지 {page})")
    for row in result:
        print(row)

    return result.rowcount  # 반환된 행의 수를 반환
>>>>>>> Stashed changes:DummyGenerator/utils/db_utils.py
