from sqlalchemy import inspect, text
import os

#DB이름들의 리스트를 받아서, config 폴더에 해당 db이름과 같은 yaml파일이 존재하는지 체크.
def check_yaml_exists(db_names):
    missing_yaml_files = []
    for db_name in db_names:
        yaml_filename = f'config/{db_name}.yaml'
        if not os.path.exists(yaml_filename):
            missing_yaml_files.append(db_name)
    return missing_yaml_files

#엔진 연결시 테이블 정보를 가져옴.
def get_table_detail(engine, tables):
    inspector = inspect(engine)
    table_detail = {}
    #프라이머리 키, 유니크 제약조건 확인
    for table in tables:
        columns = inspector.get_columns(table)
        primary_keys = set(inspector.get_pk_constraint(table)['constrained_columns'])
        unique_constraints = inspector.get_unique_constraints(table)
        unique_column_names = set(
            column_name
            for constraint in unique_constraints
            for column_name in constraint['column_names']
        )
        #AUTO_INCREMENT 쿼리
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
        #프라이머리 키, 유니크, 오토인크리먼트 조건이 있는 컬럼 디테일을 리스트화
        for column in columns:
            column_name = column['name']
            column['primary_key'] = column_name in primary_keys
            column['unique'] = column_name in unique_column_names
            column['autoincrement'] = column_name in auto_increment_columns
            temp.append(column)
        table_detail[table] = {'details': temp}
    return table_detail

#테이블을 초기화
def truncate_table(selected_table, engine):
    conn = engine.connect()
    conn.execute(text(f"TRUNCATE TABLE {selected_table};"))
    conn.commit()
    print(f"{selected_table} 내 데이터들을 삭제했습니다.")

#select all data
def select_paginated_data(selected_table, engine):
    with engine.begin() as conn:
        result = conn.execute(text(f"SELECT * FROM {selected_table};"))
    print(f"{selected_table}의 데이터")
    for i in result:
        print(i)