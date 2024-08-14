import sqlalchemy as db
from sqlalchemy import create_engine, inspect, text, MetaData
from faker import Faker
import yaml

def truncate_table(selected_table, engine):
    conn = engine.connect()
    conn.execute(text(f"TRUNCATE TABLE {selected_table};"))
    conn.commit()
    print(f"{selected_table} 내 데이터들을 삭제했습니다.")
        
def select_all_data(selected_table, engine):
    with engine.begin() as conn:
        result = conn.execute(text(f"SELECT * FROM {selected_table};"))
    print(f"{selected_table}의 데이터")
    for i in result:
        print(i)

def get_table_detail(engine, tables):
    inspector = inspect(engine)
    table_detail = {}

    for table in tables:

        columns = inspector.get_columns(table)
        primary_keys = set(inspector.get_pk_constraint(table)['constrained_columns'])
        unique_constraints = inspector.get_unique_constraints(table)
        unique_column_names = set(
            column_name
            for constraint in unique_constraints
            for column_name in constraint['column_names']
        )

        # MySQL INFORMATION_SCHEMA를 사용하여 AUTO_INCREMENT 확인
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

            # PRIMARY KEY 확인
            column['primary_key'] = column_name in primary_keys

            # 유니크 제약 조건 확인
            column['unique'] = column_name in unique_column_names

            # auto_increment 확인
            column['autoincrement'] = column_name in auto_increment_columns

            temp.append(column)

        table_detail[table] = {'details': temp}

    return table_detail

def get_existing_unique_values(table_name, column_name, engine):
    with engine.connect() as conn:
        query = text(f"SELECT {column_name} FROM {table_name};")
        result = conn.execute(query)
        return set(row[0] for row in result)

def generate_dummy_data(data_dict, fake, existing_values=None, auto_increment_start=1, auto_increment_counter=None):
    if existing_values is None:
        existing_values = set()

    if data_dict.get('autoincrement'):
        if auto_increment_counter is None:
            raise ValueError("auto_increment_counter must be provided for auto_increment columns")
        value = auto_increment_start + auto_increment_counter[0]
        auto_increment_counter[0] += 1
        return value

    # 다른 데이터 타입에 대한 처리
    if data_dict['data_type'] == 'VARCHAR':
        new_value = fake.pystr(max_chars=data_dict['data_length'])
    elif data_dict['data_type'] == 'CHAR':
        new_value = fake.pystr(max_chars=data_dict['data_length'])
    elif data_dict['data_type'] == 'TEXT':
        new_value = fake.paragraph()
    elif data_dict['data_type'] in ['INTEGER', 'MEDIUMINT', 'SMALLINT']:
        new_value = fake.pyint()
    elif data_dict['data_type'] in ['FLOAT', 'DOUBLE']:
        new_value = fake.pyfloat(left_digits=2, right_digits=2, positive=True)
    elif data_dict['data_type'] == 'DECIMAL':
        new_value = fake.pydecimal(left_digits=data_dict['data_left_digits'], right_digits=data_dict['data_right_digits'], positive=True)
    elif data_dict['data_type'] == 'DATE':
        new_value = fake.date()
    elif data_dict['data_type'] == 'TIME':
        new_value = fake.time()
    elif data_dict['data_type'] in ['DATETIME', 'TIMESTAMP']:
        new_value = fake.date_time()
    elif data_dict['data_type'] == 'YEAR':
        new_value = fake.year()
    elif data_dict['data_type'] == 'BOOLEAN':
        new_value = fake.boolean()
    elif data_dict['data_type'] == 'ENUM':
        new_value = fake.random_element(elements=data_dict['data_options'])
    elif data_dict['data_type'] in ['BLOB', 'BINARY']:
        new_value = fake.binary(length=10)
    else:
        return None

    # 유니크 제약 조건 처리
    if data_dict.get('unique') and new_value in existing_values:
        return generate_dummy_data(data_dict, fake, existing_values)
    return new_value

def create_dummy_data_list(table_name, dummy_num, table_detail, engine):
    datas = []
    fake = Faker()

    existing_unique_values = {}
    auto_increment_counters = {}
    primary_key_values = set()  # Primary key 값을 저장할 집합

    for column in table_detail[table_name]['details']:
        if column.get('unique') and not column.get('autoincrement'):
            existing_unique_values[column['name']] = get_existing_unique_values(table_name, column['name'], engine)
        if column.get('autoincrement'):
            auto_increment_counters[column['name']] = [0]

    for _ in range(dummy_num):
        temp = {}
        for row in table_detail[table_name]['details']:
            data_dict = row.copy()
            if data_dict.get('autoincrement'):
                auto_increment_start = 1
                auto_increment_counter = auto_increment_counters.get(data_dict['name'])
                dummy_data = generate_dummy_data(data_dict, fake, auto_increment_counter=auto_increment_counter, auto_increment_start=auto_increment_start)
                temp[data_dict['name']] = dummy_data
            else:
                data = row['type']
                data_dict['data_type'] = data.__class__.__name__
                if data_dict['data_type'] in ['CHAR', 'VARCHAR']:
                    data_dict['data_length'] = data.length
                elif data_dict['data_type'] == 'DECIMAL':
                    data_dict['data_left_digits'] = data.precision - data.scale
                    data_dict['data_right_digits'] = data.scale
                elif data_dict['data_type'] == 'ENUM':
                    data_dict['data_options'] = data.enums
                elif data_dict['data_type'] == 'TINYINT':
                    data_dict['data_display_width'] = data.display_width
                del data_dict['type']

                existing_values = existing_unique_values.get(data_dict['name'])
                dummy_data = generate_dummy_data(data_dict, fake, existing_values)

                # Primary key 컬럼의 경우 중복값 확인
                if data_dict.get('primary_key') and dummy_data in primary_key_values:
                    break  # 중복값이 발견되면 루프를 종료
                if data_dict.get('primary_key'):
                    primary_key_values.add(dummy_data)
                    
                if dummy_data == 'unique_constraints':
                    return datas
                temp[data_dict['name']] = dummy_data
        datas.append(temp)

    return datas

def insert_dummy_data(selected_table, metadata, engine, data):
    table = db.Table(selected_table, metadata, autoload=True, autoload_with=engine)
    insert_num = 0
    with engine.connect() as conn:
        with conn.begin():
            for d in data:
                try:
                    query = table.insert().values(d)
                    conn.execute(query)
                    insert_num += 1
                except Exception as e:

                    continue
    if insert_num == 0:
        print("제약조건으로 인해 데이터를 더 추가할 수 없습니다.")
    else:
        print(f"{insert_num}개의 데이터를 {selected_table}에 넣었습니다.")

def load_settings(file_path):
    with open(file_path, 'r') as file:
        settings = yaml.safe_load(file)
    return settings

def main():
    command_data = load_settings('settings.yaml')

    BASE_DATABASE_URL = 'mysql+pymysql://root:208300@localhost/'
    DATABASE_NAME = 'airportdb'
    connection_string = f"{BASE_DATABASE_URL}{DATABASE_NAME}"
    
    engine = create_engine(connection_string, echo=False)
    metadata = MetaData()
    metadata.reflect(bind=engine)
    inspector = inspect(engine)

    tables = inspector.get_table_names()
    table_detail = get_table_detail(engine, tables)
    table_names = command_data.get('table_names', [])
    dummy_nums = command_data.get('dummy_nums', {})

    for table_name in table_names:
        if table_name not in tables:
            print(f'{table_name} 테이블은 데이터베이스에 존재하지 않는 테이블 이름입니다.')
            continue
        
        dummy_num = dummy_nums.get(table_name, 1000)  # 기본값 1000
        truncate_table(table_name, engine)
        datas = create_dummy_data_list(table_name, dummy_num, table_detail, engine)
        insert_dummy_data(table_name, metadata, engine, datas)
        select_all_data(table_name, engine)

if __name__ == "__main__":
    main()