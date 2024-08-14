# data_generator.py
from faker import Faker
from .db_utils import get_existing_unique_values

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
    primary_key_values = set()

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
                    break
                if data_dict.get('primary_key'):
                    primary_key_values.add(dummy_data)

                temp[data_dict['name']] = dummy_data
        datas.append(temp)

    return datas