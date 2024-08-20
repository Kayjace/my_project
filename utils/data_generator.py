import random
import string
from faker import Faker

def generate_random_string(length):
    # 문자와 숫자를 포함한 랜덤 문자열 생성
    characters = string.ascii_letters + string.digits
    return ''.join(random.choices(characters, k=length))

def generate_dummy_data(data_dict, fake):
    # 데이터 타입에 따라 더미 데이터 생성
    data_type = data_dict.get('data_type')
    
    if data_dict.get('autoincrement'):
        return None

    fake_instance = fake.unique if data_dict.get('unique') or data_dict.get('primary_key') else fake

    # 데이터 생성
    data_generators = {
        'VARCHAR': lambda: fake_instance.pystr(max_chars=data_dict['data_length']),
        'CHAR': lambda: fake_instance.pystr(max_chars=data_dict['data_length']),
        'TEXT': lambda: fake_instance.paragraph(),
        'INTEGER': lambda: fake_instance.pyint(),
        'MEDIUMINT': lambda: fake_instance.pyint(),
        'SMALLINT': lambda: fake_instance.pyint(),
        'FLOAT': lambda: fake_instance.pyfloat(left_digits=2, right_digits=2, positive=True),
        'DOUBLE': lambda: fake_instance.pyfloat(left_digits=2, right_digits=2, positive=True),
        'DECIMAL': lambda: fake_instance.pydecimal(left_digits=data_dict['data_left_digits'], right_digits=data_dict['data_right_digits'], positive=True),
        'DATE': lambda: fake_instance.date(),
        'TIME': lambda: fake_instance.time(),
        'DATETIME': lambda: fake_instance.date_time(),
        'TIMESTAMP': lambda: fake_instance.date_time(),
        'YEAR': lambda: fake_instance.year(),
        'BOOLEAN': lambda: fake_instance.boolean(),
        'ENUM': lambda: fake_instance.random_element(elements=data_dict['data_options']),
        'BLOB': lambda: fake_instance.binary(length=10),
        'BINARY': lambda: fake_instance.binary(length=10),
    }

    return data_generators.get(data_type, lambda: None)()

def create_dummy_data_list(table_name, dummy_num, table_detail, engine):
    datas = []
    fake = Faker()

    for _ in range(dummy_num):
        temp = {}
        for row in table_detail[table_name]['details']:
            data_dict = row.copy()
            if data_dict.get('autoincrement'):
                continue

            data_type = data_dict.pop('type', None)
            if data_type:
                data_dict['data_type'] = data_type.__class__.__name__
                if data_dict['data_type'] in ['CHAR', 'VARCHAR']:
                    data_dict['data_length'] = data_type.length
                elif data_dict['data_type'] == 'DECIMAL':
                    data_dict['data_left_digits'] = data_type.precision - data_type.scale
                    data_dict['data_right_digits'] = data_type.scale
                elif data_dict['data_type'] == 'ENUM':
                    data_dict['data_options'] = data_type.enums
                elif data_dict['data_type'] == 'TINYINT':
                    data_dict['data_display_width'] = data_type.display_width

            dummy_data = generate_dummy_data(data_dict, fake)
            temp[data_dict['name']] = dummy_data
        datas.append(temp)

    return datas