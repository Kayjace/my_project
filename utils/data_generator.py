# data_generator.py
import random, string
from faker import Faker


def generate_random_string(length):
    # 문자와 숫자를 포함한 랜덤 문자열 생성
    characters = string.ascii_letters + string.digits
    return ''.join(random.choices(characters, k=length))

def generate_dummy_data(data_dict, fake):
    if data_dict.get('autoincrement'):
        return 
    
    if data_dict.get('unique') or data_dict.get('primary_key'):
        fake_instance = fake.unique
    else: fake_instance=fake

    # 다른 데이터 타입에 대한 처리
    if data_dict['data_type'] == 'VARCHAR':
        new_value = fake_instance.pystr(max_chars=data_dict['data_length'])
    elif data_dict['data_type'] == 'CHAR':
         new_value =fake_instance.pystr(max_chars=data_dict['data_length'])
    elif data_dict['data_type'] == 'TEXT':
        new_value = fake_instance.paragraph()
    elif data_dict['data_type'] in ['INTEGER', 'MEDIUMINT', 'SMALLINT']:
        new_value = fake_instance.pyint()
    elif data_dict['data_type'] in ['FLOAT', 'DOUBLE']:
        new_value = fake_instance.pyfloat(left_digits=2, right_digits=2, positive=True)
    elif data_dict['data_type'] == 'DECIMAL':
        new_value = fake_instance.pydecimal(left_digits=data_dict['data_left_digits'], right_digits=data_dict['data_right_digits'], positive=True)
    elif data_dict['data_type'] == 'DATE':
        new_value = fake_instance.date()
    elif data_dict['data_type'] == 'TIME':
        new_value = fake_instance.time()
    elif data_dict['data_type'] in ['DATETIME', 'TIMESTAMP']:
        new_value = fake_instance.date_time()
    elif data_dict['data_type'] == 'YEAR':
        new_value = fake_instance.year()
    elif data_dict['data_type'] == 'BOOLEAN':
        new_value = fake_instance.boolean()
    elif data_dict['data_type'] == 'ENUM':
        new_value = fake_instance.random_element(elements=data_dict['data_options'])
    elif data_dict['data_type'] in ['BLOB', 'BINARY']:
        new_value = fake_instance.binary(length=10)
    else:
        return None

    return new_value

def create_dummy_data_list(table_name, dummy_num, table_detail, engine):
    datas = []
    fake = Faker()

    for _ in range(dummy_num):
        temp = {}
        for row in table_detail[table_name]['details']:
            data_dict = row.copy()
            if data_dict.get('autoincrement'):
                continue
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

            dummy_data = generate_dummy_data(data_dict, fake)
            temp[data_dict['name']] = dummy_data
        datas.append(temp)

    return datas