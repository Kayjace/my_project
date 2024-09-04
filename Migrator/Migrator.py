import yaml
import mysql.connector
from pymongo import MongoClient
import datetime
import decimal
import os

#설정 파일 로드 > MySQL 연결 및 스키마 정보 추출 > MongoDB 연결 설정 > MySQL 데이터를 MongoDB 형식으로 변환 > 청크 단위로 데이터 마이그레이션 수행 > 마이그레이션 결과를 README 파일에 기록

# 설정 파일을 로드하는 함수
def load_config():
    # 현재 스크립트의 디렉토리 경로를 가져옴
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # 설정 파일의 경로를 생성
    config_path = os.path.join(script_dir, 'config', 'config.yaml')
    # YAML 파일을 열고 내용을 파이썬 객체로 변환하여 반환
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

# MySQL 테이블의 스키마를 가져오는 함수
def get_mysql_schema(cursor, table_name):
    # 테이블 구조를 조회하는 SQL 실행
    cursor.execute(f"DESCRIBE `{table_name}`")
    # 결과를 딕셔너리 형태로 변환하여 반환
    return {row['Field']: {'type': row['Type'], 'nullable': row['Null'] == 'YES', 'key': row['Key']} for row in cursor.fetchall()}

# MongoDB 컬렉션의 스키마를 추정하는 함수
def get_mongo_schema(collection):
    # 컬렉션에서 하나의 문서를 샘플로 가져옴
    sample = collection.find_one()
    # 샘플 문서의 각 필드 타입을 딕셔너리로 반환
    return {k: type(v).__name__ for k, v in sample.items()} if sample else {}

# MySQL 데이터 타입을 MongoDB 데이터 타입으로 매핑하는 함수
def map_data_type(mysql_type):
    type_mapping = {
        'int': 'NumberInt',
        'bigint': 'NumberLong',
        'float': 'NumberDecimal',
        'double': 'NumberDecimal',
        'datetime': 'Date',
        'varchar': 'String',
        'text': 'String',
        'blob': 'BinData'
    }
    # MySQL 타입에 해당하는 MongoDB 타입을 반환, 없으면 'String' 반환
    return type_mapping.get(mysql_type.lower().split('(')[0], 'String')

# MySQL 스키마를 MongoDB 스키마로 변환하는 함수
def transform_schema(mysql_schema):
    mongo_schema = {}
    for column, properties in mysql_schema.items():
        mongo_type = map_data_type(properties['type'])
        mongo_schema[column] = {
            'type': mongo_type,
            'required': not properties['nullable']
        }
    return mongo_schema

# README 파일에 추가할 내용을 생성하는 함수
def generate_readme_content(table_name, mysql_schema, mongo_schema):
    content = f"## {table_name}\n"
    content += "   - MySQL:\n"
    content += "     - Columns: " + ", ".join([f"{k} ({v['type']})" for k, v in mysql_schema.items()]) + "\n"
    content += "   - MongoDB:\n"
    content += "     - Fields: " + ", ".join([f"{k} ({v['type']})" for k, v in mongo_schema.items()]) + "\n\n"
    return content

# MySQL의 row 데이터를 MongoDB에 적합한 형태로 변환하는 함수
def transform_row(row):
    transformed_row = {}
    for k, v in row.items():
        if isinstance(v, (datetime.date, datetime.datetime)):
            transformed_row[k] = v.isoformat()
        elif isinstance(v, bytes):
            transformed_row[k] = v.decode('utf-8', errors='ignore')
        elif isinstance(v, decimal.Decimal):
            transformed_row[k] = float(v)  # Decimal을 float로 변환
        else:
            transformed_row[k] = v
    return transformed_row

# 데이터 마이그레이션을 수행하는 주요 함수
def migrate_data(source_db, source_table, target_db, target_collection):
    config = load_config()
    mysql_db = None
    mongo_client = None
    
    try:
        # MySQL 데이터베이스에 연결
        mysql_db = mysql.connector.connect(
            host=config['source']['host'],
            user=config['source']['user'],
            password=config['source']['password'],
            database=source_db
        )
        cursor = mysql_db.cursor(dictionary=True)

        # MongoDB에 연결
        mongo_client = MongoClient(config['target']['uri'])
        mongo_db = mongo_client[target_db]
        collection = mongo_db[target_collection]

        # MySQL 테이블의 스키마 가져오기
        mysql_schema = get_mysql_schema(cursor, source_table)

        # MySQL 스키마를 MongoDB 스키마로 변환
        mongo_schema = transform_schema(mysql_schema)

        # MySQL에서 데이터 가져오기
        cursor.execute(f"SELECT * FROM `{source_table}`")
        
        # 청크 단위로 데이터 처리 및 MongoDB에 삽입
        chunk_size = 1000
        inserted_count = 0
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            transformed_rows = [transform_row(row) for row in rows]
            collection.insert_many(transformed_rows)
            inserted_count += len(rows)
            print(f"Inserted {inserted_count} records...")

        print(f"Migrated {inserted_count} records from {source_table} to {target_collection}")

        return mysql_schema, mongo_schema

    except Exception as e:
        print(f"Error during migration: {e}")
        return {}, {}

    finally:
        # 데이터베이스 연결 종료
        if mysql_db:
            mysql_db.close()
        if mongo_client:
            mongo_client.close()

# README.md 파일을 업데이트하는 함수
def update_readme(content):
    try:
        with open('README.md', 'w') as f:
            f.write("# MySQL to MongoDB Migration\n\n")
            f.write("## Table Comparisons\n\n")
            f.write(content)
            f.write("\n## Migration Notes\n\n")
            f.write("- All integer IDs from MySQL are preserved in MongoDB, but MongoDB also adds its own `_id` field.\n")
            f.write("- MySQL's `datetime` type is converted to MongoDB's `date` type.\n")
            f.write("- Decimal fields in MySQL are converted to double in MongoDB.\n")
        print("README.md has been updated successfully.")
    except Exception as e:
        print(f"Error updating README.md: {e}")

# 메인 함수: 전체 마이그레이션 프로세스를 조정
def main():
    config = load_config()
    readme_content = ""

    # 설정 파일의 각 테이블에 대해 마이그레이션 수행
    for table in config['tables']:
        mysql_schema, mongo_schema = migrate_data(
            config['source']['database'],
            table['mysql_table'],
            config['target']['database'],
            table['mongo_collection']
        )
        if mysql_schema and mongo_schema:
            readme_content += generate_readme_content(table['mysql_table'], mysql_schema, mongo_schema)

    # README 파일 업데이트
    update_readme(readme_content)

# 스크립트가 직접 실행될 때만 main 함수 호출
if __name__ == "__main__":
    main()