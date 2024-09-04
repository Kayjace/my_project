import yaml
import mysql.connector
from pymongo import MongoClient
import datetime
import decimal
import os

def load_config():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config', 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def get_mysql_schema(cursor, table_name):
    cursor.execute(f"DESCRIBE `{table_name}`")
    return {row['Field']: {'type': row['Type'], 'nullable': row['Null'] == 'YES', 'key': row['Key']} for row in cursor.fetchall()}

def get_mongo_schema(collection):
    sample = collection.find_one()
    return {k: type(v).__name__ for k, v in sample.items()} if sample else {}

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
    return type_mapping.get(mysql_type.lower().split('(')[0], 'String')

def transform_schema(mysql_schema):
    mongo_schema = {}
    for column, properties in mysql_schema.items():
        mongo_type = map_data_type(properties['type'])
        mongo_schema[column] = {
            'type': mongo_type,
            'required': not properties['nullable']
        }
    return mongo_schema

def generate_readme_content(table_name, mysql_schema, mongo_schema):
    content = f"## {table_name}\n"
    content += "   - MySQL:\n"
    content += "     - Columns: " + ", ".join([f"{k} ({v['type']})" for k, v in mysql_schema.items()]) + "\n"
    content += "   - MongoDB:\n"
    content += "     - Fields: " + ", ".join([f"{k} ({v['type']})" for k, v in mongo_schema.items()]) + "\n\n"
    return content

def transform_row(row):
    transformed_row = {}
    for k, v in row.items():
        if isinstance(v, (datetime.date, datetime.datetime)):
            transformed_row[k] = v.isoformat()
        elif isinstance(v, bytes):
            transformed_row[k] = v.decode('utf-8', errors='ignore')
        elif isinstance(v, decimal.Decimal):
            transformed_row[k] = float(v)  # Convert Decimal to float
        else:
            transformed_row[k] = v
    return transformed_row

def migrate_data(source_db, source_table, target_db, target_collection):
    config = load_config()
    mysql_db = None
    mongo_client = None
    
    try:
        # MySQL 연결
        mysql_db = mysql.connector.connect(
            host=config['source']['host'],
            user=config['source']['user'],
            password=config['source']['password'],
            database=source_db
        )
        cursor = mysql_db.cursor(dictionary=True)

        # MongoDB 연결
        mongo_client = MongoClient(config['target']['uri'])
        mongo_db = mongo_client[target_db]
        collection = mongo_db[target_collection]

        # MySQL 스키마 가져오기
        mysql_schema = get_mysql_schema(cursor, source_table)

        # MySQL 스키마 변환
        mongo_schema = transform_schema(mysql_schema)

        # MySQL에서 데이터 가져오기
        cursor.execute(f"SELECT * FROM `{source_table}`")
        
        # 청크 단위로 처리
        chunk_size = 1000
        inserted_count = 0
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            # MongoDB에 데이터 삽입
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
        if mysql_db:
            mysql_db.close()
        if mongo_client:
            mongo_client.close()

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

    update_readme(readme_content)

if __name__ == "__main__":
    main()