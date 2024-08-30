import yaml
import mysql.connector
from pymongo import MongoClient

def load_config():
    with open('./config/config.yaml', 'r') as f:
        return yaml.safe_load(f)

def get_mysql_schema(cursor, table_name):
    cursor.execute(f"DESCRIBE {table_name}")
    return {row[0]: row[1] for row in cursor.fetchall()}

def get_mongo_schema(collection):
    sample = collection.find_one()
    return {k: type(v).__name__ for k, v in sample.items()} if sample else {}

def generate_readme_content(table_name, mysql_schema, mongo_schema):
    content = f"## {table_name}\n"
    content += "   - MySQL:\n"
    content += "     - Columns: " + ", ".join([f"{k} ({v})" for k, v in mysql_schema.items()]) + "\n"
    content += "   - MongoDB:\n"
    content += "     - Fields: " + ", ".join([f"{k} ({v})" for k, v in mongo_schema.items()]) + "\n\n"
    return content

def migrate_data(source_db, source_table, target_db, target_collection):
    config = load_config()
    
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

    # MySQL에서 데이터 가져오기
    cursor.execute(f"SELECT * FROM {source_table}")
    rows = cursor.fetchall()

    # MongoDB에 데이터 삽입
    if rows:
        collection.insert_many(rows)
    
    # MongoDB 스키마 가져오기
    mongo_schema = get_mongo_schema(collection)

    print(f"Migrated {len(rows)} records from {source_table} to {target_collection}")

    mysql_db.close()
    mongo_client.close()

    return mysql_schema, mongo_schema

def update_readme(content):
    with open('README.md', 'w') as f:
        f.write("# MySQL to MongoDB Migration\n\n")
        f.write("## Table Comparisons\n\n")
        f.write(content)
        f.write("\n## Migration Notes\n\n")
        f.write("- All integer IDs from MySQL are preserved in MongoDB, but MongoDB also adds its own `_id` field.\n")
        f.write("- MySQL's `datetime` type is converted to MongoDB's `date` type.\n")
        f.write("- Decimal fields in MySQL are converted to double in MongoDB.\n")

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
        readme_content += generate_readme_content(table['mysql_table'], mysql_schema, mongo_schema)

    update_readme(readme_content)

if __name__ == "__main__":
    main()

# 개별 테이블 마이그레이션 예시:
# mysql_schema, mongo_schema = migrate_data('my_mysql_db', 'users', 'my_mongo_db', 'users_collection')
# update_readme(generate_readme_content('users', mysql_schema, mongo_schema))