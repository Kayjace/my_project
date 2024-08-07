import yaml
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from faker import Faker

# 설정 파일 로드
def load_config(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

# 데이터베이스 설정
DATABASE_URL = 'mysql+pymysql://root:208300@localhost/dummyDB'
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

# Faker 객체 생성
fake = Faker()

# 테이블에 더미 데이터 삽입
def generate_dummy_data(table, num_rows, truncate):
    if truncate:
        session.execute(f'TRUNCATE TABLE {table.name}')  # 테이블을 truncate하여 id를 초기화
    else:
        # 기존 데이터 개수 확인
        existing_count = session.query(table).count()
        # 추가해야 할 데이터 개수 계산
        num_rows = min(max(1000, num_rows + existing_count), 20000) - existing_count

    columns = [col.name for col in table.columns if col.name != 'id']  # 'id'를 제외한 모든 컬럼
    for _ in range(num_rows):
        values = [fake.word() if 'name' in col else fake.text() for col in columns]
        data = dict(zip(columns, values))
        session.execute(table.insert().values(data))
    session.commit()

# 테이블 생성
def create_table_from_config(table_config, metadata):
    columns = [Column('id', Integer, primary_key=True, autoincrement=True)]
    num_columns = table_config.get('num_columns', 1)  # 기본 1개의 컬럼 추가
    for i in range(num_columns):
        columns.append(Column(f'column{i+1}', String(255)))  # 컬럼 이름은 column1, column2, ...
    
    table = Table(
        table_config['name'],
        metadata,
        *columns,
        extend_existing=True
    )
    metadata.create_all(engine)
    return table

def main():
    config = load_config('config.yaml')
    metadata = MetaData()

    # 메타데이터에 기존 테이블을 로드
    metadata.reflect(bind=engine)

    for table_config in config['tables']:
        table_name = table_config['name']
        num_rows = table_config['rows']
        truncate = table_config.get('truncate', False)

        # 데이터 개수가 범위 내에 있는지 확인
        if num_rows < 1000 or num_rows > 20000:
            print(f"테이블 {table_name}의 데이터 개수 {num_rows}이 범위를 초과합니다. 1000에서 20000 사이의 값만 허용됩니다.")
            continue

        # 테이블이 존재하는지 확인
        if table_name in metadata.tables:
            table = metadata.tables[table_name]
        else:
            # 테이블이 없으면 새로 생성
            print(f"Table {table_name} not found in the database. Creating it...")
            table = create_table_from_config(table_config, metadata)

        generate_dummy_data(table, num_rows, truncate)

if __name__ == "__main__":
    main()