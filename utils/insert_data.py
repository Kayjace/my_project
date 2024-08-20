import sqlalchemy as db
import logging

# 로깅 설정
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)  # ERROR 레벨 이상의 로그만 기록

# 콘솔 핸들러 (로그를 콘솔에 출력)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# 파일 핸들러 (로그를 파일에 기록)
file_handler = logging.FileHandler('error.log')
file_handler.setLevel(logging.ERROR)
file_handler.setFormatter(formatter)

# 핸들러를 로거에 추가
logger.addHandler(console_handler)
logger.addHandler(file_handler)

#더미데이터를 삽입
def insert_dummy_data(selected_table, metadata, engine, data):
    table = db.Table(selected_table, metadata, autoload=True, autoload_with=engine)
    insert_num = 0
    #데이터가 존재하지 않는다면
    if not data:
        logger.error("No data provided for insertion.")
        print("삽입할 데이터가 없습니다.")
        return
    with engine.connect() as conn:
        with conn.begin():
            for d in data:
                try:
                    query = table.insert().values(d)
                    conn.execute(query)
                    insert_num += 1
                except Exception as e:
                    logger.error(f"Failed to insert data into {selected_table}: {e}", exc_info=True)
                    continue
    if insert_num == 0:
        print("제약조건으로 인해 데이터를 더 추가할 수 없습니다.")
    else:
        print(f"{insert_num}개의 데이터를 {selected_table}에 넣었습니다.")