import pymysql
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search
import os
from dotenv import load_dotenv
from fastapi import FastAPI

app = FastAPI()

load_dotenv('db.env')


@app.get("/bulk_data")
async def bulk():
    connection = pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        port=int(os.getenv("DB_PORT"))
    )
    cursor = connection.cursor()
    es = Elasticsearch(os.getenv("elastic_add"))
    
    # 기존 인덱스 삭제 후 생성
    if es.indices.exists(index="dog_info"):
        es.indices.delete(index="dog_info")
    
    es.indices.create(index="dog_info", body={
        "settings": {"number_of_shards": 1, "number_of_replicas": 1},
        "mappings": {
            "properties": {
                "dog": {
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {"type": "text"},
                        "age": {"type": "integer"},
                        "blood_type": {"type": "keyword"},
                        "gender": {"type": "keyword"},
                        "heartworm_vaccination_date": {"type": "date"},
                        "height": {"type": "float"},
                        "image": {"type": "keyword"},
                        "is_neutered": {"type": "boolean"},
                        "leg_length": {"type": "float"},
                        "menstruation_cycle": {"type": "integer"},
                        "menstruation_duration": {"type": "integer"},
                        "menstruation_start_date": {"type": "date"},
                        "puppy_species": {"type": "keyword"},
                        "recent_checkup_date": {"type": "date"},
                        "registration_number": {"type": "keyword"},
                        "weight": {"type": "float"},
                        "owner_name": {"type": "text"},
                        "owner_email": {"type": "keyword"}
                    }
                },
                "poop_logs": {
                    "type": "nested",
                    "properties": {
                        "poop_time": {"type": "date"},
                        "poop_type": {"type": "keyword"}
                    }
                },
                "food_intake": {
                    "type": "nested",
                    "properties": {
                        "amount": {"type": "float"},
                        "intake_time": {"type": "date"}
                    }
                },
                "walks": {
                    "type": "nested",
                    "properties": {
                        "distance": {"type": "float"},
                        "dog_consumable_calories": {"type": "float"},
                        "end_time": {"type": "date"},
                        "start_time": {"type": "date"},
                        "user_consumable_calories": {"type": "float"}
                    }
                },
                "walk_activities": {
                    "type": "nested",
                    "properties": {
                        "bowel_movements": {"type": "integer"},
                        "distance": {"type": "float"},
                        "timestamp": {"type": "date"}
                    }
                }
            }
        }
    })

    # 개별 반려견 정보 가져오기
    cursor.execute("""
        SELECT d.id, d.name, d.age, d.blood_type, d.gender, d.heartworm_vaccination_date, 
               d.height, d.image, d.is_neutered, d.leg_length, d.menstruation_cycle, 
               d.menstruation_duration, d.menstruation_start_date, d.puppy_species, 
               d.recent_checkup_date, d.registration_number, d.weight, u.name AS owner_name, 
               u.email AS owner_email
        FROM dog d
        JOIN user u ON d.user_id = u.id
    """)

    dogs = cursor.fetchall()
    dog_columns = [col[0] for col in cursor.description]
    
    actions = []

    for dog in dogs:
        dog_data = dict(zip(dog_columns, dog))
        # is_neutered 필드 변환: bytes -> boolean
        dog_data["is_neutered"] = True if dog_data["is_neutered"] == b'\x01' else False

        # 해당 반려견의 추가 데이터 (배변, 급여량, 산책 등) 가져오기
        dog_id = dog_data["id"]

        # 배변 기록
        cursor.execute("SELECT poop_time, poop_type FROM dog_poop_log WHERE dog_id = %s", (dog_id,))
        poop_logs = [dict(zip([col[0] for col in cursor.description], row)) for row in cursor.fetchall()]

        # 급여량 기록
        cursor.execute("SELECT amount, intake_time FROM food_intake WHERE dog_id = %s", (dog_id,))
        food_intake = [dict(zip([col[0] for col in cursor.description], row)) for row in cursor.fetchall()]

        # 산책 기록
        cursor.execute("""
            SELECT distance, dog_consumable_calories, end_time, start_time, user_consumable_calories 
            FROM walk WHERE dog_id = %s
        """, (dog_id,))
        walks = [dict(zip([col[0] for col in cursor.description], row)) for row in cursor.fetchall()]

        # 산책 활동 기록
        cursor.execute("""
            SELECT bowel_movements, distance, timestamp 
            FROM walk_activity WHERE dog_id = %s
        """, (dog_id,))
        walk_activities = [dict(zip([col[0] for col in cursor.description], row)) for row in cursor.fetchall()]

        # Elasticsearch 데이터 변환
        doc = {
            "_index": "dog_info",
            "_id": dog_data["id"],
            "_source": {
                "dog": dog_data,
                "poop_logs": poop_logs,
                "food_intake": food_intake,
                "walks": walks,
                "walk_activities": walk_activities
            }
        }
        actions.append(doc)

    # Elasticsearch에 데이터 삽입
    if actions:
        helpers.bulk(es, actions)

    cursor.close()
    connection.close()
    
    return {"message": "벌크 성공"}


# 배변 API
@app.get("/poo/{dog_id}")
async def get_poop_data(dog_id: int, field_name="poop_logs", size=10000):
    es = Elasticsearch(os.getenv("elastic_add"))
    
    s = Search(using=es, index="dog_info").query("match", **{"dog.id": dog_id}).source([field_name]).extra(size=size)
    
    response = s.execute()
    poop_data = []
    for hit in response:
        if field_name in hit:
            poop_data.extend(hit[field_name])
    return {"dog_id": dog_id, "poop_logs": poop_data}


# 산책 기록 API
@app.get("/walk/{dog_id}")
async def get_walks_data(dog_id: int, field_name="walks", size=10000):
    es = Elasticsearch(os.getenv("elastic_add"))
    
    s = Search(using=es, index="dog_info").query("match", **{"dog.id": dog_id}).source([field_name]).extra(size=size)
    
    response = s.execute()
    walk_data = []  # 변수 이름 일관성 유지
    for hit in response:
        if field_name in hit:
            walk_data.extend(hit[field_name])
    return {"dog_id": dog_id, "walks": walk_data}


# 급여량 기록 API
@app.get("/food_intake/{dog_id}")
async def get_food_intake_data(dog_id: int, field_name="food_intake", size=10000):
    es = Elasticsearch(os.getenv("elastic_add"))
    
    s = Search(using=es, index="dog_info").query("match", **{"dog.id": dog_id}).source([field_name]).extra(size=size)
    
    response = s.execute()
    food_intake_data = []  # 변수 이름 수정
    for hit in response:
        if field_name in hit:
            food_intake_data.extend(hit[field_name])
    return {"dog_id": dog_id, "food_intake": food_intake_data}


# 생리 추적 API
@app.get("/menstruation/{dog_id}")
async def get_menstruation_data(dog_id: int, size=10000):
    es = Elasticsearch(os.getenv("elastic_add"))

    fields = ["dog.menstruation_cycle", "dog.menstruation_duration", "dog.menstruation_start_date"]
    
    s = Search(using=es, index="dog_info").query("match", **{"dog.id": dog_id}).source(fields).extra(size=size)
    
    response = s.execute()
    menstruation_data = []
    
    for hit in response:
        menstruation_info = {
            "menstruation_cycle": hit["dog"]["menstruation_cycle"] if "dog" in hit and "menstruation_cycle" in hit["dog"] else None,
            "menstruation_duration": hit["dog"]["menstruation_duration"] if "dog" in hit and "menstruation_duration" in hit["dog"] else None,
            "menstruation_start_date": hit["dog"]["menstruation_start_date"] if "dog" in hit and "menstruation_start_date" in hit["dog"] else None
        }
        menstruation_data.append(menstruation_info)

    return {
        "dog_id": dog_id,
        "menstruation_info": menstruation_data
    }


# 주간 리포트 API (예시로 간단한 메시지 반환)
@app.get("/week_report/{dog_id}")
async def get_week_report(dog_id: int):
    # 주간 리포트 관련 로직을 추가하세요.
    return {"dog_id": dog_id, "report": "주간 리포트 기능은 아직 구현되지 않았습니다."}



