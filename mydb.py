import pymysql
from elasticsearch import Elasticsearch, helpers
import os
from dotenv import load_dotenv
from fastapi import FastAPI

app = FastAPI()

load_dotenv('db.env')


#엘라스틱 삽입 
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
    
    if es.indices.exists(index="dog_info"):
        es.indices.delete(index="dog_info")
    
    es.indices.create(index="dog_info", body={
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 1
        },
        "mappings": {
            "properties": {
                "dog": {
                    "properties": {
                    "id": { "type": "integer" },
                    "name": { "type": "text" },
                    "age": { "type": "integer" },
                    "blood_type": { "type": "keyword" },
                    "gender": { "type": "keyword" },
                    "heartworm_vaccination_date": { "type": "date" },
                    "height": { "type": "float" },
                    "image": { "type": "keyword" },
                    "is_neutered": { "type": "boolean" },
                    "leg_length": { "type": "float" },
                    "menstruation_cycle": { "type": "integer" },
                    "menstruation_duration": { "type": "integer" },
                    "menstruation_start_date": { "type": "date" },
                    "puppy_species": { "type": "keyword" },
                    "recent_checkup_date": { "type": "date" },
                    "registration_number": { "type": "keyword" },
                    "weight": { "type": "float" },
                    "owner_name": { "type": "text" },
                    "owner_email": { "type": "keyword" }
                    }
                },
                "poop_logs": {
                    "type": "nested",
                    "properties": {
                    "poop_time": { "type": "date" },
                    "poop_type": { "type": "keyword" }
                    }
                },
                "food_intake": {
                    "type": "nested",
                    "properties": {
                    "amount": { "type": "float" },
                    "intake_time": { "type": "date" }
                    }
                },
                "walks": {
                    "type": "nested",
                    "properties": {
                    "distance": { "type": "float" },
                    "dog_consumable_calories": { "type": "float" },
                    "end_time": { "type": "date" },
                    "start_time": { "type": "date" },
                    "user_consumable_calories": { "type": "float" }
                    }
                },
                "walk_activities": {
                    "type": "nested",
                    "properties": {
                    "bowel_movements": { "type": "integer" },
                    "distance": { "type": "float" },
                    "timestamp": { "type": "date" }
                    }
                }
            }
        }
    }
    )

    cursor.execute("""
        SELECT d.id, d.name, d.age, d.gender, d.weight, d.puppy_species, u.name as owner_name, u.email as owner_email
        FROM dog d
        JOIN user u ON d.user_id = u.id
    """)

    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    actions = [
        {
            "_index": "dog_info",
            "_id": row[0],  # id 값을 엘라스틱 id
            "_source": dict(zip(columns, row))
        }
        for row in rows
    ]
    helpers.bulk(es, actions)
    cursor.close()
    connection.close()
    return {"message": "벌크 성공"}


#배변 api
@app.get("/poo/{dog_id}")
async def deposit(dog_id: int):
    return 

#이동거리 api
@app.get("/walk/{dog_id}")
async def deposit(dog_id: int):
    return 

#급여량 api
@app.get("/deposit/{dog_id}")
async def deposit(dog_id: int):
    return 

#생리추적 api
@app.get("/deposit/{dog_id}")
async def deposit(dog_id: int):
    return 

#주간 리포트 api
@app.get("/deposit/{dog_id}")
async def deposit(dog_id: int):
    return 



