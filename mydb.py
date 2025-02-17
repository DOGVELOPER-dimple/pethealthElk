import pymysql
from elasticsearch import Elasticsearch, helpers
import fastapi
import os
from dotenv import load_dotenv

load_dotenv('db.env')

connection = pymysql.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME"),
    port=int(os.getenv("DB_PORT"))
)
cursor = connection.cursor()

es = Elasticsearch(os.getenv("elastic_add"))

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

# 엘라스틱 벌크
helpers.bulk(es, actions)

print("벌크 성공")

cursor.close()
connection.close()


#배변 api

#이동거리 api

#급여량 api

#생리추적 api

#주간 리포트 api
