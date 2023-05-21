import json
import pandas as pd
from db_connection import DbConnection
import redis


def derialise():
    conn = DbConnection.database_connection()
    mycursor = conn.cursor(dictionary=True)
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    mycursor.execute("SELECT * FROM icg_billing LIMIT 2")
    results = mycursor.fetchall()
    json_obj_list = []

    try:
        for data in results:
            id = data['id']
            msisdn = data['msisdn']
            productID = data['productID']
            message = data['message']
            shortCode = data['shortCode']
            category = data['category']

            obj_data = {
                "id": id,
                "msisdn": msisdn,
                "productID": productID,
                "message": message,
                "shortCode": shortCode,
                "category": category
            }
            json_obj_list.append(obj_data)

        rs = json.dumps(json_obj_list)
        # print(json_obj_list)

        for dt in json_obj_list:
            key = f"user:{dt['id']}"
            values = json.dumps(dt)
            r.set(key, values)

            rst = r.get(key)
            # print(rst)

        print('stagging successfuly in redis ....')

    except Exception as e:
        print(e)


def searchData():
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    id = 3075
    searched_key = f"user:{id}"

    try:
        rs = r.get(searched_key)

        if not rs:
            print('no such data in redis')
        else:
            data = json.loads(rs)
            msisdn = data['msisdn']
            print(msisdn)

            deleted_data = r.delete(searched_key)

            if deleted_data == 0:
                print('data not deleted')
            else:
                print('data delete successfuly')

    except Exception as e:
        print(e)


def setData():
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    data = [
        {'id': '1', 'name': 'Alice', 'age': '30', 'gender': 'female'},
        {'id': '2', 'name': 'Bob', 'age': '25', 'gender': 'male'},
        {'id': '3', 'name': 'Charlie', 'age': '35', 'gender': 'male'},
        {'id': '4', 'name': 'Diana', 'age': '27', 'gender': 'female'},
    ]
    try:
        for item in data:
            r.sadd('user', json.dumps(item))
            r.sadd('user:age:' + item['age'], json.dumps(item))
            r.sadd('user:gender:' + item['gender'], json.dumps(item))

        members = r.smembers('user')

        for member in members:
            rs = json.loads(member)
            print(rs)
    except Exception as e:
        print(e)


def searchSetData():
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    try:
        result = r.sinter('user', 'user:age:31', 'user:gender:female')

        if not result:
            print('data not found...')
        else:
            for data in result:
                rs = json.loads(data)
                print('searched data is:', rs)

        deleted_data = r.delete('user', 'user:age:30', 'user:gender:female')

        if deleted_data == 0:
            print('data not deleted......')
        else:
            print('delete successfuly....')

    except Exception as e:
        print(e)


# derialise()
# searchData()
setData()
# searchSetData()
