import uuid
import json
import pandas as pd
from db_connection import DbConnection
import redis

class Stagging:
    conn = DbConnection.database_connection()
    mycursor = conn.cursor(dictionary=True)
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    def store_data(self):
        self.mycursor.execute("SELECT * FROM icg_billing LIMIT 2")
        results = self.mycursor.fetchall()

        for data in results:
            try:
                key = str(uuid.uuid4())
                values = data[1:]
                self.r.hmset(key, {'col{}'.format(i): json.dumps(v) for i, v in enumerate(values)})
                print('Staging successful...')
            except Exception as e:
                print(e)
    
    def fetch_data(self):
        cursor = '0'
        data = []

        while cursor != 0:
            cursor, keys = self.r.scan(cursor=cursor, count=1000)
            for key in keys:
                try:
                    row = self.r.hgetall(key)
                    row = {k.decode('utf-8'): json.loads(v.decode('utf-8')) for k, v in row.items()}
                    data.append(row)
                except Exception as e:
                    print(e)
        
        df = pd.DataFrame(data)
        print(df)
    

    def derialise(self):
        self.mycursor.execute("SELECT * FROM icg_billing LIMIT 2")
        results = self.mycursor.fetchall()
        json_obj_list = []

        for data in results:
            id = data['id']
            msisdn = data['msisdn']
            productID = data['productID']
            message = data['message']
            shortCode = data['shortCode']
            category = data['category']

            obj_data = {
                "id": id,
                "msisdn" : msisdn,
                "productID" : productID,
                "message" : message,
                "shortCode" : shortCode,
                "category" : category
                }
            json_obj_list.append(obj_data)

        rs = json.dumps(json_obj_list)
        print(rs)


    def pythonObj(self):
        json_data = [{"id": 1, "name": "John", "age": 30, "city": "New York"},
             {"id": 2, "name": "Mary", "age": 25, "city": "London"}]
        
        for obj in json_data:
            key = f"user:{obj['id']}"
            value = json.dumps(obj)
            self.r.set(key,value)

            rs = self.r.get(key)
            print(json.loads(rs))





# Stagging().derialise()
# Stagging().store_data()
# Stagging().fetch_data()
Stagging().pythonObj()

