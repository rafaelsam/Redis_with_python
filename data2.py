import redis
import json


def Data():
    # Connect to Redis
    r = redis.Redis(host='localhost', port=6379, db=0)

    # Assume that `json_data` is a list of JSON objects
    json_data = [{"id": 1, "name": "John", "age": 30, "city": "New York"},
                {"id": 2, "name": "Mary", "age": 25, "city": "London"}]

    # Store each JSON object in Redis with a unique key
    for obj in json_data:
        key = f"user:{obj['id']}"
        value = json.dumps(obj)
        r.set(key, value)
        r.zadd("id_index", {key: obj["id"]})
        r.zadd("name_index", {key: obj["name"]})
        r.zadd("age_index", {key: obj["age"]})

    # Find users with id=1, name="John", and age=30
    result_keys = r.zinterstore("search_result", ["id_index", "name_index", "age_index"], aggregate="MAX")
    result = [json.loads(r.get(key)) for key in result_keys]

    # Print the result
    print(result)
