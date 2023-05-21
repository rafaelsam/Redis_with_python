import redis
from redis.commands.json.path import Path
import redis.commands.search.aggregation as aggregations
import redis.commands.search.reducers as reducers
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import NumericFilter, Query


class Demo:
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    def demo_test(self):
        self.r.set('studentNames', 'RafaJs')
        value = self.r.get('mykey')

        self.r.hset('userID:3212', mapping={
            'fullname': 'Rafael Js',
            'gender': 'Male',
            'age': '24'
        })
        user_id = self.r.hgetall('userID:3212')
        print(user_id)

    
    def readJson(self):
        user1 = {
            "user":{
                "name": "Paul John",
                "email": "paul.john@example.com",
                "age": 42,
                "city": "London"
            }
        }
        user2 = {
            "user":{
                "name": "Eden Zamir",
                "email": "eden.zamir@example.com",
                "age": 29,
                "city": "Tel Aviv"
            }
        }
        user3 = {
            "user":{
                "name": "Paul Zamir",
                "email": "paul.zamir@example.com",
                "age": 35,
                "city": "Tel Aviv"
            }
        }

        self.r.json().set("user:1", Path.root_path(), user1)
        self.r.json().set('user:2',Path.root_path(), user2)
        self.r.json().set('user:3',Path.root_path(), user3)

        schema = (
            TextField('$.name',as_name = 'name'),
            TagField('$.city',as_name = 'city'),
            NumericField('$.age',as_name='age')
        )

        self.ft().create_index(schema, definition=IndexDefinition(prefix=["user:"], index_type=IndexType.JSON))

        search_res = self.r.ft().search('paul')

        print(search_res)

Demo().demo_test()
