from redis.cluster import RedisCluster

class Cluster:
    rc = RedisCluster(host='localhost', port=16379)

    def demo(self):
        print(self.rc.get_node())


Cluster().demo()