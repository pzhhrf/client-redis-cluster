#!/usr/bin/env python3
# encoding: utf-8
import redis,os,traceback,json,functools
from btcmarket.config import db,const
from btcmarket.lib.storage import storage_context
from .pipeline import Pipeline
from .redis_cmd import SHARD_METHODS,translate_data
from multiprocessing.dummy import Pool as ThreadPool

class RedisEngine():

    def __init__(self,kwargs):
        '''
        servers is a string like "192.168.0.1:9988"
        '''
        if not kwargs:
            raise RuntimeError("redis db config error")

        self.connections = {}
        self.kwargs = kwargs
        self.nodes = []
        for key,value in kwargs.get("servers").items():
            host = value.get("host")
            password = value.get("password")
            s,p,d = host.split(":")
            self.connections[key] = redis.StrictRedis(connection_pool = 
                redis.ConnectionPool(host=s, port= int(p),db = int(d), password = password,decode_responses = True))
            self.nodes.append(key)

        self.pool = None

    def _build_pool(self):
        if self.pool is None:
            self.pool = ThreadPool(len(self.nodes))

    def get_server_name(self,key):
        """
            根据当前key的名称,获取服务名字
        """
        sharding = self.kwargs.get("sharding")
        policy = None
        for name,vlist in sharding.items():
            for v in vlist:
                if v in key:
                    policy = name
                    break

        if not policy:
            policy = self.kwargs.get("default_sharding")
        return policy


    def get_server(self,key):
        name = self.get_server_name(key)
        return self.connections[name]

    def __wrap(self,method,*args,**kwargs):
        try:
            key = args[0]
            assert isinstance(key, str)
        except:
            raise ValueError("method '%s' requires a key param as the first argument" % method)
        server = self.get_server(key)
        f = getattr(server, method)
        t = f(*args, **kwargs)
        return translate_data(t)

    def __getattr__(self,method):
        if method in SHARD_METHODS:
            return functools.partial(self.__wrap, method)
        else:
            raise NotImplementedError("method '%s' cannot be sharded" % method)

    def pipeline(self):
        return Pipeline(self)


__all__ = [k for k,v in db.storage_engines.items() if v.get("type") == "redis"]

for x in __all__:
    if '_' not in x:
        raise RuntimeError("redis config name catch error,must be have a '_' character")
    globals()[x] = RedisEngine(db.storage_engines.get(x))

__all__.append("translate_data")

