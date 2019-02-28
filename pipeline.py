#!/usr/bin/env python3
# encoding: utf-8

import functools
from .redis_cmd import SHARD_METHODS

class Pipeline(object):

    def __init__(self, shard_api):
        self.shard_api = shard_api
        self.pipelines = {}
        self.__counter = 0
        self.__indexes = {}
        self.__watching = None
        self.shard_api._build_pool()

    def get_pipeline(self, key):
        name = self.shard_api.get_server_name(key)
        if name not in self.pipelines:
            self.pipelines[name] = self.shard_api.connections[name].pipeline()
        return self.pipelines[name]

    def __record_index(self, pipeline):
        if pipeline not in self.__indexes:
            self.__indexes[pipeline] = [self.__counter]
        else:
            self.__indexes[pipeline].append(self.__counter)
        self.__counter += 1


    def __wrap(self, method, *args, **kwargs):
        try:
            key = args[0]
            assert isinstance(key, str)
        except:
            raise ValueError("method '%s' requires a key param as the first argument" % method)
        pipeline = self.get_pipeline(key)
        f = getattr(pipeline, method)
        r = f(*args, **kwargs)
        self.__record_index(pipeline)
        return r

    def execute(self):
        try:
            if self.__watching:
                return self.pipelines[self.__watching].execute()
            else:
                results = []

                # Pipeline concurrently
                dictvalues = lambda x: list(x.values())
                values = self.shard_api.pool.map(self.__unit_execute, dictvalues(self.pipelines))
                for v in values:
                    results.extend(v)

                self.__counter = 0
                self.__indexes = {}

                results.sort(key=lambda x: x[0])
                results = [r[1] for r in results]
                return results
        finally:
            self.reset()

    def __unit_execute(self, pipeline):
        result = pipeline.execute()
        return zip(self.__indexes.get(pipeline, []), result)

    def __getattr__(self, method):
        if method in SHARD_METHODS:
            return functools.partial(self.__wrap, method)
        else:
            raise NotImplementedError("method '%s' cannot be sharded" % method)

    def watch(self, *keys):
        """
            关于事务的暂时先忽略
            https://github.com/zhihu/redis-shard/blob/master/redis_shard/pipeline.py
        """
        pass

    def multi(self):
        pass

    def reset(self):
        if self.__watching:
            pipeline = self.pipelines[self.__watching]
            try:
                pipeline.reset()
            except Exception:
                print('failed to reset pipeline')
            self.__watching = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.reset()

    def __del__(self):
        self.reset()

