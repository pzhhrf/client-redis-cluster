#!/usr/bin/env python3
# encoding: utf-8

"""
    数据库配置文件,注意,此配置文件的key必须包含下划线_以及后续的名称,用做初始化不同的数据库连接
"""
storage_engines = {
    "redis_cli" : {
        "type" : "redis",
        "cache" : True,
        "servers" : {
            "policy_sharding_1" : {
                "host" : "127.0.0.1:6379:0",
            },
        },
        "default_sharding" : "policy_sharding_1",
        #sharding规则自己定义
        "sharding":{
        }
    },
}
