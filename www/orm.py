#!/usr/bin/python3
# -*- coding: utf-8 -*-
__author__='Windyear'

import asycio,logging
import aiomysql

#use to print the sql statement
def log(sql,args=()):
    logging.info('SQL:%s' % sql)
    
#一个产生数据库连接的池
async def create_pool(loop,**kw):
    logging.info('create database connection pool...')
    global __pool
    __pool =await aiomysql.create_pool(host=kw.get('host','localhost'),
        port=kw.get('port',3306),user=kw['user'],password=kw['password'],
        db=kw['db'],charset=kw.get('charset','utf8'),
        autosommit=kw.get('autocommit',True),maxsize=kw.get('maxsize',10),
        minsize=kw.get('minsize',1),loop=loop)
#查询语句的执行函数
async def select(sql,args,size=None):
    log(sql,args)
    global __pool
    async with __pool.get() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?','%s'),args or())
            if size:
                rs=await cur.fetmany(size)
            else:
                rs=await cur.fetchal()
        logging.info('rows returned: %s' % len(rs))
        return rs

#其他语句的执行函数，返回产生影响的行数
async def execute(sql,args,autocommit=True):
    log(sql)
    global __pool
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        async with conn.cursor(aiomysql.DictCursor) as cur:
            