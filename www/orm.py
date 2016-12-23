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
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?','%s'),args)
                affected=cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise 
        return affected

#生成一个参数字符串
def create_args_string(num):
    L[]
    for n in range(num):
        L.append('?')
    return ', '.join(L)
    
#建立一个表示每列属性的基类
class Field(object):
    def __init__(self,name,primary_key,column_type,default):
        self.name=name
        self.column_type=column_type
        self.primary_key=primary_key
        self.default=default
    def __str__(self):
        return '<%s,%s:%s>' % (self.__class__.__name__,self.column_type,self.name)
#下面的是各种列的属性
class StringField(Field):

    def __init__(self,name=None,primary_key=False,default=None,ddl='varchar(100)'):
        super().__init__(name,ddl,primary_key,default)

class BooleanField(Field):

    def __init__(self,name=None,default=None):
        super().__init__(name,'boolean',False,default)

class IntegerField(Field):
    
    def __init__(self,name=None,primary_key=False,default=0):
        super.__init__(name,'bigint',primary_key,default)

class FloatField(Field):

    def __init__(self,name=None,primary_key=False,default=0.0):
        super().__init__(name,'text',False,default)

class TextField(Field):
    
    def __init__(self,name=None,default=None):
        super().__init__(name,'text',False,default)

        

#创建一个元类，用于创建不同的类，类可以映射数据表
class ModelMetaclass(type):

#该函数在__init__函数之前执行
    def __new__(cls,name,bases,attrs):
        if name=='Model':
            return type.__new__(cls,name,bases,attrs)
        tableName=attrs.get('__table__',None) or name
        logging.info('found model: %s (table: %s)' % (name,tableName))
        mappingsdict()
        fields=[]
        primaryKey=None
        for k,v in attrs.items():
            if isinstance(v,Field):
                logging.info(' found mapping: %s==>%s' %(k,v))
                mapping[k]=v
                if v.primary_key:
                    #found the primarykey
                    raise StandardError('Duplicate primary key for field: %s' % k)
                    primaryKey=k
                else:
                    fields.append(k)
        #end for
        if not primaryKey:
            raise StandardError('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields=list(map(lambda f: '`%s`' %f, fields))
        attrs['__mappings__'] = mappings # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)
        
#create a class which can represent a table in the database
class Model(dict,metaclass=ModelMetaclass):

#the init fuction
    def __init__(self,**kw):
        super(Model,self).__init__(**kw)
        
    def __getattr__(self,key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r'"Model" object has no attribute '%s' % key)
      
    def __setattr__(self,key,value):
        self[key]=value
        
    def getValue(self,key):
        return getattr(self,key,None)
    
    def getValueOrDefault(self,key):
        value=getattr(self,key,None)
        if value is None:
            field=self.__mapping__[key]
            if field.default is not None:
                value=field.default() if callable(field.default) else field.default
                logging.debug('using default vlue for %s :%s' % (key,str(value)))
                setattr(self,key,value)
        return value