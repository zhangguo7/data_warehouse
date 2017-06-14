# -*- coding:utf-8 -*-

def mysql_engine(**kwargs):
    from sqlalchemy import create_engine
    if kwargs.get('port'):
        return create_engine("mysql+pymysql://%s:%s@%s:%s/%s?charset=%s" \
            %(kwargs['user'],kwargs['password'],kwargs['host'],kwargs['port'],kwargs['db'],kwargs['charset']))
    return create_engine("mysql+pymysql://%s:%s@%s:3306/%s?charset=%s" \
            %(kwargs['user'],kwargs['password'],kwargs['host'],kwargs['db'],kwargs['charset']))

def mssql_engine(**kwargs):
    from sqlalchemy import create_engine
    if kwargs.get('port'):
        return create_engine("mssql+pymssql://%s:%s@%s:%s/%s" \
            % (kwargs['user'],kwargs['password'],kwargs['server'],kwargs['port'],kwargs['database']))
    return create_engine("mssql+pymssql://%s:%s@%s:1433/%s" \
            % (kwargs['user'], kwargs['password'], kwargs['server'], kwargs['database']))