# -*- coding:utf-8 -*-

def mysql_engine(**kwargs):
    from sqlalchemy import create_engine
    mysql_engine = create_engine("mysql+pymysql://%s:%s@%s/%s?charset=%s" \
        % (kwargs['user'],kwargs['password'],kwargs['host'],kwargs['db'],kwargs['charset']))
    return mysql_engine


def mssql_engine(**kwargs):
    from sqlalchemy import create_engine
    mssql_engine = create_engine("mssql+pymssql://%s:%s@%s/%s" \
        % (kwargs['user'],kwargs['password'],kwargs['server'],kwargs['database']))
    return mssql_engine