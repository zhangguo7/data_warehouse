# -*- coding:utf-8 -*-
"""
setup 模块用于建立数据仓库及表
支持MySQL数据库
依赖:
    1 创建表的create_tables.sql文件（由navicat的数据模型生成）
    2 根目录的db.cfg文件（部署需要修改db.cfg文件）
"""
import os
import configparser

def create_db(db_info):
    """创建数据库

    :param db_info: 数据库信息
    """
    try:
        cmd = 'mysql -h%s -u%s -p%s -e "CREATE DATABASE IF NOT EXISTS %s"'\
              %(db_info['host'],db_info['user'],db_info['password'],db_info['db'])
        os.system(cmd)
    except Exception as e:
        print(e)

def create_tables(db_info,sql_path):
    """创建数据表

    :param db_info: 数据库信息
    :param sql_path: SQL文件路径
    """
    with open(sql_path,'r',encoding='utf-8') as sql_file:
        sql_lst = sql_file.read().replace('\n',' ').split(';')

    for sql in sql_lst:
        if sql:
            cmd = 'mysql -h%s -u%s -p%s %s -e "%s"' \
                  %(db_info['host'],db_info['user'],db_info['password'],db_info['db'],sql)
            os.system(cmd)

if __name__ == '__main__':
    db_cfg = configparser.ConfigParser()
    db_cfg.read('../db.cfg')
    dw_info = db_cfg['dwloc']
    create_db(dw_info)
    create_tables(dw_info,'create_tables.sql')