# -*- coding:utf-8 -*-
import sys

import time

import pandas as pd
from collections import defaultdict
sys.path.append('../../tools')
import configparser

from extract import Extract
from transform import Transform
from load import Load

from db_funcs import mysql_engine,mssql_engine

def etl_fact_market(*args):
    """fact_market表主函数
    
    :param args: 按位参数engine_zone_macro,engine_draw,engine_target
    :return: 
    """
    # 初始化 extract,transform和load三个对象
    extract = Extract(engine_zone_macro,engine_draw,engine_target)
    transform = Transform()
    load = Load(engine_target)

    # 抽取已经经过etl的商圈
    # t0 = time.time()
    done_market = extract.done_market()
    # t1 = time.time()
    df_tag_counts = extract.tag_counts()
    # t2 = time.time()
    df_industry = extract.industry()
    # t3 = time.time()
    # print('extract done samples %.2f s'% (t1 - t0))
    # print('extract tag counts %.2f s' % (t2 - t1))
    # print('extract industry %.2f s' % (t3 - t2))
    for i,sample_tag_counts in df_tag_counts.iterrows():

        grandParentId = sample_tag_counts['grandParentId']
        if len(grandParentId) != 36:  # 判断grandParentId的有效性
            print(i,grandParentId,'is not valid !')
            continue
        elif grandParentId in done_market:  # 判断该商圈是已经经过etl
            print(i, grandParentId, 'etl before !')
            continue

        # 抽取数据
        zone_grandparent = extract.zone_grandparent(grandParentId)
        if len(zone_grandparent) == 0:
            print(i, grandParentId, 'has no draw samples !')
            continue
        rent = extract.rent_details(grandParentId)
        industry_tmp = df_industry[df_industry['grandParentId'] == grandParentId]
        # 转换数据
        rent = transform.rent_calculate(rent)
        industry_dict = transform.reshape_industry(industry_tmp)
        # 组合数据
        clean = transform.compile_dfs(sample_tag_counts,rent,industry_dict,zone_grandparent)
        try:
            load.loading(clean)
        except Exception as e:
            print(e)

if __name__ == '__main__':
    db_cfg = configparser.ConfigParser()
    db_cfg.read('../../db.cfg')

    engine_zone_macro = mysql_engine(**db_cfg['zone_macro'])
    engine_draw = mssql_engine(**db_cfg['ht'])
    engine_target = mysql_engine(**db_cfg['dw_test'])

    etl_fact_market(engine_zone_macro,engine_draw,engine_target)
