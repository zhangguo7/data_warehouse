# -*- coding:utf-8 -*-
import sys

import time

sys.path.append('../../tools')
import configparser

from threading import Thread
from extract import Extract
from transform import Transform
from load import Load

from db_funcs import mysql_engine,mssql_engine

def etl_fact_market(*args):

    extract = Extract(engine_zone_macro,engine_draw)
    transform = Transform()
    load = Load(engine_target)
    df_tag_counts = extract.tag_counts()

    for i,sample_tag_counts in df_tag_counts.iterrows():

        grandParentId = sample_tag_counts['grandParentId']
        # 按照商圈的id提取数据
        zone_grandparent = extract.zone_grandparent(grandParentId)
        if zone_grandparent.ix[0,'districtId'][:2] != '51':
            print('非成都商圈')
            continue
        t1 = time.time()
        rent = extract.rent_details(grandParentId)
        t2 = time.time()
        industry = extract.industry(grandParentId)
        t3 = time.time()
        # 转换数据
        rent = transform.rent_calculate(rent)
        t4 = time.time()
        industry = transform.reshape_industry(industry)
        t5 = time.time()
        clean = transform.merge(sample_tag_counts,rent,industry,zone_grandparent)
        t6 = time.time()
        print('extract rent         costs  %.2f s' % (t2 - t1))
        print('extract industry     costs  %.2f s' % (t3 - t2))
        print('calculate rent       costs  %.2f s' % (t4 - t3))
        print('reshape industry     costs  %.2f s' % (t5 - t4))
        print('merge                costs  %.2f s' % (t6 - t5))

        try:
            load.loading(clean)
            print(i, clean.ix[0,['marketGuid','marketName']])
        except Exception as e:
            print(e)

if __name__ == '__main__':
    db_cfg = configparser.ConfigParser()
    db_cfg.read('../../db.cfg')

    engine_zone_macro = mysql_engine(**db_cfg['zone_macro'])
    engine_draw = mssql_engine(**db_cfg['ht'])
    engine_target = mysql_engine(**db_cfg['dw_test'])

    etl_fact_market(engine_zone_macro,engine_draw,engine_target)
