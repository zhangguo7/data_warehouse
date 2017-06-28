# -*- coding:utf-8 -*-
import sys
import logging,logging.config

sys.path.append('../../tools')
import configparser

from extract import Extract
from transform import Transform
from load import Load

from db_funcs import mysql_engine,mssql_engine

def etl_fact_market(*args):
    """fact_market表主函数
    
    :param args: 按位参数engine_zone_macro,engine_draw,engine_target
    """
    # 初始化 extract,transform和load三个对象
    extract = Extract(engine_zone_macro,engine_draw,engine_target)
    transform = Transform()
    load = Load(engine_target)

    # 抽取已经经过etl的商圈
    done_market = extract.done_market()
    df_tag_counts = extract.tag_counts()
    df_industry = extract.industry()
    has_dealed = []

    for i,sample_tag_counts in df_tag_counts.iterrows():

        grandParentId = sample_tag_counts['grandParentId']
        if len(grandParentId) != 36:  # 判断grandParentId的有效性
            logging.warning('Round %d, %s is invalid ,skipped.'%(i,grandParentId))
            continue

        elif grandParentId in done_market:  # 判断该商圈是已经经过etl
            logging.warning('Round %d, %s etl before'%(i,grandParentId))
            continue

        if grandParentId in has_dealed:
            logging.warning('Round %d, %s etl before' % (i, grandParentId))
            continue
        else:
            has_dealed.append(grandParentId)

        # 抽取数据
        zone_grandparent = extract.zone_grandparent(grandParentId)
        if len(zone_grandparent) == 0:
            logging.warning('Round %d, has no draw samples'%i)
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
            logging.info('Round %d, %s etl secceed'%(i,grandParentId))
        except Exception as e:
            logging.error('Round %d, %s'%(i,e))

if __name__ == '__main__':
    db_cfg = configparser.ConfigParser()
    db_cfg.read('../../db.cfg')

    logging.config.fileConfig('log.cfg')
    engine_zone_macro = mysql_engine(**db_cfg['zone_macro'])
    engine_draw = mssql_engine(**db_cfg['ht_test'])
    engine_target = mysql_engine(**db_cfg['dw_test'])

    etl_fact_market(engine_zone_macro,engine_draw,engine_target)
