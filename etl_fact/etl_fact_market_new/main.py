"""
version:
author :
"""
import configparser
import logging.config

from extract import Extract
from transform import Transform
from load import Load
from record import Record


def etl_fact_market(source_engine,target_engine,rec_path):

    extract = Extract(source_engine,target_engine)
    transform = Transform()
    load = Load(target_engine)
    record = Record('rec.cfg')


    start_params = record.get_record()
    unique_marketguid = []
    done_market = []
    has_dealed = []

    for i,grandParentId in enumerate(unique_marketguid):

        if len(grandParentId) != 36:  # 判断grandParentId的有效性
            logging.error('Round %d, %s is not valid.'%(i,grandParentId))
            continue

        elif grandParentId in done_market:  # 判断该商圈是已经经过etl
            logging.warning('Round %d, %s etl before'%(i,grandParentId))
            continue

        if grandParentId in has_dealed:
            logging.warning('Round %d, %s etl before' % (i, grandParentId))
            continue
        else:
            has_dealed.append(grandParentId)

        zone_grandparent = extract.zone_grandparent(grandParentId)
        if len(zone_grandparent) == 0:
            logging.warning('Round %d, has no draw samples' % i)
            continue

        rent = extract.rent_details(grandParentId)
        industry_tmp = industry[industry['grandParentId'] == grandParentId]
        # 转换数据
        rent = transform.rent_calculate(rent)
        industry_dict = transform.reshape_industry(industry_tmp)
        # 组合数据
        clean = transform.compile_dfs(sample_tag_counts, rent, industry_dict, zone_grandparent)
        try:
            load.loading(clean)
        except Exception as e:
            logging.error('Round %d, %s' % (i, e))

if __name__ == '__main__':
    db_cfg = configparser.ConfigParser()
    db_cfg.read('db.cfg')

    logging.config.fileConfig('log.cfg')



