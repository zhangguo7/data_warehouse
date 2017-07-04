# coding:utf-8
import sys
import configparser
import logging,logging.config
from zg7.db_funcs import mysql_engine
from extract import Extract
from transform import Transform
from load import Load

def etl_demension_division(target_engine):
    """division表的etl主函数

    从统计局爬取的标准csv表中抽取数据，载入到数据仓库
    :param target_engine:目标数据库引擎
    """
    extract = Extract()
    transform = Transform()
    load = Load(target_engine)
    logging.info('Initialize three instances')

    division_datasets = extract.std_divisions()
    std_districts = transform.std_districts(division_datasets)
    load.loading(std_districts)

if __name__ == '__main__':
    db_cfg = configparser.ConfigParser()
    db_cfg.read('../../db.cfg')
    logging.config.fileConfig('log.cfg')

    target_engine = mysql_engine(**db_cfg['dw_loc'])
    etl_demension_division(target_engine)