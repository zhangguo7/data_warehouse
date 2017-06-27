# coding:utf-8
"""
该项ETL为数据仓库到应用层数据库的ETL项目


涵盖4个.py文件
1. main.py        etl主函数，包含 market_to_api2 一个主函数
2. extract.py     抽取原始数据的class
3. transform.py   转换数据的class
4. load.py        装载数据的class
"""
import configparser
import logging,logging.config
from extract import Extract
from transform import Transform
from load import Load

from tools.db_funcs import mysql_engine

def market_to_api2(source,target,record_file='api2.record'):
    """anti_fraud数据库api2表的etl主函数
    
    :param source: 源数据库引擎
    :param target: 目标数据库引擎
    :param record_file: 负责记录装载id的文件名，默认为 app2.record
    """
    # 初始化对象
    extract = Extract(source,target,record_file)
    transform = Transform()
    load = Load(target,record_file)

    # 抽取数据
    market_df = extract.market()
    draw_samples = extract.draw_samples()

    # 转换数据
    reshaped_market = transform.reshape_market(market_df)
    aggregated_samples = transform.aggregate_from_samples(draw_samples)
    api2_df = transform.compile(reshaped_market,aggregated_samples)

    # 装载数据
    load.loading(api2_df)

if __name__ == '__main__':

    db_cfg = configparser.ConfigParser()
    db_cfg.read('db.cfg')

    logging.config.fileConfig('log.cfg')

    source = mysql_engine(**db_cfg['dw_test'])
    target = mysql_engine(**db_cfg['anti_fraud'])

    market_to_api2(source, target)