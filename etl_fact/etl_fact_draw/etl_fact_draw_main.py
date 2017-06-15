# -*- coding:utf-8 -*-
import sys
sys.path.append('../')
sys.path.append('../../tools')
import configparser

from db_funcs import mysql_engine,mssql_engine
from extract import Extract
from transform import Transform
from load import Load


def etl_fact_draw_main(engine_source, engine_target):
    """绘图事实表的ETL

    :param engine_source: 源数据库引擎
    :param engine_target: 目标数据库引擎
    :return:
    """
    extract = Extract(engine_source)
    df_industry,df_draw_gen = extract.extract_main()

    for k,df_draw in enumerate(df_draw_gen,1):
        print(k,k*1000,'start!')
        # if k > 3:break
        transform = Transform(df_industry, df_draw)
        df_clean = transform.transform_main()

        load = Load(engine_target)
        load.load_main(df_clean)

if __name__ == '__main__':
    db_cfg = configparser.ConfigParser()
    db_cfg.read('../../db.cfg')

    engine_source = mssql_engine(**db_cfg['ht'])
    engine_target = mysql_engine(**db_cfg['dw'])

    etl_fact_draw_main(engine_source,engine_target)
