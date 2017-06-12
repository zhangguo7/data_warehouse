# -*- coding:utf-8 -*-
import sys
sys.path.append('./')
import configparser

from tools.db_funcs import mysql_engine,mssql_engine
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
    df_industry,df_draw = extract.extract_main()

    transform = Transform(df_industry, df_draw)
    df_clean = transform.transform_main()

    load = Load(engine_target)
    load.load_main(df_clean)

if __name__ == '__main__':
    db_cfg = configparser.ConfigParser()
    db_cfg.read('../../db.cfg')

    engine_source = mssql_engine(**db_cfg['ht250'])
    engine_target = mysql_engine(**db_cfg['dw240'])

    etl_fact_draw_main(engine_source,engine_target)
