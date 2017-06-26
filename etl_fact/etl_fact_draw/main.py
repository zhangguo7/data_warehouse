# -*- coding:utf-8 -*-
import sys
sys.path.append('../../tools')

import configparser

from db_funcs import mysql_engine,mssql_engine
from extract import Extract
from transform import Transform
from load import Load


def etl_fact_draw_main(engine_source, engine_target,chunksize=5000):
    """绘图事实表的ETL

    :param engine_source: 源数据库引擎
    :param engine_target: 目标数据库引擎
    """
    extract = Extract(engine_source,chunksize)
    df_industry,df_draw_gen = extract.extract_main()

    for k,df_draw in enumerate(df_draw_gen,1):
        print('第%s轮,第%s条到第%s条数据,start!'% \
              (k,(k-1)*chunksize,k*chunksize))

        transform = Transform(df_industry, df_draw)
        df_clean = transform.transform_main()

        load = Load(engine_target)

        try:
            load.load_main(df_clean)
        except Exception as e:
            df_clean['drawGuid'].to_csv('unsecceed_samples.csv', mode='a',index=False)
            print(e)

if __name__ == '__main__':
    db_cfg = configparser.ConfigParser()
    db_cfg.read('../../db.cfg')

    engine_source = mssql_engine(**db_cfg['ht'])
    engine_target = mysql_engine(**db_cfg['dw_test'])

    etl_fact_draw_main(engine_source, engine_target)
