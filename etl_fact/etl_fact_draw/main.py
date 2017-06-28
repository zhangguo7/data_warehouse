# -*- coding:utf-8 -*-
import sys
sys.path.append('../../tools')

import configparser

from db_funcs import mysql_engine,mssql_engine
from extract import Extract
from transform import Transform
from load import Load
import logging,logging.config

def etl_fact_draw_main(engine_source, engine_target,chunksize=5000,record_file='etl_fact_draw.record'):
    """绘图事实表的ETL

    :param engine_source: 源数据库引擎
    :param engine_target: 目标数据库引擎
    """
    extract = Extract(engine_source,chunksize,record_file)
    transform = Transform()
    load = Load(engine_target)
    # 抽取数据
    df_industry,df_draw_gen = extract.extract_main()
    logging.info('Extract datasets completed.')

    for k,df_draw in enumerate(df_draw_gen,1):
        logging.info('Round %d, From obs.%d to obs.%d,start.' % \
                     (k, (k-1)*chunksize, k*chunksize))
        # 清理、转换数据
        df_clean = transform.transform_main(df_industry, df_draw)
        logging.info('Round %d, Data cleaning completed.'%k)

        try:
            load.load_main(df_clean)
            logging.info('Round %d, loading %d obs. Secceed '%(k,len(df_clean)))
            print(df_draw.columns)
            with open(record_file,'w') as f:
                f.write(str(max(df_draw['id'])))
        except Exception as e:
            df_clean[['drawGuid', 'marketGuid']].to_csv('unsecceed_samples.csv', mode='a',index=False)
            logging.error('Round %d,%s' %(k,e))

if __name__ == '__main__':
    db_cfg = configparser.ConfigParser()
    db_cfg.read('../../db.cfg')

    logging.config.fileConfig('log.cfg')

    engine_source = mssql_engine(**db_cfg['ht_test'])
    engine_target = mysql_engine(**db_cfg['dw_test'])

    etl_fact_draw_main(engine_source, engine_target)
