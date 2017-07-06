"""
version: 1.0.0
author : Zhang Guoqi
"""
import configparser
import logging.config
from zg7.db_funcs import mysql_engine

from extract import Extract
from transform import Transform
from load import Load
from record import Record


def etl_fact_macro_details(source_engine, target_engine):
    """fact_macro_details的etl主函数

    从235 tag_detail表etl到240 fact_macro_details表
    :param source_engine: 源数据库引擎
    :param target_engine: 目标数据库引擎
    """
    extract = Extract(source_engine, target_engine)
    transform = Transform()
    load = Load(target_engine)
    record = Record(table='fact_macro_detail', record_path='rec.cfg')

    start_params = record.get_record()
    divisions = extract.std_divisions()

    for i in range(start_params['rounds']):
        start_id = start_params['update_id'] + i * start_params['chunksize'] + 1
        end_id = start_params['update_id'] + (i + 1) * start_params['chunksize'] + 1

        tag_details = extract.tag_details(start_id, end_id)
        if len(tag_details) == 0:
            continue
        macro_details = transform.compile_datasets(tag_details, divisions)
        load.loading(macro_details)
        update_id = tag_details['id'].max() if tag_details['id'].max() else start_params['update_id']
        record.update_record(update_id)


if __name__ == '__main__':
    db_config = configparser.ConfigParser()
    db_config.read('../../db.cfg')
    logging.config.fileConfig('log.cfg')

    source_engine = mysql_engine(**db_config['zone_macro'])
    target_engine = mysql_engine(**db_config['dw_loc'])

    etl_fact_macro_details(source_engine, target_engine)