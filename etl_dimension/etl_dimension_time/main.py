
import configparser
import logging.config
from zg7.db_funcs import mysql_engine
from extract import Extract
from transform import Transform
from load import Load


def etl_dimension_time(target_engine):
    """时间维度表主函数

    :param target_engine: 目标数据库引擎
    """
    extract = Extract()
    transform = Transform()
    load = Load(target_engine)

    full_time = extract.gen_full_time()
    time_table = transform.gen_date(full_time)
    load.loading(time_table)


if __name__ == '__main__':
    db_cfg = configparser.ConfigParser()
    db_cfg.read('../../db.cfg')

    logging.config.fileConfig('log.cfg')

    target_engine = mysql_engine(**db_cfg['dw_loc'])
    etl_dimension_time(target_engine)
