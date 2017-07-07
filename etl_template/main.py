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


def template_main_func():
    extract = Extract()
    transform = Transform()
    load = Load()
    record = Record()


if __name__ == '__main__':
    db_cfg = configparser.ConfigParser()
    db_cfg.read('db.cfg')

    logging.config.fileConfig('log.cfg')

    template_main_func()
