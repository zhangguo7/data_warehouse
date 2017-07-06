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


def etl_func():
    extract = Extract()
    transform = Transform()
    load = Load()
    record = Record()


if __name__ == '__main__':
    pass
