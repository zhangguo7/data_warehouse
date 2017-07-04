# coding:utf-8
import logging

import pandas as pd


class Extract(object):

    def std_divisions(self):
        """抽取标准的行政区划数据

        :return: 标准的行政区划数据
        """
        std_divisions = pd.read_csv('std_districts.csv',encoding='gbk')
        logging.info('Succeed to extract std_divisions')
        return std_divisions