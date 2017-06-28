# -*- coding:utf-8 -*-

import pandas as pd
import logging


class Extract(object):
    """绘图事实表的数据抽取模块

    抽取行业表和绘图样本表
    _extract_industry和_extract_draw为内部函数
    最后用extract_main进行了封装
    """
    def __init__(self,engine,chunksize,record_file):
        self.engine = engine
        self.chunksize = chunksize
        self.record_file = record_file

    def _extract_industry(self):
        """抽取行业表

        :return: 绘图样本的行业
        包含：attachId     -->  guid
             industryId   -->  行业id
             industryName -->  行业名
             industryPid  -->  上一级行业id（Pid,Parent Id）
        """
        ind_sql = "SELECT " \
                  " attachId," \
                  " industryId," \
                  " industryName," \
                  " industryPid " \
                  "FROM CommercialZone_Industry " \
                  "WHERE isDel = 0 AND type = 6"
        return pd.read_sql_query(sql=ind_sql, con=self.engine)

    def _extract_draw(self):
        """抽取绘图样本"""
        try:
            with open(self.record_file, 'r') as f:
                begin_id = int(f.read())
        except (FileNotFoundError, ValueError) as e:
            logging.warning(e)
            begin_id = 0
            logging.error('%s, no record before, begine id default 0' %e)

        draw_sql = "SELECT " \
                   " id," \
                   " guid," \
                   " provinceName," \
                   " cityName," \
                   " districtName," \
                   " grandParentName," \
                   " grandParentId," \
                   " zoneGuid," \
                   " districtId," \
                   " receiveDate," \
                   " inputDate," \
                   " mateAddress," \
                   " doorPlate," \
                   " selfNum," \
                   " sampleName," \
                   " sampleMobile," \
                   " sampleTel," \
                   " bdLatitude," \
                   " bdlongitude," \
                   " photoCount," \
                   " shopCount," \
                   " decorateDescrption," \
                   " isBusinessLicence," \
                   " operatingState1," \
                   " operatingState2," \
                   " operatingState " \
                   "FROM CommercialZone_Sample " \
                   " WHERE guid != '' " \
                   " AND cityName != '成都市'" \
                   " AND isDel = 0" \
                   " AND checkStatus IN (1,3)" \
                   " AND id > %d" \
                   " ORDER BY id" %begin_id
        draw_ge = pd.read_sql_query(sql=draw_sql, con=self.engine, chunksize=self.chunksize)
        logging.info('Secceed to extract draw_ge as gen, chunksize=%d'%self.chunksize)
        return draw_ge

    def extract_main(self):
        """extract步骤的主函数

        :return: 行业数据框和绘图数据框
        """
        df_industry = self._extract_industry()
        df_draw_gen = self._extract_draw()
        return df_industry, df_draw_gen
