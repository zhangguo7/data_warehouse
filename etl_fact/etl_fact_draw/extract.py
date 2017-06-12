# -*- coding:utf-8 -*-

import pandas as pd


class Extract(object):
    """绘图事实表的数据抽取模块

    抽取行业表和绘图样本表
    _extract_industry和_extract_draw为内部函数
    最后用extract_main进行了封装
    """
    def __init__(self,engine):
        self.engine = engine

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
        vars = [
            'guid', 'grandParentId', 'zoneGuid', 'districtId', 'receiveDate',
            'inputDate', 'mateAddress', 'doorPlate', 'selfNum', 'sampleName',
            'sampleMobile', 'sampleTel', 'bdLatitude', 'bdlongitude',
            'photoCount', 'shopCount', 'decorateDescrption', 'isBusinessLicence',
            'operatingState1', 'operatingState2', 'operatingState'
        ]
        return pd.read_sql_table(table_name='CommercialZone_Sample',con=self.engine, columns=vars)

    def extract_main(self):
        """extract步骤的主函数

        :return: 行业数据框和绘图数据框
        """
        df_industry = self._extract_industry()
        df_draw = self._extract_draw()
        return df_industry, df_draw