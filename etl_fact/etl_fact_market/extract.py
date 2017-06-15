# -*- coding:utf-8 -*-
import pandas as pd


class Extract(object):

    def __init__(self,engine_zone_macro,engine_draw):
        self.engine_zone_macro = engine_zone_macro
        self.engine_draw = engine_draw

    def tag_counts(self):
        """抽取tag_counts全表
        
        :param engine_zone_macro: zone_macro数据库引擎
        :return: 所有商圈tag计数的数据框
        """
        tag_counts = pd.read_sql_table(
            table_name='tag_counts',
            con=self.engine_zone_macro)
        # tag_counts = pd.read_sql_query(sql='select * from tag_counts',
        #                                con=self.engine_zone_macro)
        return tag_counts

    def rent_details(self,grandParentId):
        """抽取租金明细
        
        :param engine_zone_macro: zone_macro数据库引擎
        :param grandParentId: 对应商圈的id
        :return: 一个商圈的租金明细数据框
        """
        sql = "SELECT " \
              " houseType," \
              " rent," \
              " unit," \
              " coveringArea " \
              "FROM rent_detail " \
              "WHERE grandParentId = '%s'"%grandParentId

        rent_details = pd.read_sql_query(sql=sql,con=self.engine_zone_macro)
        return rent_details

    def zone_grandparent(self,grandParentId):
        """抽取商圈省市县对应表
        
        :return: 商圈省市县对应数据框
        """
        sql = "SELECT TOP 1" \
              " zoneGuid," \
              " zoneName," \
              " grandParentName," \
              " provinceName, "\
              " cityName, "\
              " districtName, "\
              " districtId " \
              "FROM CommercialZone_Sample " \
              " WHERE grandParentId = '%s'"%grandParentId
        zone_grandparent = pd.read_sql_query(sql=sql,con=self.engine_draw)

        return zone_grandparent

    def industry(self,grandParentId):
        """抽取对应商圈的行业
        
        :param grandParentId: 对应商圈的id
        :return: 对应商圈的行业数据框
        """
        sql = "SELECT " \
              " attachId," \
              " industryId," \
              " industryName," \
              " industryPid " \
              "FROM CommercialZone_Industry " \
              " WHERE attachId = '%s' AND isDel = 0" %grandParentId
        industry = pd.read_sql_query(sql=sql,con=self.engine_draw)
        return industry