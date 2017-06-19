# -*- coding:utf-8 -*-
import pandas as pd


class Extract(object):
    """数据抽取
    
    1 检查已经传输过的商圈
    2 抽取商圈下的poi计数信息
    3 抽取租金
    4 抽取商圈对应的省市县列表
    5 收取商圈的行业列表
    """
    def __init__(self,engine_zone_macro,engine_draw,engine_target):
        self.engine_zone_macro = engine_zone_macro
        self.engine_draw = engine_draw
        self.engine_target = engine_target

    def done_market(self):
        """已经完成etl的市场
        
        :return: 
        """
        done_market = pd.read_sql_table(
            table_name='fact_market',
            con=self.engine_target,
            columns=['marketGuid']
        )
        # return done_market['marketGuid'].values

        return "('" + "','".join(list(done_market['marketGuid'])) + "')"

    def tag_counts(self,done_market):
        """抽取tag_counts全表
        
        :param engine_zone_macro: zone_macro数据库引擎
        :return: 所有商圈tag计数的数据框
        """
        sql = "SELECT * " \
              "FROM tag_counts " \
              " WHERE type != 1 " \
              " AND grandParentId NOT IN %s"%done_market
        tag_counts = pd.read_sql_query(sql=sql,con=self.engine_zone_macro)

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
              "WHERE rent > 0 " \
              "AND grandParentId = '%s'"%grandParentId

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