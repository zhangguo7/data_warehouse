# -*- coding:utf-8 -*-
import sys
sys.path.append('../../tools')

from collections import defaultdict

import pandas as pd
from tool_funcs import other2int

class Transform(object):
    """转换数据集类
    
    包含3个主要方法：
    1、rent_calculate 租金计算
    2、reshape_industry 重构行业数据并排序
    3、compile_dfs 组合所提取的数据框
    """
    def rent_calculate(self,rent):
        """租金计算
        
        :param rent: 原始租金数据
        :return: 包含住宅租金和写字楼租金的字典
        """
        rent['rent'] = rent['rent'].apply(other2int)
        rent['coveringArea'] = rent['coveringArea'].apply(other2int)

        cond_1 = (rent['unit'] == '元/㎡/月') & (rent['rent'] < 2000) & \
                 (rent['rent'] > 0) & (rent['coveringArea'] > 0)
        rent.ix[cond_1,'rent'] = rent.ix[cond_1, 'rent'] / 30
        rent1 = rent.ix[cond_1, ['houseType', 'rent']]

        cond_2 = (rent['unit'] == '元/月') & (rent['rent'] < 200000) & \
                 (rent['rent'] > 0) & (rent['coveringArea'] > 0)
        rent.ix[cond_2, 'rent'] = rent.ix[cond_2, 'rent'] /\
                                  rent.ix[cond_2, 'coveringArea']/30
        rent2 = rent.ix[cond_2, ['houseType','rent']]

        cond_3 = (rent['unit'] == '元/㎡/天') & (rent['rent'] < 100) & \
                 (rent['rent'] > 0) & (rent['coveringArea'] > 0)
        rent3 = rent.ix[cond_3, ['houseType', 'rent']]

        rent = pd.concat([rent1,rent2,rent3])
        rent['rent'] = rent['rent'].apply(other2int)

        try:
            rent = rent.groupby(['houseType'])['rent'].mean()
        except Exception as e:
            print(e,'residence/office building rent will be filled with 0')
            return {'residence':0,'office_building':0}

        try:
            residence_rent = rent['住宅区']
        except Exception as e:
            print(e,'residence rent will be filled with 0')
            residence_rent = 0

        try:
            office_building_rent = rent['写字楼']
        except Exception as e:
            print(e,'office building rent will be filled with 0')
            office_building_rent = 0

        return {'residence':residence_rent,'office_building':office_building_rent}


    def reshape_industry(self,industry_tmp):
        """计算市场表的industry排名
        
        :param industry: 由1级、2级行业，纵向拼接成的数据框
        :return: 包含一个商圈排名前三的1、2级行业的 industry 字典
        """
        k=0
        industry_dict = defaultdict()
        for j,industry_row in industry_tmp.iterrows():
            k+=1
            if k >3:break
            industry_dict['industryNo_1_%d' % k] = industry_row['industryNo_1']
            industry_dict['industry_1_%d' % k] = industry_row['industry_1']
            industry_dict['industryNo_2_%d' % k] = industry_row['industryNo_2']
            industry_dict['industry_2_%d' % k] = industry_row['industry_2']

        return industry_dict


    def compile_dfs(self,sample_tag_counts,rent,industry_dict,zone_grandparent):
        # print(list(map(len,[sample_tag_counts,rent,industry_dict,zone_grandparent])))
        # print(sample_tag_counts,rent,industry_dict,zone_grandparent)
        """组合sample_tag_counts,rent,industry,zone_grandparent三个数据框
        
        用字典先封装，同时完成变量筛选和重命名的工作
        :param sample_tag_counts: tag样本的计数表
        :param rent: 租金表
        :param industry: 行业数据表
        :param zone_grandparent: 商圈所在的行政区划表
        :return: merge组合结果
        """
        def type_swich(int_x):
            mapping_dict = {
                1:'街道',
                2:'市场',
                3:'商场'
            }
            return mapping_dict.get(int_x)

        def clean_market_name(name):
            if name.find(":") != -1:
                return name.split(':')[0]
            else:
                return name
        merged_dict = {
            'marketGuid': sample_tag_counts['grandParentId'],
            'marketName': clean_market_name(zone_grandparent.ix[0,'grandParentName']),
            'marketZoneGuid': zone_grandparent['zoneGuid'],
            'marketZoneName': zone_grandparent['zoneName'],
            'divisionKey': zone_grandparent['districtId'],
            'marketTypeName': type_swich(sample_tag_counts['type']),
            'marketType': sample_tag_counts['type'],
            'marketArea': 0,
            'marketBankOutletsNum': sample_tag_counts['bankOutlets'],
            'marketATMNum': sample_tag_counts['ATM'],
            'marketBusStationNum': sample_tag_counts['busStation'],
            'marketBusStopNum': sample_tag_counts['busStop'],
            'marketTrainStationNum': sample_tag_counts['trainStation'],
            'marketBusLineNum': sample_tag_counts['busLine'],
            'marketMetroStationNum': sample_tag_counts['metroStation'],
            'marketRestaurantNum': sample_tag_counts['restaurant'],
            'marketHotelNum': sample_tag_counts['hotel'],
            'marketResidenceNum': sample_tag_counts['residence'],
            'marketOfficeBuildingNum': sample_tag_counts['officeBuilding'],
            'marketResidenceRent': rent['residence'],
            'marketOfficeBuildingRent': rent['office_building'],
            'marketIndustry_1_1': industry_dict.get('industry_1_1'),
            'marketIndustry_1_2': industry_dict.get('industry_1_2'),
            'marketIndustry_1_3': industry_dict.get('industry_1_3'),
            'marketIndustry_2_1': industry_dict.get('industry_2_1'),
            'marketIndustry_2_2': industry_dict.get('industry_2_2'),
            'marketIndustry_2_3': industry_dict.get('industry_2_3'),
            'marketIndustryNo_1_1': industry_dict.get('industryNo_1_1'),
            'marketIndustryNo_1_2': industry_dict.get('industryNo_1_2'),
            'marketIndustryNo_1_3': industry_dict.get('industryNo_1_3'),
            'marketIndustryNo_2_1': industry_dict.get('industryNo_2_1'),
            'marketIndustryNo_2_2': industry_dict.get('industryNo_2_2'),
            'marketIndustryNo_2_3': industry_dict.get('industryNo_2_3')
        }
        return pd.DataFrame(merged_dict)