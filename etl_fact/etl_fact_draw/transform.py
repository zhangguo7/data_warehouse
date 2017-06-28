# -*- coding:utf-8 -*-
"""
绘图事实表的数据转换模块

定义了一个class Transform
用于完成对fact_draw表的数据转换与清理

内部构建了一个 transform_main 方法
以及其他内部调用方法：
行业提取 _filter_ind1_ind2
行业并入 _merge_ind_draw
转换经营状态 _deal_operatingState
增加日期和时间键 _trans_DT
拼接电话和手机 _concat_tel
删除多余的变量 _del_unnecessary_vars
重命名 _rename
"""
import re
import sys
sys.path.append('../../tools')

import numpy as np
import pandas as pd
from tool_funcs import other2int,angle2half


class Transform(object):
    """转换绘图数据"""

    def _filter_ind1_ind2(self,df_industry):
        """筛选出一级行业和二级行业"""
        df_industry1 = df_industry.ix[df_industry['industryPid'] == '0',:]
        df_industry2 = df_industry.ix[df_industry['industryPid'] != '0', :]
        return df_industry1, df_industry2

    def _merge_ind_draw(self,df_draw, df_industry1, df_industry2):
        """将一级、二级行业增加到对应的新列"""
        df_draw = pd.merge(df_draw,df_industry1,
                                left_on='guid',right_on='attachId')
        merge_ind = pd.merge(df_draw,df_industry2,
                             left_on='guid',right_on='attachId')
        return merge_ind

    def _deal_operatingState(self, df):
        """处理经营状态，对三个经营状态变量提取相关信息

        :param df: 只有原始经营状态的数据框 operatingState,operatingState1,operatingState2
        :return df:增加了转租、空置、招聘、装修、仓库
        """
        add_vars = ['drawSublease','drawEmpty','drawRecruit','drawRenovation',
                    'drawWarehouse','drawClose','drawNormal']
        for var in add_vars:
            df[var] = None
        # 转租、转让
        df.ix[df['operatingState'].apply(lambda x: '5' in str(x)), 'drawSublease'] = '转租、转让'
        df.ix[df['operatingState2'].apply(lambda x: '1' in str(x)), 'drawSublease'] = '转租、转让'
        # 装修
        df.ix[df['operatingState'].apply(lambda x: '6' in str(x)), 'drawRenovation'] = '装修'
        df.ix[df['operatingState1'].apply(lambda x: '6' in str(x)), 'drawRenovation'] = '装修'
        df.ix[df['operatingState2'].apply(lambda x: '3' in str(x)), 'drawRenovation'] = '装修'
        # 仓库
        df.ix[df['operatingState'].apply(lambda x: '3' in str(x)), 'drawWarehouse'] = '仓库'
        df.ix[df['operatingState1'].apply(lambda x: '3' in str(x)), 'drawWarehouse'] = '仓库'
        df.ix[df['operatingState1'].apply(lambda x: '5' in str(x)), 'drawWarehouse'] = '仓库'
        # 空置
        df.ix[df['operatingState'].apply(lambda x: '4' in str(x)), 'drawEmpty'] = '空置'
        df.ix[df['operatingState1'].apply(lambda x: '4' in str(x)), 'drawEmpty'] = '空置'
        # 招聘
        df.ix[df['operatingState1'].apply(lambda x: '4' in str(x)), 'drawRecruit'] = '招聘'
        # 关门
        df.ix[df['operatingState'].apply(lambda x: '2' in str(x)), 'drawClose'] = '关门'
        df.ix[df['operatingState1'].apply(lambda x: '2' in str(x)), 'drawClose'] = '关门'
        # 正常
        df.ix[df['operatingState'].apply(lambda x: '1' in str(x)), 'drawNormal'] = '正常'
        df.ix[df['operatingState1'].apply(lambda x: '1' in str(x)), 'drawNormal'] = '正常'
        return df

    def _trans_DT(self,df):
        """增加日期和时间键

        :param df: 未包含日期和时间键的数据框
        :return: 包含日期和时间键的数据框
        """
        def _extract_datekey(x):
            return int(str(x)[:10].replace('-',''))

        def _extract_timekey(x):
            return int(str(x)[11:19].replace(':', ''))

        df['receiveDateKey'] = df['receiveDate'].apply(_extract_datekey)
        df['receiveTimeKey'] = df['receiveDate'].apply(_extract_timekey)
        df['inputDateKey'] = df['receiveDate'].apply(_extract_datekey)
        df['inputTimeKey'] = df['receiveDate'].apply(_extract_timekey)
        return df

    def _trans_deco(self,df):
        def mapping(x):
            deco_dict = {
                1: '无装修',
                2: '简单装修',
                3: '精装修',
                4: '无法观测'
            }
            return deco_dict.get(x)
        df['decorateDescrption'] = df['decorateDescrption'].apply(mapping)
        return df

    def _concat_tel(self,df):
        """拼接电话和手机

        :param df: 未拼接电话和手机的数据框
        :return: 拼接了电话和手机的数据框
        """
        df['drawTel'] = np.where(
            (df['sampleMobile'] != '') & (df['sampleTel'] != ''),
            df['sampleMobile']+','+df['sampleTel'],
            df['sampleMobile'] + df['sampleTel']
        )
        df['drawTel'] = df['drawTel'].apply(lambda x:x.replace('|',','))
        return df


    def _split_zbh(self,doorplate_lst):
        new_dp_lst = []
        selfnum_lst = []
        for dp in doorplate_lst:
            try:
                zbh = re.search('自编号*\d+号*',dp).group()
                new_dp = dp.replace(zbh,'').replace('|','').replace('#','号')
            except:
                zbh = ''
                new_dp=dp

            new_dp_lst.append(new_dp)
            selfnum_lst.append(zbh)

        return new_dp_lst, selfnum_lst

    def _doorplate_selfnum(self,df):
        """从门牌号中提取自编号
        
        :param df: 门牌号和自编号混淆的数据框
        :return: 门牌号和自编号分离的数据框
        """
        new_dp_lst, selfnum_lst = self._split_zbh(df['doorPlate'])
        df['doorPlate'] = pd.Series(new_dp_lst).apply(angle2half)
        tmp_zbh = pd.Series(selfnum_lst)

        df['selfNum'] = np.where((df['selfNum'] == '') | (df['selfNum'].isnull()),
                                 tmp_zbh, df['selfNum'])
        return df

    def _trans_has_licence(self,df):
        df['isBusinessLicence'] = df['isBusinessLicence'].\
            apply(lambda x: '悬挂' if x == 1 else '未悬挂')
        return df

    def _concat_companyaddress(self,df):
        """拼接地址

        :param df:
        :return:
        """
        def split_grandParentName(x):
            x = str(x)
            if x.find(':') != -1:
                return x.split(':')[0]
            return x
        df['grandParentName'] = df['grandParentName'].apply(split_grandParentName)

        city_lst = [
            '东莞市', '中山市',
            '北京市辖区', '北京的县',
            '重庆市辖区', '重庆的县',
            '上海市辖区', '上海市的县',
            '天津市辖区', '天津市的县'
        ]
        df['cityName'] = df['cityName'].apply(lambda x: '' if x in city_lst else x)
        df['drawCompanyAddress'] = df['provinceName'] + df['cityName'] + \
                                   df['districtName'] + df['grandParentName']

        return df

    def transform_main(self,df_industry, df_draw):
        # 转换行业
        df_ind1, df_ind2 = self._filter_ind1_ind2(df_industry)
        merge_ind = self._merge_ind_draw(df_draw,df_ind1,df_ind2)
        # 转换经营状态
        df = self._deal_operatingState(merge_ind)
        # 增加日期和时间键
        df = self._trans_DT(df)
        # 拼接电话和手机
        df = self._concat_tel(df)
        # 拼接地址
        df = self._concat_companyaddress(df)
        # 清理自编号
        df = self._doorplate_selfnum(df)
        # 清理装修
        df = self._trans_deco(df)
        # 悬挂营业执照
        df = self._trans_has_licence(df)

        # 数据类型转换
        df['districtId'] = df['districtId'].apply(other2int)
        df = df[-df['districtId'].isnull()]
        clean_dicf = {
            'drawGuid': df['guid'],
            'marketGuid': df['grandParentId'],
            'drawZoneGuid': df['zoneGuid'],
            'divisionKey': df['districtId'],
            'drawMateAddress': df['mateAddress'],
            'drawDoorPlate': df['doorPlate'],
            'drawSelfNum': df['selfNum'],
            'drawCompanyName': df['sampleName'],
            'drawLatitude': df['bdLatitude'],
            'drawLongitude': df['bdlongitude'],
            'drawPhotoCount': df['photoCount'],
            'drawShopCount': df['shopCount'],
            'drawDecorate': df['decorateDescrption'],
            'drawHagLicence': df['isBusinessLicence'],
            'drawIndustryNo_1': df['industryId_x'],
            'drawIndustryName_1': df['industryName_x'],
            'drawindustryNo_2': df['industryId_y'],
            'drawIndustryName_2': df['industryName_y'],
            'drawSublease': df['drawSublease'],
            'drawEmpty': df['drawEmpty'],
            'drawRecruit': df['drawRecruit'],
            'drawRenovation': df['drawRenovation'],
            'drawWarehouse': df['drawWarehouse'],
            'drawClose': df['drawClose'],
            'drawNormal': df['drawNormal'],
            'receiveDateKey': df['receiveDateKey'],
            'receiveTimeKey': df['receiveTimeKey'],
            'inputDateKey': df['inputDateKey'],
            'inputTimeKey': df['inputTimeKey'],
            'drawTel': df['drawTel'],
            'drawCompanyAddress': df['drawCompanyAddress']
        }
        return pd.DataFrame(clean_dicf)