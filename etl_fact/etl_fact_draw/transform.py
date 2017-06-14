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
from tool_funcs import other2int


class Transform(object):
    """转换绘图数据"""

    def __init__(self, df_industry, df_draw):
        self.df_industry = df_industry
        self.df_draw = df_draw

    def _filter_ind1_ind2(self):
        """筛选出一级行业和二级行业"""
        df_industry1 = self.df_industry.ix[self.df_industry['industryPid'] == '0',:]
        df_industry2 = self.df_industry.ix[self.df_industry['industryPid'] != '0', :]
        return df_industry1, df_industry2

    def _merge_ind_draw(self, df_industry1, df_industry2):
        """将一级、二级行业增加到对应的新列"""
        self.df_draw = pd.merge(self.df_draw,df_industry1,
                                left_on='guid',right_on='attachId')
        merge_ind = pd.merge(self.df_draw,df_industry2,
                             left_on='guid',right_on='attachId')
        return merge_ind

    def _deal_operatingState(self, df):
        """处理经营状态，对三个经营状态变量提取相关信息

        :param df: 只有原始经营状态的数据框 operatingState,operatingState1,operatingState2
        :return df:增加了转租、空置、招聘、装修、仓库
        """
        df['drawSublease'] = ''
        df['drawEmpty'] = ''
        df['drawRecruit'] = ''
        df['drawRenovation'] = ''
        df['drawWarehouse'] = ''
        # 转租、转让
        df.ix[df['operatingState'].apply(lambda x: '5' in str(x)),'drawSublease'] = '转租、转让'
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
        df.ix[df['operatingState'].apply(lambda x: '2' in str(x)), 'drawEmpty'] = '空置'
        df.ix[df['operatingState1'].apply(lambda x: '2' in str(x)), 'drawEmpty'] = '空置'
        # 招聘
        df.ix[df['operatingState1'].apply(lambda x: '4' in str(x)), 'drawRecruit'] = '招聘'

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
                1:'无装修',
                2:'简单装修',
                3:'精装修',
                4:'无法观测'
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
            df['sampleMobile']+'|'+df['sampleTel'],
            df['sampleMobile'] + df['sampleTel']
        )
        return df

    def _split_zbh(self,doorplate_lst):
        new_dp_lst = []
        selfnum_lst = []
        for dp in doorplate_lst:
            try:
                zbh = re.search('自编号\d+号*',dp).group()
                new_dp = dp.replace(zbh,'').replace('|','')
            except Exception as e:
                print(e)
                zbh = ''
                new_dp=dp

            new_dp_lst.append(new_dp)
            selfnum_lst.append(zbh)

        return new_dp_lst, selfnum_lst


    def _del_unnecessary_vars(self,df):
        """删除多余的变量

        :param df: 未删除多余变量的数据框
        :return: 删除多余变量的数据框
        """
        unnecessary_vars = [
            'attachId_x','attachId_y','industryPid_x','industryPid_y',
            'operatingState','operatingState1','operatingState2',
            'inputDate','receiveDate','sampleMobile', 'sampleTel'
        ]
        for var in unnecessary_vars:
            del df[var]
        return df

    def _doorplate_selfnum(self,df):
        """从门牌号中提取自编号
        
        :param df: 门牌号和自编号混淆的数据框
        :return: 门牌号和自编号分离的数据框
        """
        dp_series = df['doorPlate']
        new_dp_lst, selfnum_lst = self._split_zbh(dp_series)
        df['doorPlate'] = pd.Series(new_dp_lst)
        tmp_zbh = pd.Series(selfnum_lst)

        df['selfNum'] = np.where((df['selfNum'] == '') | (df['selfNum'].isnull()),
                                 tmp_zbh, df['selfNum'])
        return df

    def _trans_has_licence(self,df):
        df['isBusinessLicence'] = df['isBusinessLicence'].\
            apply(lambda x:'悬挂' if x == 1 else '未悬挂')
        return df

    def _rename(self,df):
        """重命名函数

        :param df: 原始变量名的数据框
        :return: 更新了变量名的数据框
        """
        """变量重命名"""
        new_names = [
            'drawGuid','marketGuid','drawZoneGuid','divisionKey','drawMateAddress',
            'drawDoorPlate','drawSelfNum','drawCompanyName','drawLatitude',
            'drawLongitude','drawPhotoCount','drawShopCount','drawDecorate',
            'drawHagLicence','drawIndustryNo_1','drawIndustryName_1','drawindustryNo_2',
            'drawIndustryName_2','drawSublease','drawEmpty','drawRecruit',
            'drawRenovation','drawWarehouse','receiveDateKey','receiveTimeKey',
            'inputDateKey','inputTimeKey','drawTel'
        ]
        df.columns = new_names
        return df

    def transform_main(self):
        # 转换行业
        df_ind1, df_ind2 = self._filter_ind1_ind2()
        merge_ind = self._merge_ind_draw(df_ind1,df_ind2)
        # 转换经营状态
        df = self._deal_operatingState(merge_ind)
        # 增加日期和时间键
        df = self._trans_DT(df)
        # 拼接电话和手机
        df = self._concat_tel(df)
        # 清理自编号
        df = self._doorplate_selfnum(df)
        # 清理装修
        df = self._trans_deco(df)
        # 删除多余的变量
        df = self._del_unnecessary_vars(df)
        # 重命名
        df = self._rename(df)

        # 数据类型转换
        df['divisionKey'] = df['divisionKey'].apply(other2int)
        return df
