# -*- coding:utf-8 -*-
import pandas as pd

from tools.tool_funcs import other2int


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

        :param df: 增加了转租、空置、招聘、装修、仓库
        """
        df['drawSublease'] = ''
        df['drawEmpty'] = ''
        df['drawRecruit'] = ''
        df['drawRenovation'] = ''
        df['drawWarehouse'] = ''
        # 转租、转让
        df.ix[df['operatingState'] == 5,'drawSublease'] = '转租、转让'
        df.ix[df['operatingState2'] == 1, 'drawSublease'] = '转租、转让'
        # 装修
        df.ix[df['operatingState'] == 6, 'drawRenovation'] = '装修'
        df.ix[df['operatingState1'] == 6, 'drawRenovation'] = '装修'
        df.ix[df['operatingState2'] == 3, 'drawRenovation'] = '装修'
        # 仓库
        df.ix[df['operatingState'] == 3, 'drawWarehouse'] = '仓库'
        df.ix[df['operatingState1'] == 3, 'drawWarehouse'] = '仓库'
        df.ix[df['operatingState1'] == 5, 'drawWarehouse'] = '仓库'
        # 空置
        df.ix[df['operatingState'] == 4, 'drawEmpty'] = '空置'
        df.ix[df['operatingState1'] == 4, 'drawEmpty'] = '空置'
        # 招聘
        df.ix[df['operatingState1'] == 4, 'drawRecruit'] = '招聘'

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

    def _concat_tel(self,df):
        """拼接电话和手机

        :param df: 未拼接电话和手机的数据框
        :return: 拼接了电话和手机的数据框
        """
        df['drawTel'] = df['sampleMobile']+df['sampleTel']
        return df

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

    def _rename(self,df):
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
        # 删除多余的变量
        df = self._del_unnecessary_vars(df)
        # 重命名
        df = self._rename(df)

        # 数据类型转换
        df['divisionKey'] = df['divisionKey'].apply(other2int)
        return df