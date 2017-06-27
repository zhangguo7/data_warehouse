# coding:utf-8
import logging
import numpy as np
import pandas as pd


class Transform(object):

    def _str_rate(self,rate):
        return '%.2f %%'%(float(rate)*100)

    def _merge2industry_id(self,industry_id):
        return 25 if industry_id in [23,24] else industry_id

    def _nums_clean(self,num):
        try:
            num = int(num)
            return num if num > 0 else 0
        except Exception as e:
            logging.warning('%s, _nums_clean input error !'%e)
            return 0

    def reshape_market(self,market_df):
        # 修改行业相关信息
        for i in range(1, 4):
            # 遍历i个二级行业，合并23、24、25
            market_df['marketIndustryNo_2_%d' % i] = \
                market_df['marketIndustryNo_2_%d' % i].apply(self._merge2industry_id)
            # 百货商场、超市、小卖部合并
            market_df['marketIndustry_2_%d' % i] = \
                np.where(market_df['marketIndustryNo_2_%d' % i] == 25,
                         '百货商场、超市、小卖部',
                         market_df['marketIndustry_2_%d' % i])
            # 批发零售业的行业改为2级名称
            market_df['marketIndustry_1_%d' % i] = \
                np.where(market_df['marketIndustryNo_1_%d' % i] == 1,
                         market_df['marketIndustry_2_%d' % i],
                         market_df['marketIndustry_1_%d' % i])

        # 修改计数相关信息
        nan_vars = [
            'marketATMNum', 'marketBusStationNum', 'marketTrainStationNum',
            'marketBusLineNum', 'marketMetroStationNum', 'marketRestaurantNum',
            'marketHotelNum', 'marketResidenceNum', 'marketOfficeBuildingNum'
        ]
        for var in nan_vars:
            market_df[var] = market_df[var].apply(self._nums_clean)
        logging.info('Secceed to transform market_df,size = %d ' % len(market_df))

        return market_df

    def aggregate_from_samples(self,draw_samples):
        """将draw_samples进行分类汇总
        
        :param draw_samples: 绘图样本经营状态
        :return: 
        """
        var_lst = [
            'drawNormal', 'drawClose', 'drawEmpty', 'drawRecruit',
            'drawRenovation', 'drawSublease', 'drawWarehouse'
        ]
        for var in var_lst:
            draw_samples[var] = np.where(
                draw_samples[var].isnull(),
                np.zeros(len(draw_samples)),
                np.ones(len(draw_samples))
            )
        grouped_df = draw_samples.groupby(['marketGuid'])
        sampleCount_df = grouped_df.size()
        normalCount_df = grouped_df['drawNormal'].sum()
        notOpen_df = grouped_df['drawClose'].sum()
        vacancyCount_df = grouped_df['drawEmpty'].sum()
        transferCount_df = grouped_df['drawSublease'].sum()
        recruitmentCount_df = grouped_df['drawRecruit'].sum()
        renovationCount_df = grouped_df['drawRenovation'].sum()
        warehouseCount_df = grouped_df['drawWarehouse'].sum()
        aggregate_df = pd.concat([
            sampleCount_df, normalCount_df, notOpen_df,vacancyCount_df,
            transferCount_df, recruitmentCount_df, renovationCount_df ,warehouseCount_df
        ],axis=1)
        aggregate_df.columns = [
            'jdSampleCount', 'normalCount', 'notOpenCount', 'vacancyCount',
            'transferCount', 'recruitmentCount', 'renovationCount', 'warehouseCount'
        ]
        aggregate_df['normalRate']=aggregate_df['normalCount']/aggregate_df['jdSampleCount']
        aggregate_df['notOpenRate'] = aggregate_df['notOpenCount']/aggregate_df['jdSampleCount']
        aggregate_df['vacancyRate'] = aggregate_df['vacancyCount']/aggregate_df['jdSampleCount']
        aggregate_df['warehouseRate'] = aggregate_df['warehouseCount']/aggregate_df['jdSampleCount']
        aggregate_df['renovationRate'] = aggregate_df['renovationCount']/aggregate_df['jdSampleCount']
        aggregate_df['transferRate'] = aggregate_df['transferCount']/aggregate_df['jdSampleCount']
        aggregate_df['recruitmentRate'] = aggregate_df['recruitmentCount']/aggregate_df['jdSampleCount']

        aggregate_df['normalRate'] = aggregate_df['normalRate'].apply(self._str_rate)
        aggregate_df['notOpenRate'] = aggregate_df['notOpenRate'].apply(self._str_rate)
        aggregate_df['vacancyRate'] = aggregate_df['vacancyRate'].apply(self._str_rate)
        aggregate_df['warehouseRate'] = aggregate_df['warehouseRate'].apply(self._str_rate)
        aggregate_df['renovationRate'] = aggregate_df['renovationRate'].apply(self._str_rate)
        aggregate_df['transferRate'] = aggregate_df['transferRate'].apply(self._str_rate)
        aggregate_df['recruitmentRate'] = aggregate_df['recruitmentRate'].apply(self._str_rate)

        logging.info('Secceed to transform aggregate_df,size = %d '%len(aggregate_df))
        return aggregate_df

    def compile(self,reshaped_market,aggregated_samples):
        """
        
        :param reshaped_market: 经过重新构造的市场表
        :param aggregated_samples: 经过市场级别汇总统计的经营状态表
        :return: 可以进入 load 步骤的数据框
        """
        merged_df = pd.merge(reshaped_market, aggregated_samples,
                             how='left',left_on='marketGuid', right_index=True)

        api2_dict = {
            'relId': merged_df['marketGuid'],
            'ATM':merged_df['marketATMNum'],
            'restaurant':merged_df['marketRestaurantNum'],
            'metroStation':merged_df['marketMetroStationNum'],
            'busLine':merged_df['marketBusLineNum'],
            'trainStation':merged_df['marketTrainStationNum'],
            'hotel':merged_df['marketHotelNum'],
            'busStation':merged_df['marketBusStationNum'],
            'officeBuilding':merged_df['marketOfficeBuildingNum'],
            'bankOutlets':merged_df['marketBankOutletsNum'],
            'residence':merged_df['marketResidenceNum'],
            'type':merged_df['marketType'],
            'averageRent':np.zeros(len(merged_df)),
            'officeBuildingAverageRent':merged_df['marketOfficeBuildingRent'],
            'residenceAverageRent':merged_df['marketResidenceRent'],
            'area':np.zeros(len(merged_df)),
            'industryNo1_1':merged_df['marketIndustryNo_1_1'],
            'industryName1_1':merged_df['marketIndustry_1_1'],
            'industryNo1_2':merged_df['marketIndustryNo_1_2'],
            'industryName1_2':merged_df['marketIndustry_1_2'],
            'industryNo1_3':merged_df['marketIndustryNo_1_3'],
            'industryName1_3':merged_df['marketIndustry_1_3'],
            'industryNo2_1':merged_df['marketIndustryNo_2_1'],
            'industryName2_1':merged_df['marketIndustry_2_1'],
            'industryNo2_2':merged_df['marketIndustryNo_2_2'],
            'industryName2_2':merged_df['marketIndustry_2_2'],
            'industryNo2_3':merged_df['marketIndustryNo_2_3'],
            'industryName2_3':merged_df['marketIndustry_2_3'],
            'jdSampleCount':merged_df['jdSampleCount'],
            'normalCount':merged_df['normalCount'],
            'normalRate':merged_df['normalRate'],
            'notOpenCount':merged_df['notOpenCount'],
            'notOpenRate':merged_df['notOpenRate'],
            'vacancyCount':merged_df['vacancyCount'],
            'vacancyRate':merged_df['vacancyRate'],
            'warehouseCount':merged_df['warehouseCount'],
            'warehouseRate':merged_df['warehouseRate'],
            'renovationCount':merged_df['renovationCount'],
            'renovationRate':merged_df['renovationRate'],
            'transferCount':merged_df['transferCount'],
            'transferRate':merged_df['transferRate'],
            'recruitmentCount':merged_df['recruitmentCount'],
            'recruitmentRate':merged_df['recruitmentRate']
        }
        return pd.DataFrame(api2_dict)