import sys
sys.path.append('../../tools')
import pandas as pd
from tool_funcs import other2int

class Transform(object):

    def rent_calculate(self,rent):

        rent['rent'] = rent['rent'].apply(other2int)
        rent['coveringArea'] = rent['coveringArea'].apply(other2int)

        cond_1 = (rent['unit'] == '元/㎡/月') & (rent['rent'] < 2000)
        rent.ix[cond_1,'rent'] = rent.ix[cond_1, 'rent'] / 30
        rent1 = rent.ix[cond_1, ['houseType', 'rent']]

        cond_2 = (rent['unit'] == '元/月') & (rent['rent'] < 200000)
        rent.ix[cond_2, 'rent'] = rent.ix[cond_2, 'rent'] /\
                                  rent.ix[cond_2, 'coveringArea']/30
        rent2 = rent.ix[cond_2, ['houseType','rent']]

        cond_3 = (rent['unit'] == '元/㎡/天') & (rent['rent'] < 100)
        rent3 = rent.ix[cond_3, ['houseType', 'rent']]

        rent = pd.concat([rent1,rent2,rent3])

        try:
            rent = rent.groupby(['houseType'])['rent'].mean()
        except Exception as e:
            print(e)
            return {'residence':0,'office_building':0}
        try:
            residence = rent['住宅区']
        except:
            residence = 0

        try:
            office_building = rent['写字楼']
        except:
            office_building = 0

        return {'residence':residence,'office_building':office_building}


    def reshape_industry(self,industry):
        """重新构建industry
        
        :param industry: 
        :return: 
        """
        industry_1 = industry.ix[industry['industryPid'] == '0',
                                 ['industryId','industryName']]
        industry_2 = industry.ix[industry['industryPid'] != '0',
                                 ['industryId', 'industryName','industryPid']]
        industry_new = pd.merge(industry_1,industry_2,how='outer',
                                left_on='industryId',right_on='industryPid')

        industry_nums = len(industry_new)

        industry_1_1 = industry_new.ix[0, 'industryName_x'] if industry_nums > 0 else None
        industryNo_1_1 = industry_new.ix[0, 'industryId_x'] if industry_nums > 0 else None
        industry_2_1 = industry_new.ix[0, 'industryName_y'] if industry_nums > 0 else None
        industryNo_2_1 = industry_new.ix[0, 'industryId_y'] if industry_nums > 0 else None

        industry_1_2 = industry_new.ix[1, 'industryName_x'] if industry_nums > 1 else None
        industryNo_1_2 = industry_new.ix[1, 'industryId_x'] if industry_nums > 1 else None
        industry_2_2 = industry_new.ix[1, 'industryName_y'] if industry_nums > 1 else None
        industryNo_2_2 = industry_new.ix[1, 'industryId_y'] if industry_nums > 1 else None

        industry_1_3 = industry_new.ix[2, 'industryName_x'] if industry_nums > 2 else None
        industryNo_1_3 = industry_new.ix[2, 'industryId_x'] if industry_nums > 2 else None
        industry_2_3 = industry_new.ix[2, 'industryName_y'] if industry_nums > 2 else None
        industryNo_2_3 = industry_new.ix[2, 'industryId_y'] if industry_nums > 2 else None

        industry = {
            'industry_1_1':industry_1_1,
            'industry_1_2':industry_1_2,
            'industry_1_3':industry_1_3,
            'industry_2_1':industry_2_1,
            'industry_2_2':industry_2_2,
            'industry_2_3':industry_2_3,
            'industryNo_1_1':industryNo_1_1,
            'industryNo_1_2':industryNo_1_2,
            'industryNo_1_3':industryNo_1_3,
            'industryNo_2_1':industryNo_2_1,
            'industryNo_2_2':industryNo_2_2,
            'industryNo_2_3':industryNo_2_3
        }

        return industry

    def merge(self,sample_tag_counts,rent,industry,zone_grandparent):

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
        print(zone_grandparent.ix[0,'grandParentName'])
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
            'marketIndustry_1_1': industry['industry_1_1'],
            'marketIndustry_1_2': industry['industry_1_2'],
            'marketIndustry_1_3': industry['industry_1_3'],
            'marketIndustry_2_1': industry['industry_2_1'],
            'marketIndustry_2_2': industry['industry_2_2'],
            'marketIndustry_2_3': industry['industry_2_3'],
            'marketIndustryNo_1_1': industry['industryNo_1_1'],
            'marketIndustryNo_1_2': industry['industryNo_1_2'],
            'marketIndustryNo_1_3': industry['industryNo_1_3'],
            'marketIndustryNo_2_1': industry['industryNo_2_1'],
            'marketIndustryNo_2_2': industry['industryNo_2_2'],
            'marketIndustryNo_2_3': industry['industryNo_2_3']
        }
        return pd.DataFrame(merged_dict)