# coding:utf-8
import logging
from pandas import DataFrame

class Transform(object):
    def _region_map(self,x):
        region_dict = {
            '北京市':'东部',
            '天津市':'东部',
            '河北省':'东部',
            '山西省':'中部',
            '内蒙古自治区':'西部',
            '辽宁省':'东部',
            '吉林省':'中部',
            '黑龙江省':'中部',
            '上海市':'东部',
            '江苏省':'东部',
            '浙江省':'东部',
            '安徽省':'中部',
            '福建省':'东部',
            '江西省':'中部',
            '山东省':'东部',
            '河南省':'中部',
            '湖北省':'中部',
            '湖南省':'中部',
            '广东省':'东部',
            '广西壮族自治区':'西部',
            '海南省':'东部',
            '重庆市':'西部',
            '四川省':'西部',
            '贵州省':'西部',
            '云南省':'西部',
            '西藏自治区':'西部',
            '陕西省':'西部',
            '甘肃省':'西部',
            '青海省':'西部',
            '宁夏回族自治区':'西部',
            '新疆维吾尔自治区':'西部'
        }
        return region_dict.get(x)

    def std_districts(self, division_datasets):

        std_divisions = {
            'divisionKey':division_datasets['districtno'],
            'divisionProv':division_datasets['prov'],
            'divisionCity':division_datasets['city'],
            'divisionDistrict':division_datasets['district'],
            'divisionProvNo':division_datasets['provno'],
            'divisionCityNo':division_datasets['cityno'],
            'divisionRegion':division_datasets['prov'].apply(self._region_map)
        }
        logging.info('Succeed to transform division_datasets')
        return DataFrame(std_divisions)