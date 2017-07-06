import logging

import pandas as pd
from numpy import where

class Transform(object):


    def _date_key(self,string_datetime):
        """将插入的时间转换为日期键的形式

        :param string_datetime:
        """
        return int(str(string_datetime)[:10].replace('-', ''))

    def _other_2_int(self,x):
        """将其他类型转换为int型"""
        try:
            return int(x)
        except:
            return 0

    def _mapping_poi_type(self,x):
        """映射poi type和no的关系

        :param x:type的no
        :return:type文字
        """
        poi_dict = {
            1: '公交', 2: '地铁', 3: '火车站', 4: '汽车站', 5: '餐饮店',
            6: '酒店', 7: '银行网点', 8: 'ATM', 9: '写字楼', 10: '住宅'
        }
        return poi_dict.get(x)

    def compile_datasets(self,tag_details,divisions):
        """组合数据集

        :param tag_details:tag详情表
        :param divisions: 区县表
        :return:
        """

        merged_df = pd.merge(
            tag_details,divisions, how='left',
            left_on=['provinceName', 'cityName', 'districtName'],
            right_on=['divisionProv', 'divisionCity', 'divisionDistrict']
        )
        macro_details_dict = {
            'marketGuid': merged_df['grandParentId'],
            'detailTagType': merged_df['poiType'].apply(self._mapping_poi_type),
            'detailTagTypeNo': merged_df['poiType'],
            'detailShopName': merged_df['poiName'],
            'divisionKey': merged_df['divisionKey'],
            'detailLatitude': merged_df['bdLatitude'],
            'detailLongitude': merged_df['bdLongitude'],
            'detailAddress': where(merged_df['poiType'] != 1,merged_df['address'],''),
            'detailBusLine': where(merged_df['poiType'] == 1,merged_df['address'],''),
            'detailStreetId': merged_df['street_id'],
            'detailTelephone': merged_df['telephone'],
            'detailUid': merged_df['uid'],
            'detailDistance': merged_df['distance'],
            'detailBDType': merged_df['type'],
            'detailBDTag': merged_df['tag'],
            'detailPrice': merged_df['price'].apply(self._other_2_int),
            'detailShopHours': merged_df['shop_hours'],
            'detailOverallRating': merged_df['overall_rating'],
            'detailTasteRating': merged_df['taste_rating'],
            'detailServiceRating': merged_df['service_rating'],
            'detailEnvironmentRating': merged_df['environment_rating'],
            'detailFacilityRating': merged_df['facility_rating'],
            'detailHygieneRating': merged_df['hygiene_rating'],
            'detailTechnologyRating': merged_df['technology_rating'],
            'detailImageNum': merged_df['image_num'].apply(self._other_2_int),
            'detailGrouponNum': merged_df['groupon_num'].apply(self._other_2_int),
            'detailDiscountNum': merged_df['discount_num'].apply(self._other_2_int),
            'detailCommentNum': merged_df['comment_num'].apply(self._other_2_int),
            'detailFavoriteNum': merged_df['favorite_num'].apply(self._other_2_int),
            'detailCheckinNum': merged_df['checkin_num'].apply(self._other_2_int),
            'detailAtmosphere': merged_df['atmosphere'],
            'detailFeaturedService': merged_df['featured_service'],
            'detailRecommendation': merged_df['recommendation'],
            'detailDescription': merged_df['description'],
            'detailReviewKeyword': merged_df['di_review_keyword'],
            'detailCategory': merged_df['category'],
            'detailInnerFacility': merged_df['inner_facility'],
            'detailHotelFacility': merged_df['hotel_facility'],
            'detailPaymentType': merged_df['payment_type'],
            'detailAlias': merged_df['alias'],
            'detailBrand': merged_df['brand'],
            'detailHotelService': merged_df['hotel_service'],
            'createDateKey': merged_df['createTime'].apply(lambda x:str(x)[:10].replace('-',''))
        }
        logging.info('Succeed to transform macro_details')
        return pd.DataFrame(macro_details_dict)