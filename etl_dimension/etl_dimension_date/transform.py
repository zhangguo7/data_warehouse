import logging

from pandas import date_range,DataFrame
from numpy import where,int64


class Transform(object):

    def gen_date(self,full_date):

        date_table = {
            'dateKey':full_date.strftime('%Y%m%d').astype(int64),
            'dateFullDate':full_date,
            'dateDayOfWeek':full_date.weekday+1,
            'dateYear':full_date.year,
            'dateQuarter':full_date.quarter,
            'dateMonth':full_date.month,
            'dateDay':full_date.day,
            'dateIsWeekday':where(full_date.weekday > 4,'非工作日','工作日')
        }
        logging.info('Succeed to transform date_table')
        return DataFrame(date_table)



