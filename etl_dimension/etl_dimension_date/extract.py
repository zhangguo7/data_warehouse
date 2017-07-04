import logging

from pandas import date_range

class Extract(object):

    def gen_full_date(self,start_date='1900-1-1',end_date='2099-12-31'):
        """生成Full Date Range

        :param start_date: 开始日期
        :param end_date: 结束日期
        :return: full_date_range
        """
        full_date = date_range(start_date, end_date)
        logging.info('Full date is generated, from %s to %s'%(start_date,end_date))
        return full_date