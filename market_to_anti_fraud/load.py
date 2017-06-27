# coding:utf-8
import os,sys

import logging


class Load(object):
    """ api2 数据装载类
    
    包含1个loading方法
    """
    def __init__(self,target_engine,record_file):
        self.target_engine = target_engine
        self.record_file = record_file

    def loading(self, df):
        """api2 数据装载函数
        
        若装载成功，更新record文件
        :param df: 已经完成et的数据框
        """
        try:
            df.to_sql(name='API_2', con=self.target_engine,
                      if_exists='append', index=False)
            logging.info('ETL secceed, %d obs. loaded in'% len(df))
        except Exception as e:
            logging.error('%s,ETL failed, when loaded in'%e)
            os.remove(self.record_file + '.tmp')
        else:
            os.rename(self.record_file+'.tmp',self.record_file)