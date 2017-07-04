# coding:utf-8
import logging


class Load(object):
    def __init__(self,target_engine):
        self.target_engine = target_engine

    def loading(self,std_districts):
        """装载

        :param std_districts: 标准的区县数据
        """
        try:
            std_districts.to_sql(name='dimension_division',con=self.target_engine,
                                 if_exists='append',index=False)
            logging.info('Succeed to load std_districts,size = %d'%len(std_districts))
        except Exception as e:
            logging.error(e)