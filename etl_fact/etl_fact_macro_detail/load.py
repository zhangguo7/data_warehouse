import logging


class Load(object):

    def __init__(self,target_engine):
        self.target_engine = target_engine

    def loading(self,macro_details):
        """载入清理完成的数据"""
        try:
            macro_details.to_sql(name='fact_macro_detail',con=self.target_engine,
                                 if_exists='append',index=False)
        except Exception as e:
            logging.error(e)
        else:
            logging.info('Succeed to load fact_macro_detail,size=%d' % len(macro_details))